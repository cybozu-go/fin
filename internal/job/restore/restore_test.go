package restore

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/deletion"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/test/utils"

	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type setupInput struct {
	// if true, run incremental backup after full backup, otherwise, only run full backup.
	enableIncrementalBackup bool
	// if > 0, delete backups after creating them.
	// Backups are deleted starting with the oldest ones, up to the specified number of backups.
	deleteBackups int
}

type setupOutput struct {
	// RestoreInput instance used when calling the restore module.
	// 0 index is for the full backup, and 1 index is for the incremental backup.
	restoreInputs []*input.Restore
}

func setup(t *testing.T, config *setupInput) *setupOutput {
	t.Helper()

	k8sRepo, rbdRepo, volumeInfo := fake.NewStorage()
	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	rawImageChunkSize := 4096
	targetSnapshotSize := rawImageChunkSize * 2

	snapIDs := make([]int, 0)
	restoreInputs := make([]*input.Restore, 0)

	// Create backups.
	backupCount := 1
	if config.enableIncrementalBackup {
		backupCount = 2
	}
	for i := 0; i < backupCount; i++ {
		snapshot := rbdRepo.CreateFakeSnapshot(utils.GetUniqueName("snap-"), targetSnapshotSize, time.Now())
		snapIDs = append(snapIDs, snapshot.ID)
		var srcSnapID *int
		if i != 0 {
			srcSnapID = &snapIDs[i-1]
		}
		backupInput := testutil.NewBackupInput(k8sRepo, volumeInfo, snapshot.ID, srcSnapID, rawImageChunkSize)
		backupInput.Repo = finRepo
		backupInput.KubernetesRepo = k8sRepo
		backupInput.RBDRepo = rbdRepo
		backupInput.NodeLocalVolumeRepo = nlvRepo

		bk := backup.NewBackup(backupInput)
		err := bk.Perform()
		require.NoError(t, err)

		// Create the restore file
		restorePath := testutil.CreateRestoreFileForTest(t, int64(targetSnapshotSize))
		rVol := fake.NewRestoreVolume(restorePath)

		restoreInputs = append(restoreInputs, testutil.NewRestoreInputTemplate(
			backupInput, rVol, rawImageChunkSize, backupInput.TargetSnapshotID))
	}

	// Delete backups if specified.
	for i := 0; i < config.deleteBackups; i++ {
		del := deletion.NewDeletion(&deletion.DeletionInput{
			Repo:                finRepo,
			RBDRepo:             rbdRepo,
			NodeLocalVolumeRepo: nlvRepo,
			ActionUID:           uuid.New().String(),
			TargetSnapshotID:    snapIDs[0],
			TargetPVCUID:        restoreInputs[0].TargetPVCUID,
		})
		err := del.Perform()
		require.NoError(t, err)
		snapIDs = snapIDs[1:]
	}

	return &setupOutput{
		restoreInputs: restoreInputs,
	}
}

func TestRestore_FullBackup_Success(t *testing.T) {
	// CSATEST-1548
	// Description:
	//   Restore from the full backup to the restore file
	//   with no error.
	//
	// Arrange:
	//   - A full backup, `backup`, consists of two chunks.
	//   - raw.img filled with the random data.
	//   - The restore file.
	// Act:
	//   Run the restore process.
	//
	// Assert:
	//   Check if the contents of the restore file is
	//   the same as raw.img.

	// Arrange

	// Create a full backup data.
	k8sRepo, rbdRepo, volumeInfo := fake.NewStorage()
	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	rawImageChunkSize := 4096
	targetSnapshotSize := rawImageChunkSize * 2
	snap := rbdRepo.CreateFakeSnapshot(utils.GetUniqueName("snap-"), targetSnapshotSize, time.Now())
	backupInput := testutil.NewBackupInput(k8sRepo, volumeInfo, snap.ID, nil, rawImageChunkSize)
	backupInput.Repo = finRepo
	backupInput.KubernetesRepo = k8sRepo
	backupInput.RBDRepo = rbdRepo
	backupInput.NodeLocalVolumeRepo = nlvRepo

	backup := backup.NewBackup(backupInput)
	err := backup.Perform()
	require.NoError(t, err)

	// Fill raw.img with random data. Although this file has some data
	// stored by fake backup process, we can replace them here because we won't
	// use the original data in the restore process.
	buf := testutil.FillRawImageWithRandomData(t, nlvRepo.GetRawImagePath(), targetSnapshotSize)

	// Create the restore file
	restorePath := testutil.CreateRestoreFileForTest(t, int64(targetSnapshotSize))
	rVol := fake.NewRestoreVolume(restorePath)

	// Act
	r := NewRestore(testutil.NewRestoreInputTemplate(
		backupInput, rVol, rawImageChunkSize, backupInput.TargetSnapshotID))

	err = r.Perform()
	require.NoError(t, err)

	// Assert

	// Verify the contents of the restore file.
	buf2 := make([]byte, targetSnapshotSize)
	restoreFile, err := os.Open(restorePath)
	require.NoError(t, err)
	rn, err := restoreFile.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, targetSnapshotSize, rn)
	require.True(t, bytes.Equal(buf, buf2))

	// Verify the contents of the metadata
	testutil.AssertActionPrivateDataIsEmpty(t, finRepo, backupInput.ActionUID)
	require.Zero(t, len(rVol.AppliedDiffs()))
}

func TestRestore_IncrementalBackup_Success(t *testing.T) {
	// CSATEST-1549
	// Description:
	//   Restore from the incremental backup to the restore file
	//   with no error.
	//
	// Arrange:
	//   - A full backup, `fullBackup`, consists of 2 chunks.
	//   - An incremental backup, `incrementalBackup`, consists of 3 chunks.
	//   - raw.img filled with the random data.
	//     It's size is the same as the full backup.
	//   - The restore file. It's size is the same
	//     as the incremental backup's one.
	//
	// Act:
	//   Run the restore process about the incremental backup.
	//
	// Assert:
	//   Check if the contents of the restore target file is as follows.
	//     chunk0, 1: the same as the fake raw.img.
	//     chunk2: zero filled.

	// Arrange
	k8sRepo, rbdRepo, volumeInfo := fake.NewStorage()
	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	rawImageChunkSize := 4096

	fullSnapshotSize := rawImageChunkSize * 2
	fullSnapshot := rbdRepo.CreateFakeSnapshot(utils.GetUniqueName("snap-"),
		fullSnapshotSize, time.Now())
	fullBackupInput := testutil.NewBackupInput(k8sRepo, volumeInfo,
		fullSnapshot.ID, nil, rawImageChunkSize)
	fullBackupInput.Repo = finRepo
	fullBackupInput.KubernetesRepo = k8sRepo
	fullBackupInput.RBDRepo = rbdRepo
	fullBackupInput.NodeLocalVolumeRepo = nlvRepo

	incrementalSnapshotSize := rawImageChunkSize * 3
	incrementalSnapshot := rbdRepo.CreateFakeSnapshot(utils.GetUniqueName("snap-"),
		incrementalSnapshotSize, time.Now())
	incrementalBackupInput := testutil.NewBackupInput(k8sRepo, volumeInfo,
		incrementalSnapshot.ID, &fullSnapshot.ID, rawImageChunkSize)
	incrementalBackupInput.Repo = finRepo
	incrementalBackupInput.KubernetesRepo = k8sRepo
	incrementalBackupInput.RBDRepo = rbdRepo
	incrementalBackupInput.NodeLocalVolumeRepo = nlvRepo

	// Create a full backup
	fullBackup := backup.NewBackup(fullBackupInput)
	err := fullBackup.Perform()
	require.NoError(t, err)

	// Create an incremental backup
	incrementalBackup := backup.NewBackup(incrementalBackupInput)
	err = incrementalBackup.Perform()
	require.NoError(t, err)

	// Fill raw.img with random data. Although this file has some data
	// stored by fake backup process, we can replace them here because we won't
	// use the original data in the restore process.
	buf := testutil.FillRawImageWithRandomData(t, nlvRepo.GetRawImagePath(), fullSnapshotSize)

	// Create the restore file
	restorePath := testutil.CreateRestoreFileForTest(t, int64(incrementalSnapshotSize))
	rVol := fake.NewRestoreVolume(restorePath)

	// Act
	r := NewRestore(testutil.NewRestoreInputTemplate(
		incrementalBackupInput, rVol, rawImageChunkSize, incrementalBackupInput.TargetSnapshotID))
	err = r.Perform()
	require.NoError(t, err)

	// Assert
	testutil.AssertActionPrivateDataIsEmpty(t, finRepo, fullBackupInput.ActionUID)

	buf2 := make([]byte, incrementalSnapshotSize)
	restoreFile, err := os.Open(rVol.GetPath())
	require.NoError(t, err)
	rn, err := restoreFile.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, incrementalSnapshotSize, rn)
	require.True(t, bytes.Equal(buf, buf2[:fullSnapshotSize]))
	zeroBuf := make([]byte, incrementalSnapshotSize-fullSnapshotSize)
	require.True(t, bytes.Equal(buf2[fullSnapshotSize:], zeroBuf))

	assert.Equal(t, 3, len(rVol.AppliedDiffs()))
	assert.Equal(t, 0, rVol.AppliedDiffs()[0].ReadOffset)
	assert.Equal(t, fullBackupInput.MaxPartSize, rVol.AppliedDiffs()[0].ReadLength)
	assert.Equal(t, fullBackupInput.MaxPartSize, rVol.AppliedDiffs()[1].ReadOffset)
	assert.Equal(t, fullBackupInput.MaxPartSize, rVol.AppliedDiffs()[1].ReadLength)
	assert.Equal(t, fullBackupInput.MaxPartSize*2, rVol.AppliedDiffs()[2].ReadOffset)
	assert.Equal(t, fullBackupInput.MaxPartSize, rVol.AppliedDiffs()[2].ReadLength)
}

func TestRestore_ErrorBusy(t *testing.T) {
	// CSATEST-1550
	// Description:
	//   If another job has already obtained the lock, the process will return an error.

	// Arrange:
	//    The another job has already obtained the lock.
	actionUID := uuid.New().String()
	differentActionUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	err := finRepo.StartOrRestartAction(differentActionUID, model.Backup)
	require.NoError(t, err)

	// Act:
	//    Try to run the restore process.
	restore := NewRestore(&input.Restore{
		Repo:      finRepo,
		ActionUID: actionUID,
	})
	err = restore.Perform()

	// Assert:
	//   The process will return an error.
	assert.ErrorIs(t, err, job.ErrCantLock)
}

func TestRestoreCheckError_SnapshotID_FullBackup(t *testing.T) {
	// CSATEST-1568
	// Description:
	//   If the snapshot ID is invalid, the process will return an error.

	// Arrange:
	//   There is a full backup.
	cfg := setup(t, &setupInput{})
	restoreInput := cfg.restoreInputs[0] // Use the full backup input

	// Act:
	//   Call the restore process specifying a snapshot ID that differs from the backup data.
	restoreInput.TargetSnapshotID = restoreInput.TargetSnapshotID + 1
	r := NewRestore(restoreInput)
	err := r.Perform()

	// Assert:
	//   The process will return an error.
	assert.Error(t, err)
}

func TestRestoreCheckError_SnapshotID_IncrementalBackup(t *testing.T) {
	// CSATEST-1569
	// Description:
	//   If the snapshot ID is invalid, the process will return an error.

	// Arrange:
	//   There are full backup and incremental backup.
	cfg := setup(t, &setupInput{
		enableIncrementalBackup: true,
	})
	restoreInput := cfg.restoreInputs[1] // Use the incremental backup input

	// Act:
	//   Call the restore process specifying a snapshot ID that differs from the backup data.
	restoreInput.TargetSnapshotID = restoreInput.TargetSnapshotID + 1
	r := NewRestore(restoreInput)
	err := r.Perform()

	// Assert:
	//   The process will return an error.
	assert.Error(t, err)
}

func TestRestoreCheckError_PVCUID(t *testing.T) {
	// CSATEST-1572
	// Description:
	//   If the restore target PVC UID is invalid, the process will return an error.

	// Arrange:
	//   There is a full backup.
	cfg := setup(t, &setupInput{})
	restoreInput := cfg.restoreInputs[0]

	// Act:
	//   Call the restore process specifying a PVC UID that differs from the backup data.
	restoreInput.TargetPVCUID = uuid.New().String()
	r := NewRestore(restoreInput)
	err := r.Perform()

	// Assert:
	//   The process will return an error.
	assert.Error(t, err)
}

func TestRestoreCheckError_FullBackupDeleted(t *testing.T) {
	// CSATEST-1574
	// Description:
	//   If no backup exists, the process will return an error.

	// Arrange:
	//   - There is a full backup.
	// Act:
	//   - Delete the backup.
	cfg := setup(t, &setupInput{
		deleteBackups: 1,
	})
	restoreInput := cfg.restoreInputs[0]

	// Act:
	//   Call the restore process with the deleted backup.
	r := NewRestore(restoreInput)
	err := r.Perform()

	// Assert:
	//   The process will return an error.
	assert.Error(t, err)
}

func TestRestoreCheckError_IncrementalBackupDeleted(t *testing.T) {
	// CSATEST-1575
	// Description:
	//   If no backup exists, the process will return an error.

	// Arrange:
	//   There are full backup and incremental backup.
	// Act:
	//   Delete the oldest backup.
	cfg := setup(t, &setupInput{
		enableIncrementalBackup: true,
		deleteBackups:           1,
	})
	restoreInput := cfg.restoreInputs[0]

	// Act:
	//   Call the restore process with the deleted backup.
	r := NewRestore(restoreInput)
	err := r.Perform()

	// Assert:
	//   The process will return an error.
	assert.Error(t, err)
}

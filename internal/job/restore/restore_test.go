package restore

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/testutil"
	backuputil "github.com/cybozu-go/fin/internal/job/testutil/backup"
	"github.com/cybozu-go/fin/test/utils"

	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRestoreInputTemplate(bi *backup.BackupInput,
	rVol model.RestoreVolume, chunkSize, snapID int) *RestoreInput {
	return &RestoreInput{
		Repo:                bi.Repo,
		KubernetesRepo:      bi.KubernetesRepo,
		NodeLocalVolumeRepo: bi.NodeLocalVolumeRepo,
		RestoreVol:          rVol,
		RawImageChunkSize:   int64(chunkSize),
		TargetSnapshotID:    snapID,
		RetryInterval:       bi.RetryInterval,
		ActionUID:           bi.ActionUID,
		TargetPVCUID:        bi.TargetPVCUID,
	}
}

func TestRestoreFromFullBackup_Success(t *testing.T) {
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
	snapID := rbdRepo.CreateFakeSnapshot(utils.GetUniqueName("snap-"), targetSnapshotSize, time.Now())
	backupInput := backuputil.NewBackupInput(k8sRepo, volumeInfo, snapID, nil, rawImageChunkSize)
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
	r := NewRestore(newRestoreInputTemplate(
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

func TestRestoreFromIncrementalBackup_Success(t *testing.T) {
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
	fullBKsnapID := rbdRepo.CreateFakeSnapshot(utils.GetUniqueName("snap-"),
		fullSnapshotSize, time.Now())
	fullBackupInput := backuputil.NewBackupInput(k8sRepo, volumeInfo,
		fullBKsnapID, nil, rawImageChunkSize)
	fullBackupInput.Repo = finRepo
	fullBackupInput.KubernetesRepo = k8sRepo
	fullBackupInput.RBDRepo = rbdRepo
	fullBackupInput.NodeLocalVolumeRepo = nlvRepo

	incrementalSnapshotSize := rawImageChunkSize * 3
	incrementalBKsnapID := rbdRepo.CreateFakeSnapshot(utils.GetUniqueName("snap-"),
		incrementalSnapshotSize, time.Now())
	incrementalBackupInput := backuputil.NewBackupInput(k8sRepo, volumeInfo,
		incrementalBKsnapID, &fullBKsnapID, rawImageChunkSize)
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
	r := NewRestore(newRestoreInputTemplate(
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
	// Arrange
	actionUID := uuid.New().String()
	differentActionUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	err := finRepo.StartOrRestartAction(differentActionUID, model.Backup)
	require.NoError(t, err)

	// Act
	restore := NewRestore(&RestoreInput{
		Repo:      finRepo,
		ActionUID: actionUID,
	})
	err = restore.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

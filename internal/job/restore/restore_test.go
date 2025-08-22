package restore

import (
	"bytes"
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/infrastructure/restore"
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

const (
	defaultMaxPartSize = uint64(4096)
	defaultVolumeSize  = defaultMaxPartSize * 2
	defaultChunkSize   = uint64(4096)
)

type setupInput struct {
	// Split size when executing export-diff
	maxPartSize uint64
	// if true, run incremental backup after full backup, otherwise, only run full backup.
	enableIncrementalBackup bool
	// Virtual volume size when creating backups.
	volumeSize []uint64
	// Restore target block device size.
	blockDeviceSize []uint64
	// chunk size for restore
	chunkSize uint64
	// if > 0, delete backups after creating them.
	// Backups are deleted starting with the oldest ones, up to the specified number of backups.
	deleteBackups int
}

type setupOutput struct {
	nlvRepo model.NodeLocalVolumeRepository
	// RestoreInput instance used when calling the restore module.
	// 0 index is for the full backup, and 1 index is for the incremental backup.
	restoreInputs []*input.Restore
	volumeRaws    [][]byte
}

func setup(t *testing.T, config *setupInput) *setupOutput {
	t.Helper()

	k8sRepo, _, volumeInfo := fake.NewStorage()
	rbdRepo := fake.NewRBDRepository2(volumeInfo.PoolName, volumeInfo.ImageName)
	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	snapIDs := make([]int, 0)
	restoreInputs := make([]*input.Restore, 0)
	volumeRaws := make([][]byte, 0)

	if config.maxPartSize == 0 {
		config.maxPartSize = defaultMaxPartSize
	}
	if config.chunkSize == 0 {
		config.chunkSize = defaultChunkSize
	}
	// Create backups.
	backupCount := 1
	if config.enableIncrementalBackup {
		backupCount = 2
	}
	for i := 0; i < backupCount; i++ {
		volumeSize := defaultVolumeSize
		if len(config.volumeSize) > i {
			volumeSize = config.volumeSize[i]
		}
		snapshot, volumeRaw, err := rbdRepo.CreateSnapshotWithRandomData(utils.GetUniqueName("snap-"), volumeSize)
		require.NoError(t, err)
		snapIDs = append(snapIDs, snapshot.ID)
		volumeRaws = append(volumeRaws, volumeRaw)
		var srcSnapID *int
		if i != 0 {
			srcSnapID = &snapIDs[i-1]
		}
		backupInput := testutil.NewBackupInput(k8sRepo, volumeInfo, snapshot.ID, srcSnapID, config.maxPartSize)
		backupInput.Repo = finRepo
		backupInput.KubernetesRepo = k8sRepo
		backupInput.RBDRepo = rbdRepo
		backupInput.NodeLocalVolumeRepo = nlvRepo

		bk := backup.NewBackup(backupInput)
		err = bk.Perform()
		require.NoError(t, err)

		// Create the restore file
		blockDeviceSize := volumeSize
		if len(config.blockDeviceSize) > i {
			blockDeviceSize = config.blockDeviceSize[i]
		}
		restorePath := testutil.CreateLoopDevice(t, blockDeviceSize)
		rVol := restore.NewRestoreVolume(restorePath)

		restoreInputs = append(restoreInputs, testutil.NewRestoreInputTemplate(
			backupInput, rVol, config.chunkSize, backupInput.TargetSnapshotID))
	}

	// Delete backups if specified.
	for i := 0; i < config.deleteBackups; i++ {
		del := deletion.NewDeletion(&input.Deletion{
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
		nlvRepo:       nlvRepo,
		restoreInputs: restoreInputs,
		volumeRaws:    volumeRaws,
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
	//   - Restore destination volume filled with random data.
	//
	// Act:
	//   Run the restore process.
	//
	// Assert:
	//   Check if the contents of the restore file is
	//   the same as raw.img.

	// Arrange

	// Create a full backup data.
	backupVolumeSize := defaultVolumeSize
	cfg := setup(t, &setupInput{
		volumeSize: []uint64{backupVolumeSize},
	})

	testutil.FillFileRandomData(
		t,
		cfg.restoreInputs[0].RestoreVol.GetPath(),
		backupVolumeSize)

	// Act
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.Perform())

	// Assert

	// Verify the contents of the restore file.
	rawFile, err := os.Open(cfg.nlvRepo.GetRawImagePath())
	require.NoError(t, err)
	defer func() { require.NoError(t, rawFile.Close()) }()

	restoreVolume, err := os.Open(cfg.restoreInputs[0].RestoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, rawFile, restoreVolume)
}

func TestRestore_IncrementalBackup_Success(t *testing.T) {
	// CSATEST-1549
	// Description:
	//   Restore from the incremental backup to the restore file
	//   with no error.
	//
	// Arrange:
	//   - A full backup consists of 2 chunks.
	//   - An incremental backup consists of 3 chunks.
	//   - Restore destination volume filled with random data.
	//     It's size is the same as the 4 chunks.
	//
	// Act:
	//   Run the restore process about the incremental backup.
	//
	// Assert:
	//   Check if the contents of the restore target file is as follows.
	//     chunk 0, 1, 2: the same as the incremental backup.
	//     chunk 3: zero filled.

	// Arrange
	fullBackupVolumeSize := defaultChunkSize * 2
	incrementalVolumeSize := defaultChunkSize * 3
	blockDeviceSize := defaultChunkSize * 4

	cfg := setup(t, &setupInput{
		// Create an incremental backup
		enableIncrementalBackup: true,
		volumeSize: []uint64{
			fullBackupVolumeSize,
			incrementalVolumeSize,
		},
		blockDeviceSize: []uint64{
			blockDeviceSize,
			blockDeviceSize,
		},
	})

	testutil.FillFileRandomData(
		t,
		cfg.restoreInputs[1].RestoreVol.GetPath(),
		defaultChunkSize*4)

	// Act
	r := NewRestore(cfg.restoreInputs[1])
	require.NoError(t, r.Perform())

	// Assert
	restoreVolume, err := os.Open(cfg.restoreInputs[1].RestoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()
	restoreData := make([]byte, blockDeviceSize)
	_, err = restoreVolume.Read(restoreData)
	require.NoError(t, err)

	// Verify the contents of the restore file.
	assert.Equal(t, cfg.volumeRaws[1], restoreData[:incrementalVolumeSize])
	zeroData := make([]byte, defaultChunkSize)
	assert.Equal(t, zeroData, restoreData[incrementalVolumeSize:])
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

func TestRestore_FullBackup_Success_withIncrementalBackup(t *testing.T) {
	// CSATEST-1576
	// Description:
	//   Restore from the full backup to the restore destination volume
	//   with no error.
	//
	// Arrange:
	//   - There are full backup and incremental backup.
	//   - Restore destination volume filled with random data.
	//
	// Act:
	//   Run the restore process about the full backup.
	//
	// Assert:
	//   Check if the contents of the restore destination volume is
	//   the same as the backup target volume.

	// Arrange

	cfg := setup(t, &setupInput{
		enableIncrementalBackup: true,
	})

	testutil.FillFileRandomData(
		t,
		cfg.restoreInputs[0].RestoreVol.GetPath(),
		defaultVolumeSize)

	// Act
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.Perform())

	// Assert
	restoreVolume, err := os.Open(cfg.restoreInputs[0].RestoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, bytes.NewReader(cfg.volumeRaws[0]), restoreVolume)
}

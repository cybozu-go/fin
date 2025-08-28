package restore

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/infrastructure/restore"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/deletion"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/pkg/zeroreader"
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
	blockDeviceSize uint64
	// chunk size for restore
	chunkSize uint64
	// if > 0, delete backups after creating them.
	// Backups are deleted starting with the oldest ones, up to the specified number of backups.
	deleteBackups int
}

type setupOutput struct {
	nlvRepo    model.NodeLocalVolumeRepository
	finRepo    model.FinRepository
	restoreVol model.RestoreVolume
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

	// Create block device & RestoreVolume.
	blockDeviceSize := defaultVolumeSize
	if config.blockDeviceSize > 0 {
		blockDeviceSize = config.blockDeviceSize
	} else if len(config.volumeSize) > 0 {
		blockDeviceSize = config.volumeSize[len(config.volumeSize)-1]
	}
	restorePath := testutil.CreateLoopDevice(t, blockDeviceSize)
	restoreVol := restore.NewRestoreVolume(restorePath)

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

		restoreInputs = append(restoreInputs, testutil.NewRestoreInputTemplate(
			backupInput, restoreVol, config.chunkSize, backupInput.TargetSnapshotID))
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
		finRepo:       finRepo,
		restoreVol:    restoreVol,
		restoreInputs: restoreInputs,
		volumeRaws:    volumeRaws,
	}
}

func createPrivateData(t *testing.T, cfg *setupOutput, restoreInput *input.Restore, data *restorePrivateData) {
	t.Helper()

	finRepo := cfg.finRepo

	err := finRepo.StartOrRestartAction(restoreInput.ActionUID, model.Restore)
	require.NoError(t, err)

	err = setRestorePrivateData(finRepo, restoreInput.ActionUID, data)
	require.NoError(t, err)
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
		cfg.restoreVol.GetPath(),
		backupVolumeSize)

	// Act
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.Perform())

	// Assert

	// Verify the contents of the restore file.
	rawFile, err := os.Open(cfg.nlvRepo.GetRawImagePath())
	require.NoError(t, err)
	defer func() { require.NoError(t, rawFile.Close()) }()

	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
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
		blockDeviceSize: blockDeviceSize,
	})

	testutil.FillFileRandomData(
		t,
		cfg.restoreVol.GetPath(),
		defaultChunkSize*4)

	// Act
	r := NewRestore(cfg.restoreInputs[1])
	require.NoError(t, r.Perform())

	// Assert
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
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
		cfg.restoreVol.GetPath(),
		defaultVolumeSize)

	// Act
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.Perform())

	// Assert
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, bytes.NewReader(cfg.volumeRaws[0]), restoreVolume)
}

func TestRestore_phase_skip(t *testing.T) {
	// CSATEST-1577
	// Description:
	//   When restoring from a halfway point, skip processing according to the phase.
	//
	// Arrange:
	//   There is a full backup.
	//   The phase of private_data in the action_status table is set to `completed`.
	//
	// Act:
	//   Fill destination volume with random data.
	//   Run the restore process about the full backup.
	//
	// Assert:
	//   The process will return nil.
	//   The contents of the restore volume have not been restored (random data remains).

	// Arrange
	backupVolumeSize := defaultVolumeSize
	cfg := setup(t, &setupInput{
		volumeSize: []uint64{backupVolumeSize},
	})
	createPrivateData(t, cfg, cfg.restoreInputs[0], &restorePrivateData{
		Phase: Completed,
	})

	// Act
	randomData := testutil.FillFileRandomData(
		t,
		cfg.restoreVol.GetPath(),
		backupVolumeSize)

	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.Perform())

	// Assert

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, bytes.NewReader(randomData), restoreVolume)
}

func TestRestore_doInitialPhase_success(t *testing.T) {
	// CSATEST-1578
	// Description:
	//   Confirm normal processing for doInitialPhase.
	//
	// Arrange:
	//   There is a full backup.
	//   The phase of private_data in the action_status table is empty.
	//
	// Act:
	//   Run doInitialPhase process about the full backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table is set to `discard`.

	// Arrange
	cfg := setup(t, &setupInput{})
	restorePD := &restorePrivateData{}
	createPrivateData(t, cfg, cfg.restoreInputs[0], restorePD)

	// Act
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.doInitialPhase(restorePD))

	// Assert
	restorePD, err := getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[0].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, Discard, restorePD.Phase)
}

func TestRestore_doInitialPhase_skip(t *testing.T) {
	// CSATEST-1579
	// Description:
	//   Confirm skip processing for doInitialPhase.
	//
	// Arrange:
	//   There is a full backup.
	//   The phase of private_data in the action_status table is `discard`.
	//
	// Act:
	//   Run doInitialPhase process about the full backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table remains `discard`.

	// Arrange
	cfg := setup(t, &setupInput{})
	restorePD := &restorePrivateData{
		Phase: Discard,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[0], restorePD)

	// Act
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.doInitialPhase(restorePD))

	// Assert
	restorePD, err := getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[0].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, Discard, restorePD.Phase)
}

func TestRestore_doDiscardPhase_success(t *testing.T) {
	// CSATEST-1580
	// Description:
	//   Confirm normal processing for doDiscardPhase.
	//
	// Arrange:
	//   There is a full backup.
	//   The phase of private_data in the action_status table is set to `discard`.
	//
	// Act:
	//   Fill destination volume with random data.
	//   Run doDiscardPhase process about the full backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table is set to `restore_raw_image`.
	//   The contents of the restore volume have been zero filled.

	// Arrange
	cfg := setup(t, &setupInput{})
	restorePD := &restorePrivateData{
		Phase: Discard,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[0], restorePD)

	// Act
	testutil.FillFileRandomData(
		t,
		cfg.restoreVol.GetPath(),
		defaultVolumeSize)
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.doDiscardPhase(restorePD))

	// Assert
	restorePD, err := getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[0].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, RestoreRawImage, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, io.LimitReader(zeroreader.New(), int64(defaultVolumeSize)), restoreVolume)
}

func TestRestore_doDiscardPhase_skip(t *testing.T) {
	// CSATEST-1581
	// Description:
	//   Confirm skip processing for doDiscardPhase.
	//
	// Arrange:
	//   There is a full backup.
	//   The phase of private_data in the action_status table is set to `restore_raw_image`.
	//
	// Act:
	//   Fill destination volume with random data.
	//   Run doDiscardPhase process about the full backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table is keeping `restore_raw_image`.
	//   The contents of the restore volume have not been restored (random data remains).

	// Arrange
	cfg := setup(t, &setupInput{})
	restorePD := &restorePrivateData{
		Phase: RestoreRawImage,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[0], restorePD)

	// Act
	randomData := testutil.FillFileRandomData(
		t,
		cfg.restoreVol.GetPath(),
		defaultVolumeSize)

	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.doDiscardPhase(restorePD))

	// Assert
	restorePD, err := getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[0].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, RestoreRawImage, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()
	utils.CompareReaders(t, bytes.NewReader(randomData), restoreVolume)
}

func TestRestore_doRestoreRawImagePhase_success(t *testing.T) {
	// CSATEST-1582
	// Description:
	//   Confirm normal processing for doRestoreRawImagePhase.
	//
	// Arrange:
	//   There is a full backup.
	//   The phase of private_data in the action_status table is set to `restore_raw_image` and
	//   nextRawImageChunk is 0.
	//   The size of the restore destination volume is larger than that of the backup target volume.
	//
	// Act:
	//   Restore destination volume is zero filled.
	//   Run doRestoreRawImagePhase process about the full backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table is set to `completed`.
	//   The contents of the restore volume have been restored.
	//   The area of the restore volume that exceeds the size of the backup target volume is zero filled.

	// Arrange
	cfg := setup(t, &setupInput{
		blockDeviceSize: defaultVolumeSize + defaultMaxPartSize,
	})
	restorePD := &restorePrivateData{
		Phase:             RestoreRawImage,
		NextRawImageChunk: 0,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[0], restorePD)

	require.NoError(t, cfg.restoreVol.ZeroOut())
	metadata, err := job.GetBackupMetadata(cfg.finRepo)
	require.NoError(t, err)
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.doRestoreRawImagePhase(restorePD, metadata.Raw))

	// Assert
	restorePD, err = getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[0].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, Completed, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreInputs[0].RestoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	expectedData := make([]byte, defaultVolumeSize+defaultMaxPartSize)
	copy(expectedData, cfg.volumeRaws[0])
	utils.CompareReaders(t, bytes.NewReader(expectedData), restoreVolume)
}

func TestRestore_doRestoreRawImagePhase_resume(t *testing.T) {
	// CSATEST-1583
	// Description:
	//   Confirm resume processing for doRestoreRawImagePhase.
	//
	// Arrange:
	//   There is a full backup.
	//   The phase of private_data in the action_status table is set to `restore_raw_image` and
	//   nextRawImageChunk is 1.
	//
	// Act:
	//   Restore destination volume is zero filled.
	//   Restore destination volume's head 1 chunk is filled with random data.
	//   Run doRestoreRawImagePhase process about the full backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table is set to `completed`.
	//   The contents of the restore volume have been restored, excluding for head 1 chunk.

	// Arrange
	cfg := setup(t, &setupInput{})
	restorePD := &restorePrivateData{
		Phase:             RestoreRawImage,
		NextRawImageChunk: 1,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[0], restorePD)

	// Act
	require.NoError(t, cfg.restoreVol.ZeroOut())
	randomData := testutil.FillFileRandomData(
		t,
		cfg.restoreVol.GetPath(),
		defaultChunkSize)
	metadata, err := job.GetBackupMetadata(cfg.finRepo)
	require.NoError(t, err)
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.doRestoreRawImagePhase(restorePD, metadata.Raw))

	// Assert
	restorePD, err = getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[0].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, Completed, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	expectedData := make([]byte, defaultVolumeSize)
	copy(expectedData, randomData[:defaultChunkSize])
	copy(expectedData[defaultChunkSize:], cfg.volumeRaws[0][defaultChunkSize:])
	utils.CompareReaders(t, bytes.NewReader(expectedData), restoreVolume)
}

func TestRestore_doRestoreRawImagePhase_error(t *testing.T) {
	// CSATEST-1585
	// Description:
	//   Confirm error processing for doRestoreRawImagePhase.
	//
	// Arrange:
	//   There is a full backup but raw.img file is not found.
	//   The phase of private_data in the action_status table is set to `restore_raw_image`.
	//
	// Act:
	//   Overwrite destination volume filled with random data.
	//   Run doRestoreRawImagePhase process about the full backup.
	//
	// Assert:
	//   The process will return an error.
	//   The phase of private_data in the action_status table remains `restore_raw_image`.
	//   The contents of the restore volume have not been restored (random data remains).

	// Arrange
	cfg := setup(t, &setupInput{})
	require.NoError(t, cfg.nlvRepo.RemoveRawImage())
	restorePD := &restorePrivateData{
		Phase: RestoreRawImage,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[0], restorePD)

	// Act
	randomData := testutil.FillFileRandomData(
		t,
		cfg.restoreVol.GetPath(),
		defaultVolumeSize)
	metadata, err := job.GetBackupMetadata(cfg.finRepo)
	require.NoError(t, err)
	r := NewRestore(cfg.restoreInputs[0])
	require.Error(t, r.doRestoreRawImagePhase(restorePD, metadata.Raw))

	// Assert
	restorePD, err = getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[0].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, RestoreRawImage, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, bytes.NewReader(randomData), restoreVolume)
}

func TestRestore_doRestoreRawImagePhase_skip(t *testing.T) {
	// CSATEST-1586
	// Description:
	//   Confirm skip processing for doRestoreRawImagePhase.
	//
	// Arrange:
	//   There is a full backup.
	//   The phase of private_data in the action_status table is set to `restore_diff`.
	//
	// Act:
	//   Overwrite destination volume filled with random data.
	//   Run doRestoreRawImagePhase process about the full backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table remains `restore_diff`.
	//   The contents of the restore volume have not been restored (random data remains).

	// Arrange
	cfg := setup(t, &setupInput{})
	restorePD := &restorePrivateData{
		Phase: RestoreDiff,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[0], restorePD)

	// Act
	randomData := testutil.FillFileRandomData(
		t,
		cfg.restoreVol.GetPath(),
		defaultVolumeSize)
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.doRestoreRawImagePhase(restorePD, nil))

	// Assert
	restorePD, err := getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[0].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, RestoreDiff, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, bytes.NewReader(randomData), restoreVolume)
}

func TestRestore_doRestoreDiffPhase_success(t *testing.T) {
	// CSATEST-1587
	// Description:
	//   Confirm normal processing for doRestoreDiffPhase.
	//
	// Arrange:
	//   There is a full backup and incremental backup.
	//   Restore destination volume is restored with the full backup.
	//   The phase of private_data in the action_status table is set to `restore_diff` and
	//   nextDiffPart is 0.
	//
	// Act:
	//   Run doRestoreDiffPhase process about the incremental backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table is set to `completed`.
	//   The contents of the restore volume have been restored.

	// Arrange
	cfg := setup(t, &setupInput{
		enableIncrementalBackup: true,
	})
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.Perform())

	restorePD := &restorePrivateData{
		Phase:        RestoreDiff,
		NextDiffPart: 0,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[1], restorePD)

	// Act
	r = NewRestore(cfg.restoreInputs[1])
	metadata, err := job.GetBackupMetadata(cfg.finRepo)
	require.NoError(t, err)
	require.NoError(t, r.doRestoreDiffPhase(restorePD, metadata))

	// Assert
	restorePD, err = getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[1].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, Completed, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, bytes.NewReader(cfg.volumeRaws[1]), restoreVolume)
}

func TestRestore_doRestoreDiffPhase_resume(t *testing.T) {
	// CSATEST-1588
	// Description:
	//   Confirm normal processing for doRestoreDiffPhase.
	//
	// Arrange:
	//   There is a full backup and incremental backup.
	//   Restore destination volume is restored with the full backup.
	//   The phase of private_data in the action_status table is set to `restore_diff` and
	//   nextDiffPart is 1.
	//
	// Act:
	//   Run doRestoreDiffPhase process about the incremental backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table is set to `completed`.
	//   The contents of the restore volume have been restored, except for the first part.

	// Arrange
	cfg := setup(t, &setupInput{
		enableIncrementalBackup: true,
	})
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.Perform())

	restorePD := &restorePrivateData{
		Phase:        RestoreDiff,
		NextDiffPart: 1,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[1], restorePD)

	// Act
	r = NewRestore(cfg.restoreInputs[1])
	metadata, err := job.GetBackupMetadata(cfg.finRepo)
	require.NoError(t, err)
	require.NoError(t, r.doRestoreDiffPhase(restorePD, metadata))

	// Assert
	restorePD, err = getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[1].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, Completed, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	expectedData := make([]byte, defaultVolumeSize)
	copy(expectedData, cfg.volumeRaws[0][:defaultMaxPartSize])
	copy(expectedData[defaultMaxPartSize:], cfg.volumeRaws[1][defaultMaxPartSize:])
	utils.CompareReaders(t, bytes.NewReader(expectedData), restoreVolume)
}

func TestRestore_doRestoreDiffPhase_error(t *testing.T) {
	// CSATEST-1589
	// Description:
	//   Confirm error processing for doRestoreDiffPhase.
	//
	// Arrange:
	//   There are a full backup and an incremental backup, but the incremental backup data will fail to be read.
	//   Restore destination volume is restored with the full backup.
	//   The phase of private_data in the action_status table is set to `restore_diff` and
	//   nextDiffPart is 0.
	//
	// Act:
	//   Run doRestoreDiffPhase process about the incremental backup.
	//
	// Assert:
	//   The process will return an error.
	//   The phase of private_data in the action_status table remains `restore_diff`.
	//   The contents of the restore volume have not been restored (full backup data remains).

	// Arrange
	cfg := setup(t, &setupInput{
		enableIncrementalBackup: true,
	})
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.Perform())

	err := cfg.nlvRepo.RemoveDiffDirRecursively(cfg.restoreInputs[1].TargetSnapshotID)
	require.NoError(t, err)

	restorePD := &restorePrivateData{
		Phase:        RestoreDiff,
		NextDiffPart: 0,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[1], restorePD)

	// Act
	r = NewRestore(cfg.restoreInputs[1])
	metadata, err := job.GetBackupMetadata(cfg.finRepo)
	require.NoError(t, err)
	assert.Error(t, r.doRestoreDiffPhase(restorePD, metadata))

	// Assert
	restorePD, err = getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[1].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, RestoreDiff, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, bytes.NewReader(cfg.volumeRaws[0]), restoreVolume)
}

func TestRestore_doRestoreDiffPhase_skip(t *testing.T) {
	// CSATEST-1590
	// Description:
	//   Confirm skip processing for doRestoreDiffPhase.
	//
	// Arrange:
	//   There is a full backup and incremental backup.
	//   Restore destination volume is restored with the full backup.
	//   The phase of private_data in the action_status table is set to `completed`.
	//
	// Act:
	//   Run doRestoreDiffPhase process about the incremental backup.
	//
	// Assert:
	//   The process will return nil.
	//   The phase of private_data in the action_status table remains `completed`.
	//   The contents of the restore volume have not been restored (full backup data remains).

	// Arrange
	cfg := setup(t, &setupInput{
		enableIncrementalBackup: true,
	})
	r := NewRestore(cfg.restoreInputs[0])
	require.NoError(t, r.Perform())

	restorePD := &restorePrivateData{
		Phase: Completed,
	}
	createPrivateData(t, cfg, cfg.restoreInputs[1], restorePD)

	// Act
	r = NewRestore(cfg.restoreInputs[1])
	require.NoError(t, r.doRestoreDiffPhase(restorePD, nil))

	// Assert
	restorePD, err := getRestorePrivateData(cfg.finRepo, cfg.restoreInputs[1].ActionUID)
	require.NoError(t, err)
	assert.Equal(t, Completed, restorePD.Phase)

	// Verify the contents of the restore file.
	restoreVolume, err := os.Open(cfg.restoreVol.GetPath())
	require.NoError(t, err)
	defer func() { require.NoError(t, restoreVolume.Close()) }()

	utils.CompareReaders(t, bytes.NewReader(cfg.volumeRaws[0]), restoreVolume)
}

package cleanup

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putFakePVCAndPV(t *testing.T, nlvRoot string) {
	t.Helper()

	pvc, err := os.Create(filepath.Join(nlvRoot, "pvc.yaml"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pvc.Close()
		_ = os.Remove(filepath.Join(nlvRoot, "pvc.yaml"))
	})
	pv, err := os.Create(filepath.Join(nlvRoot, "pv.yaml"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pv.Close()
		_ = os.Remove(filepath.Join(nlvRoot, "pv.yaml"))
	})
}

func putFakeRawImage(t *testing.T, nlvRoot string) {
	t.Helper()

	raw, err := os.Create(filepath.Join(nlvRoot, "raw.img"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = raw.Close()
		_ = os.Remove(filepath.Join(nlvRoot, "raw.img"))
	})
}

func assertAllFullBackupFilesExist(t *testing.T, nlvRoot string) {
	t.Helper()

	assert.FileExists(t, filepath.Join(nlvRoot, "pvc.yaml"))
	assert.FileExists(t, filepath.Join(nlvRoot, "pv.yaml"))
	assert.FileExists(t, filepath.Join(nlvRoot, "raw.img"))
}

func assertAllFullBackupFilesDoNotExist(t *testing.T, nlvRoot string) {
	t.Helper()

	assert.NoFileExists(t, filepath.Join(nlvRoot, "pvc.yaml"))
	assert.NoFileExists(t, filepath.Join(nlvRoot, "pv.yaml"))
	assert.NoFileExists(t, filepath.Join(nlvRoot, "raw.img"))
}

func assertDiffDirExist(t *testing.T, nlvRoot string, snapID int) {
	t.Helper()

	assert.DirExists(t, filepath.Join(nlvRoot, "diff", strconv.Itoa(snapID)))
}

func assertDiffDirDoesNotExist(t *testing.T, nlvRoot string, snapID int) {
	t.Helper()

	assert.NoDirExists(t, filepath.Join(nlvRoot, "diff", strconv.Itoa(snapID)))
}

// setFakeBackupMetadata sets backup_metadata table with the given
// raw and diff snapshot IDs. If snapshots id is < 0, corresponding
// entry is not set.
func setFakeBackupMetadata(t *testing.T, finRepo model.FinRepository, pvcUID string, rawSnapID, diffSnapID int) {
	t.Helper()

	metadata := &job.BackupMetadata{
		PVCUID:       pvcUID,
		RBDImageName: "test-image",
	}
	if rawSnapID >= 0 {
		metadata.Raw = &job.BackupMetadataEntry{
			SnapID: rawSnapID,
		}
	}
	if diffSnapID >= 0 {
		metadata.Diff = []*job.BackupMetadataEntry{
			{
				SnapID: diffSnapID,
			},
		}
	}
	require.NoError(t, job.SetBackupMetadata(finRepo, metadata))
}

func TestCleanup_OnStoringDiffForFullBackup_Success(t *testing.T) {
	// CSATEST-1567
	// Description:
	//   Emulate the cleanup on storing diff for full backup.
	//
	// Arrange:
	//   1. Start a backup action.
	//   2. Create pvc.yaml and pv.yaml
	//   3. Create the diff directory for full backup.
	//   4. backup_metadata table is empty.
	//
	// Act:
	//   Run the cleanup process.
	//
	// Assert:
	//   - No in-progress action.
	//   - backup_metadata table is empty.
	//   - Diff directory doesn't exist.
	//   - Both pvc.yaml and pv.yaml don't exist.

	// Arrange

	// Emulate on-going full backup
	backupActionUID := uuid.New().String()
	pvcUID := uuid.New().String()
	nlvRepo, finRepo, nlvRoot := testutil.CreateNLVAndFinRepoForTest(t)
	require.NoError(t, finRepo.StartOrRestartAction(backupActionUID, model.Backup))

	snapID := 1
	putFakePVCAndPV(t, nlvRoot)
	require.NoError(t, nlvRepo.MakeDiffDir(snapID))

	input := input.Cleanup{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           backupActionUID,
		TargetSnapshotID:    snapID,
		TargetPVCUID:        pvcUID,
	}

	// Act
	cleanupJob := NewCleanup(&input)
	require.NoError(t, cleanupJob.Perform())

	// Assert
	_, err := job.GetBackupMetadata(finRepo)
	assert.ErrorIs(t, err, model.ErrNotFound)

	assertDiffDirDoesNotExist(t, nlvRoot, snapID)
	assertAllFullBackupFilesDoNotExist(t, nlvRoot)

	anotherActionUID := uuid.New().String()
	require.NoError(t, finRepo.StartOrRestartAction(anotherActionUID, model.Backup))
}

func TestCleanup_OnApplyingDiffForFullBackup_Success(t *testing.T) {
	// CSATEST-1599
	// Description:
	//   Emulate the cleanup on applying diff for full backup.
	//
	// Arrange:
	//   1. Start a backup action.
	//   2. Create raw.img, pvc.yaml, and pv.yaml
	//   3. Create the diff directory for full backup.
	//   4. backup_metadata table has an entry. Its raw column is
	//      empty and diff column is not empty.
	//
	// Act:
	//   Run the cleanup process.
	//
	// Assert:
	//   - backup_metadata table is empty.
	//   - Diff directory doesn't exist.
	//   - All raw.img, pvc.yaml, and pv.yaml don't exist.
	//   - No in-progress action.

	// Arrange

	// Emulate on-going full backup
	backupActionUID := uuid.New().String()
	pvcUID := uuid.New().String()
	nlvRepo, finRepo, nlvRoot := testutil.CreateNLVAndFinRepoForTest(t)
	require.NoError(t, finRepo.StartOrRestartAction(backupActionUID, model.Backup))

	snapID := 1
	putFakePVCAndPV(t, nlvRoot)
	setFakeBackupMetadata(t, finRepo, pvcUID, -1, snapID)
	putFakeRawImage(t, nlvRoot)
	require.NoError(t, nlvRepo.MakeDiffDir(snapID))

	input := input.Cleanup{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           backupActionUID,
		TargetSnapshotID:    snapID,
		TargetPVCUID:        pvcUID,
	}

	// Act
	cleanupJob := NewCleanup(&input)
	require.NoError(t, cleanupJob.Perform())

	// Assert
	_, err := job.GetBackupMetadata(finRepo)
	assert.ErrorIs(t, err, model.ErrNotFound)

	assertDiffDirDoesNotExist(t, nlvRoot, snapID)
	assertAllFullBackupFilesDoNotExist(t, nlvRoot)

	anotherActionUID := uuid.New().String()
	require.NoError(t, finRepo.StartOrRestartAction(anotherActionUID, model.Backup))
}

func TestCleanup_FullBackupExistAndNoOngoingIncrementalBackup_Success(t *testing.T) {
	// CSATEST-1601
	// Description:
	//   Emulate the cleanup when full backup exists and no ongoing incremental backup.
	//
	// Arrange:
	//   1. Create raw.img, pvc.yaml, and pv.yaml
	//   2. backup_metadata table has an entry. Its raw column is
	//      not empty and diff column is empty.
	//
	// Act:
	//   Run the cleanup process.
	//
	// Assert:
	//   - backup_metadata table is not empty.
	//   - All raw.img, pvc.yaml, and pv.yaml exist.
	//   - No in-progress action.

	// Arrange
	backupActionUID := uuid.New().String()
	pvcUID := uuid.New().String()
	nlvRepo, finRepo, nlvRoot := testutil.CreateNLVAndFinRepoForTest(t)

	snapID := 1
	putFakePVCAndPV(t, nlvRoot)
	putFakeRawImage(t, nlvRoot)
	setFakeBackupMetadata(t, finRepo, pvcUID, snapID, -1)

	input := input.Cleanup{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           backupActionUID,
		TargetSnapshotID:    snapID,
		TargetPVCUID:        pvcUID,
	}

	// Act
	cleanupJob := NewCleanup(&input)
	require.NoError(t, cleanupJob.Perform())

	// Assert
	_, err := job.GetBackupMetadata(finRepo)
	assert.NoError(t, err)

	assertAllFullBackupFilesExist(t, nlvRoot)

	anotherActionUID := uuid.New().String()
	require.NoError(t, finRepo.StartOrRestartAction(anotherActionUID, model.Backup))
}

func TestCleanup_OnIncrementalBackup_Success(t *testing.T) {
	// CSATEST-1602
	// Description:
	//   Emulate the cleanup on incremental backup.
	//
	// Arrange:
	//   1. Create raw.img, pvc.yaml, and pv.yaml
	//   2. Create the diff directory for incremental backup.
	//   3. backup_metadata table has an entry. Its raw column is
	//      not empty and diff column is empty.
	//
	// Act:
	//   Run the cleanup process.
	//
	// Assert:
	//   - backup_metadata table is not empty.
	//   - All raw.img, pvc.yaml, and pv.yaml exist.
	//   - Diff directory doesn't exist.
	//   - No in-progress action.

	// Arrange

	// Emulate on-going incremental backup
	backupActionUID := uuid.New().String()
	pvcUID := uuid.New().String()
	nlvRepo, finRepo, nlvRoot := testutil.CreateNLVAndFinRepoForTest(t)
	require.NoError(t, finRepo.StartOrRestartAction(backupActionUID, model.Backup))

	previousSnapID := 1
	snapID := 2
	putFakePVCAndPV(t, nlvRoot)
	putFakeRawImage(t, nlvRoot)
	require.NoError(t, nlvRepo.MakeDiffDir(snapID))
	setFakeBackupMetadata(t, finRepo, pvcUID, previousSnapID, -1)

	input := input.Cleanup{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           backupActionUID,
		TargetSnapshotID:    snapID,
		TargetPVCUID:        pvcUID,
	}

	// Act
	cleanupJob := NewCleanup(&input)
	require.NoError(t, cleanupJob.Perform())

	// Assert
	_, err := job.GetBackupMetadata(finRepo)
	assert.NoError(t, err)

	assertAllFullBackupFilesExist(t, nlvRoot)
	assertDiffDirDoesNotExist(t, nlvRoot, snapID)

	anotherActionUID := uuid.New().String()
	require.NoError(t, finRepo.StartOrRestartAction(anotherActionUID, model.Backup))
}

func TestCleanup_BothFullBackupAndIncrementalBackupExist_Success(t *testing.T) {
	// CSATEST-1603
	// Description:
	//   Emulate the cleanup when both full backup and incremental backup exist.
	//
	// Arrange:
	//   1. Create raw.img, pvc.yaml, pv.yaml, diff directory
	//   2. backup_metadata table has an entry. Its both raw column
	//      and diff column are not empty.
	//
	// Act:
	//   Run the cleanup process.
	//
	// Assert:
	//   - backup_metadata table is not empty.
	//   - All raw.img, pvc.yaml, pv.yaml, diff directory exist.
	//   - No in-progress action.

	// Arrange

	backupActionUID := uuid.New().String()
	pvcUID := uuid.New().String()
	nlvRepo, finRepo, nlvRoot := testutil.CreateNLVAndFinRepoForTest(t)

	previousSnapID := 1
	snapID := 2
	putFakePVCAndPV(t, nlvRoot)
	require.NoError(t, nlvRepo.MakeDiffDir(snapID))
	putFakeRawImage(t, nlvRoot)
	setFakeBackupMetadata(t, finRepo, pvcUID, previousSnapID, snapID)

	input := input.Cleanup{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           backupActionUID,
		TargetSnapshotID:    previousSnapID,
		TargetPVCUID:        pvcUID,
	}

	// Act
	cleanupJob := NewCleanup(&input)
	require.NoError(t, cleanupJob.Perform())

	// Assert
	_, err := job.GetBackupMetadata(finRepo)
	assert.NoError(t, err)

	assertAllFullBackupFilesExist(t, nlvRoot)
	assertDiffDirExist(t, nlvRoot, snapID)

	anotherActionUID := uuid.New().String()
	require.NoError(t, finRepo.StartOrRestartAction(anotherActionUID, model.Backup))
}

func TestCleanup_InstantVerifyImageExists_Success(t *testing.T) {
	// Arrange
	backupActionUID := uuid.New().String()
	pvcUID := uuid.New().String()
	nlvRepo, finRepo, nlvRoot := testutil.CreateNLVAndFinRepoForTest(t)
	require.NoError(t, finRepo.StartOrRestartAction(backupActionUID, model.Verification))

	snapID := 1
	putFakePVCAndPV(t, nlvRoot)
	putFakeRawImage(t, nlvRoot)
	setFakeBackupMetadata(t, finRepo, pvcUID, snapID, -1)

	// Create instant verify image
	img, err := os.Create(nlvRepo.GetInstantVerifyImagePath())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = img.Close()
		_ = os.Remove(nlvRepo.GetInstantVerifyImagePath())
	})

	input := input.Cleanup{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           backupActionUID,
		TargetSnapshotID:    snapID,
		TargetPVCUID:        pvcUID,
	}

	// Act
	cleanupJob := NewCleanup(&input)
	require.NoError(t, cleanupJob.Perform())

	// Assert
	_, err = job.GetBackupMetadata(finRepo)
	require.NoError(t, err)
	anotherActionUID := uuid.New().String()
	require.NoError(t, finRepo.StartOrRestartAction(anotherActionUID, model.Backup))

	assertAllFullBackupFilesExist(t, nlvRoot)
	assert.NoFileExists(t, nlvRepo.GetInstantVerifyImagePath())
}

func TestCleanup_PVCUIDMismatch_Failure(t *testing.T) {
	// CSATEST-1598
	// Description:
	//   Emulate the cleanup when the PVC UID in the backup
	//   metadata does not match the one passed by the caller.
	//
	// Arrange:
	//   1. Create raw.img, pvc.yaml, and pv.yaml
	//   2. backup_metadata table has an entry. Its raw column
	//      is not empty and diff column is empty.
	//
	// Act:
	//   Run the cleanup process with a different PVC UID.
	//
	// Assert:
	//   - Cleanup process fails with error indicating PVC UID mismatch.

	// Arrange
	backupActionUID := uuid.New().String()
	pvcUID := uuid.New().String()
	nlvRepo, finRepo, nlvRoot := testutil.CreateNLVAndFinRepoForTest(t)

	snapID := 1
	putFakePVCAndPV(t, nlvRoot)
	putFakeRawImage(t, nlvRoot)
	setFakeBackupMetadata(t, finRepo, pvcUID, snapID, -1)

	anotherPVCUID := uuid.New().String()
	input := input.Cleanup{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           backupActionUID,
		TargetSnapshotID:    snapID,
		TargetPVCUID:        anotherPVCUID,
	}

	// Act
	cleanupJob := NewCleanup(&input)

	// Assert
	require.ErrorContains(t, cleanupJob.Perform(), "target PVC UID")
}

func TestCleanup_ErrorBusy(t *testing.T) {
	// CSATEST-1566
	// Description:
	//   Test cleanup behavior when repository is busy with another action.
	//
	// Arrange:
	//   Repository is already locked by a different action UID.
	//
	// Act:
	//   Run the cleanup action with a different action UID.
	//
	// Assert:
	//   Check if the cleanup fails with ErrCantLock error.

	// Arrange
	actionUID := uuid.New().String()
	differentActionUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)
	require.NoError(t, finRepo.StartOrRestartAction(differentActionUID, model.Cleanup))

	// Act
	backup := NewCleanup(&input.Cleanup{
		Repo:      finRepo,
		ActionUID: actionUID,
	})
	err := backup.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

package cleanup

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putFakePVCAndPV(t *testing.T, root string) {
	t.Helper()

	pvc, err := os.Create(filepath.Join(root, "pvc.yaml"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pvc.Close()
		_ = os.Remove(filepath.Join(root, "pvc.yaml"))
	})
	pv, err := os.Create(filepath.Join(root, "pv.yaml"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pv.Close()
		_ = os.Remove(filepath.Join(root, "pv.yaml"))
	})
}

func assertAllFullBackupDataDoNotExist(t *testing.T, root string) {
	t.Helper()

	assert.NoFileExists(t, filepath.Join(root, "pvc.yaml"))
	assert.NoFileExists(t, filepath.Join(root, "pv.yaml"))
	assert.NoFileExists(t, filepath.Join(root, "raw.img"))
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
	//
	// Act:
	//   Run the cleanup process.
	//
	// Assert:
	//   - No in-progress action.
	//   - Diff directory doesn't exist.
	//   - pvc.yaml and pv.yaml are removed.

	// Arrange

	// Emulate on-going full backup
	backupActionUID := uuid.New().String()
	nlvRepo, finRepo, root := testutil.CreateNLVAndFinRepoForTest(t)
	err := finRepo.StartOrRestartAction(backupActionUID, model.Backup)
	require.NoError(t, err)

	snapID := 1
	err = nlvRepo.MakeDiffDir(snapID)
	require.NoError(t, err)
	putFakePVCAndPV(t, root)

	input := input.Cleanup{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           backupActionUID,
		TargetSnapshotID:    1,
		TargetPVCUID:        uuid.New().String(),
	}

	// Act
	cleanupJob := NewCleanup(&input)
	err = cleanupJob.Perform()
	require.NoError(t, err)

	// Assert
	_, err = job.GetBackupMetadata(finRepo)
	assert.ErrorIs(t, err, model.ErrNotFound)

	assert.NoFileExists(t, filepath.Join(root, fmt.Sprintf("diff/%d", snapID)))
	assertAllFullBackupDataDoNotExist(t, root)
	anotherActionUID := uuid.New().String()

	err = finRepo.StartOrRestartAction(anotherActionUID, model.Backup)
	require.NoError(t, err)
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

	err := finRepo.StartOrRestartAction(differentActionUID, model.Cleanup)
	require.NoError(t, err)

	// Act
	backup := NewCleanup(&input.Cleanup{
		Repo:      finRepo,
		ActionUID: actionUID,
	})
	err = backup.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

package cleanup_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/cleanup"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanup_OnStoringDiffForFullBackup_Success(t *testing.T) {
	// Description:
	//   Emulate the cleanup on storing diff for full backup.
	//
	// Arrange:
	//   1. Start a backup action.
	//   2. Create the diff directory for full backup.
	//
	// Act:
	//   Run the cleanup process.
	//
	// Assert:
	//   - No in-progress action.
	//   - Diff directory doesn't exist.

	// Arrange

	// Emulate on-going full backup
	backupActionUID := uuid.New().String()
	nlvRepo, finRepo, root := testutil.CreateNLVAndFinRepoForTest(t)
	err := finRepo.StartOrRestartAction(backupActionUID, model.Backup)
	require.NoError(t, err)

	snapID := 1
	err = nlvRepo.MakeDiffDir(snapID)
	require.NoError(t, err)

	input := input.Cleanup{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           backupActionUID,
		TargetSnapshotID:    1,
		TargetPVCUID:        uuid.New().String(),
	}

	// Act
	cleanupJob := cleanup.NewCleanup(&input)
	err = cleanupJob.Perform()
	require.NoError(t, err)

	// Assert
	_, err = job.GetBackupMetadata(finRepo)
	assert.ErrorIs(t, err, model.ErrNotFound)

	_, err = os.Stat(filepath.Join(root, fmt.Sprintf("diff/%d", snapID)))
	assert.ErrorIs(t, err, fs.ErrNotExist)

	anotherActionUID := uuid.New().String()
	err = finRepo.StartOrRestartAction(anotherActionUID, model.Backup)
	require.NoError(t, err)
}

func TestCleanup_ErrorBusy(t *testing.T) {
	// Arrange
	actionUID := uuid.New().String()
	differentActionUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	err := finRepo.StartOrRestartAction(differentActionUID, model.Cleanup)
	require.NoError(t, err)

	// Act
	backup := cleanup.NewCleanup(&input.Cleanup{
		Repo:      finRepo,
		ActionUID: actionUID,
	})
	err = backup.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

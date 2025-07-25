package deletion_test

import (
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/deletion"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelete_RawOnlyCase_Success(t *testing.T) {
	// Description:
	//   Delete a backup that contains only raw data with no error.
	//
	// Arrange:
	//   A backup metadata with only raw entry and no diff entries.
	//   A raw.img file exists in the node local volume.
	//
	// Act:
	//   Run the deletion process to delete the raw backup.
	//
	// Assert:
	//   Check if the backup metadata is updated (raw becomes nil) and
	//   the raw image file is deleted from the file system.

	// Arrange
	actionUID := uuid.New().String()
	targetSnapshotID := 1
	targetPVCUID := uuid.New().String()

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)
	rbdRepo := fake.NewRBDRepository("", "", nil)

	metadata := &job.BackupMetadata{
		PVCUID:       targetPVCUID,
		RBDImageName: "test-image",
		Raw: &job.BackupMetadataEntry{
			SnapID:    targetSnapshotID,
			SnapName:  "test-snap",
			SnapSize:  1000,
			PartSize:  512,
			CreatedAt: time.Now(),
		},
		Diff: []*job.BackupMetadataEntry{},
	}
	require.NoError(t, job.SetBackupMetadata(finRepo, metadata))

	// Create raw.img file to simulate existing backup
	require.NoError(t, rbdRepo.CreateEmptyRawImage(nlvRepo.GetRawImagePath(), 1000))

	// Act
	deletionJob := deletion.NewDeletion(&deletion.DeletionInput{
		Repo:                finRepo,
		RBDRepo:             rbdRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           actionUID,
		TargetSnapshotID:    targetSnapshotID,
		TargetPVCUID:        targetPVCUID,
	})
	err := deletionJob.Perform()
	require.NoError(t, err)

	// Verify backup metadata is updated (raw should be empty)
	_, err = job.GetBackupMetadata(finRepo)
	assert.ErrorIs(t, err, model.ErrNotFound)

	// Verify the raw image file was actually deleted
	_, err = os.Stat(nlvRepo.GetRawImagePath())
	require.True(t, errors.Is(err, fs.ErrNotExist), "raw image file should have been deleted")
}

func TestBackup_ErrorBusy(t *testing.T) {
	// Arrange
	actionUID := uuid.New().String()
	differentActionUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	err := finRepo.StartOrRestartAction(differentActionUID, model.Deletion)
	require.NoError(t, err)

	// Act
	deletionJob := deletion.NewDeletion(&deletion.DeletionInput{
		Repo:      finRepo,
		ActionUID: actionUID,
	})
	err = deletionJob.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

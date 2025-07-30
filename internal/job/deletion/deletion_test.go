package deletion

import (
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelete_RawOnlyCase_Success(t *testing.T) {
	// CSATEST-1565
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
	//   - The backup metadata is updated (raw becomes nil).
	//   - The raw image file is deleted.

	// Arrange
	actionUID := uuid.New().String()
	targetSnapshotID := 1
	targetPVCUID := uuid.New().String()

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

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
	file, err := os.Create(nlvRepo.GetRawImagePath())
	require.NoError(t, err)
	require.NoError(t, file.Close())

	// Act
	deletionJob := NewDeletion(&DeletionInput{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           actionUID,
		TargetSnapshotID:    targetSnapshotID,
		TargetPVCUID:        targetPVCUID,
	})
	err = deletionJob.Perform()
	require.NoError(t, err)

	// Assert
	_, err = job.GetBackupMetadata(finRepo)
	assert.ErrorIs(t, err, model.ErrNotFound)

	assert.NoFileExists(t, nlvRepo.GetRawImagePath(), "raw image file should have been deleted")
}

func TestDelete_NoExistsRawCase_Success(t *testing.T) {
	// CSATEST-1595
	//   Delete a backup when raw.img file does not exist but backup metadata has raw entry.
	//
	//   - A backup metadata with raw entry (non-empty) and no diff entries.
	//   - No raw.img file exists in the node local volume.
	//
	// Act:
	//   Run the deletion process to delete the full backup.
	//
	// Assert:
	//   - Backup metadata record is completely deleted.
	//   - Deletion job completes successfully.

	// Arrange
	actionUID := uuid.New().String()
	targetSnapshotID := 1
	targetPVCUID := uuid.New().String()

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

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

	// Act
	deletionJob := NewDeletion(&DeletionInput{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           actionUID,
		TargetSnapshotID:    targetSnapshotID,
		TargetPVCUID:        targetPVCUID,
	})
	err := deletionJob.Perform()
	require.NoError(t, err)

	// Verify backup metadata record doesn't exist.
	_, err = job.GetBackupMetadata(finRepo)
	assert.ErrorIs(t, err, model.ErrNotFound)

	err = finRepo.StartOrRestartAction(uuid.New().String(), model.Backup)
	assert.NoError(t, err)
}

func TestDelete_RawAndDiffCase_Success(t *testing.T) {
	// CSATEST-1591
	// Description:
	//   Delete the oldest backup when incremental backups exist, applying the first diff to raw.img
	//
	// Arrange:
	//   - A backup metadata with raw entry (snapID=1) and one diff entry (snapID=2).
	//   - Diff part files exist in the node local volume for the incremental backup.
	//
	// Act:
	//   Run the deletion process to delete the oldest backup (target snapID=1).
	//
	// Assert:
	//   - raw.img has diff/<snap id> applied to it.
	//   - diff/<snap id> directory is deleted.
	//   - backup_metadata target record becomes raw->diff0, diff->nil.
	//   - action_status table should be empty.

	// Arrange
	actionUID := uuid.New().String()
	targetRBDPoolName := "test-pool"
	targetRBDImageName := "test-image"
	partSize := 512

	previousSnapshotID := 1
	previousSnapshotName := "test-snap1"
	previousSnapshotSize := 1024

	targetSnapshotID := 2
	targetSnapshotName := "test-snap2"
	targetSnapshotSize := 2048
	targetPVCUID := uuid.New().String()

	targetSnapshotTimestamp := time.Now().Round(0)
	previousSnapshotTimestamp := targetSnapshotTimestamp.Add(-24 * time.Hour)

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	rbdRepo := fake.NewRBDRepository(map[fake.PoolImageName][]*model.RBDSnapshot{
		{PoolName: targetRBDPoolName, ImageName: targetRBDImageName}: {
			{
				ID:        previousSnapshotID,
				Name:      previousSnapshotName,
				Size:      previousSnapshotSize,
				Timestamp: model.NewRBDTimeStamp(previousSnapshotTimestamp),
			},
			{
				ID:        targetSnapshotID,
				Name:      targetSnapshotName,
				Size:      targetSnapshotSize,
				Timestamp: model.NewRBDTimeStamp(targetSnapshotTimestamp),
			},
		},
	})

	// Set the backup metadata
	metadata := &job.BackupMetadata{
		PVCUID:       targetPVCUID,
		RBDImageName: targetRBDImageName,
		Raw: &job.BackupMetadataEntry{
			SnapID:    previousSnapshotID,
			SnapName:  previousSnapshotName,
			SnapSize:  previousSnapshotSize,
			PartSize:  partSize,
			CreatedAt: previousSnapshotTimestamp,
		},
		Diff: []*job.BackupMetadataEntry{
			{
				SnapID:    targetSnapshotID,
				SnapName:  targetSnapshotName,
				SnapSize:  targetSnapshotSize,
				PartSize:  partSize,
				CreatedAt: targetSnapshotTimestamp,
			},
		},
	}
	require.NoError(t, job.SetBackupMetadata(finRepo, metadata))

	// Create raw.img file
	// Create diff directory and part files
	require.NoError(t, nlvRepo.MakeDiffDir(targetSnapshotID))

	// Create multiple diff parts (ceil(2048/512) = 4 parts)
	partCount := int(math.Ceil(float64(targetSnapshotSize) / float64(partSize)))
	for i := range partCount {
		err := rbdRepo.ExportDiff(&model.ExportDiffInput{
			PoolName:       targetRBDPoolName,
			ReadOffset:     i * partSize,
			ReadLength:     partSize,
			FromSnap:       &previousSnapshotName,
			MidSnapPrefix:  "test-prefix",
			ImageName:      targetRBDImageName,
			TargetSnapName: targetSnapshotName,
			OutputFile:     nlvRepo.GetDiffPartPath(targetSnapshotID, i),
		})
		require.NoError(t, err)
	}
	deletionJob := NewDeletion(&DeletionInput{
		Repo:                finRepo,
		RBDRepo:             rbdRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           actionUID,
		TargetSnapshotID:    previousSnapshotID,
		TargetPVCUID:        targetPVCUID,
	})

	// Act
	err := deletionJob.Perform()
	require.NoError(t, err)

	// Assert
	updatedMetadata, err := job.GetBackupMetadata(finRepo)
	require.NoError(t, err)
	assert.NotNil(t, updatedMetadata.Raw)
	assert.Equal(t, metadata.Diff[0].SnapID, updatedMetadata.Raw.SnapID)
	assert.Equal(t, metadata.Diff[0].SnapSize, updatedMetadata.Raw.SnapSize)
	assert.WithinDuration(t, metadata.Diff[0].CreatedAt, updatedMetadata.Raw.CreatedAt, 0)
	assert.Empty(t, updatedMetadata.Diff)

	for i := range partCount {
		assert.NoFileExistsf(t, nlvRepo.GetDiffPartPath(targetSnapshotID, i), "diff part-%d file should have been deleted", i)
	}

	// Assert
	// WARN: This assertion relies on the fake implementation.
	var rawImage fake.RawImage
	file, err := os.Open(nlvRepo.GetRawImagePath())
	require.NoError(t, err)
	defer func() { _ = file.Close() }()
	err = json.NewDecoder(file).Decode(&rawImage)
	require.NoError(t, err)
	assert.Equal(t, partCount, len(rawImage.AppliedDiffs), "raw image should have four applied diffs")

	// TODO
	// check whether the raw image file size equals the sum of the diff part sizes.

	err = finRepo.StartOrRestartAction(uuid.New().String(), model.Backup)
	assert.NoError(t, err)
}

func TestDelete_Retry_RawAndDiffCase_Success(t *testing.T) {
	// CSATEST-1596
	// Description:
	//   Test retry scenario for raw backup deletion when incremental backups exist.
	//
	// Arrange:
	//   - A backup metadata with raw entry (snapID=1) and one diff entry (snapID=2).
	//   - An existing raw.img that the first diff part has been applied.
	//   - Private data indicating retry from NextPatchPart=1.
	//
	// Act:
	//   Run the deletion process to delete the raw backup (target snapID=1) in retry mode.
	//
	// Assert:
	//   - raw.img has diff/<snap id> applied to it.
	//   - diff/<snap id> directory is deleted.
	//   - backup_metadata target record becomes raw->diff0, diff->nil.
	//   - action_status table should be empty.

	// Arrange
	actionUID := uuid.New().String()
	targetRBDPoolName := "test-pool"
	targetRBDImageName := "test-image"
	partSize := 512

	previousSnapshotID := 1
	previousSnapshotName := "test-snap1"
	previousSnapshotSize := 1024

	targetSnapshotID := 2
	targetSnapshotName := "test-snap2"
	targetSnapshotSize := 2048
	targetPVCUID := uuid.New().String()

	targetSnapshotTimestamp := time.Now().Round(0)
	previousSnapshotTimestamp := targetSnapshotTimestamp.Add(-24 * time.Hour)

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	rbdRepo := fake.NewRBDRepository(map[fake.PoolImageName][]*model.RBDSnapshot{
		{PoolName: targetRBDPoolName, ImageName: targetRBDImageName}: {
			{
				ID:        previousSnapshotID,
				Name:      previousSnapshotName,
				Size:      previousSnapshotSize,
				Timestamp: model.NewRBDTimeStamp(previousSnapshotTimestamp),
			},
			{
				ID:        targetSnapshotID,
				Name:      targetSnapshotName,
				Size:      targetSnapshotSize,
				Timestamp: model.NewRBDTimeStamp(targetSnapshotTimestamp),
			},
		},
	})

	// Set the backup metadata
	metadata := &job.BackupMetadata{
		PVCUID:       targetPVCUID,
		RBDImageName: targetRBDImageName,
		Raw: &job.BackupMetadataEntry{
			SnapID:    previousSnapshotID,
			SnapName:  previousSnapshotName,
			SnapSize:  previousSnapshotSize,
			PartSize:  partSize,
			CreatedAt: previousSnapshotTimestamp,
		},
		Diff: []*job.BackupMetadataEntry{
			{
				SnapID:    targetSnapshotID,
				SnapName:  targetSnapshotName,
				SnapSize:  targetSnapshotSize,
				PartSize:  partSize,
				CreatedAt: targetSnapshotTimestamp,
			},
		},
	}
	require.NoError(t, job.SetBackupMetadata(finRepo, metadata))

	// Create raw.img file
	file, err := os.Create(nlvRepo.GetRawImagePath())
	require.NoError(t, err)
	err = json.NewEncoder(file).Encode(&fake.RawImage{
		AppliedDiffs: []*fake.ExportedDiff{
			{
				PoolName:      targetRBDPoolName,
				ReadOffset:    0,
				ReadLength:    partSize,
				FromSnap:      &previousSnapshotName,
				MidSnapPrefix: "test-prefix",
				ImageName:     targetRBDImageName,
				SnapID:        previousSnapshotID,
				SnapName:      previousSnapshotName,
				SnapSize:      previousSnapshotSize,
				SnapTimestamp: previousSnapshotTimestamp,
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, file.Close())

	// Create diff directory and part files
	require.NoError(t, nlvRepo.MakeDiffDir(targetSnapshotID))

	// Create multiple diff parts (ceil(2048/512) = 4 parts)
	partCount := int(math.Ceil(float64(targetSnapshotSize) / float64(partSize)))
	for i := range partCount {
		err := rbdRepo.ExportDiff(&model.ExportDiffInput{
			PoolName:       targetRBDPoolName,
			ReadOffset:     i * partSize,
			ReadLength:     partSize,
			FromSnap:       &previousSnapshotName,
			MidSnapPrefix:  "test-prefix",
			ImageName:      targetRBDImageName,
			TargetSnapName: targetSnapshotName,
			OutputFile:     nlvRepo.GetDiffPartPath(targetSnapshotID, i),
		})
		require.NoError(t, err)
	}

	// Simulate the existing deletion action.
	err = finRepo.StartOrRestartAction(actionUID, model.Deletion)
	require.NoError(t, err)
	err = setDeletePrivateData(finRepo, actionUID, &deletePrivateData{NextPatchPart: 1})
	require.NoError(t, err)

	deletionJob := NewDeletion(&DeletionInput{
		Repo:                finRepo,
		RBDRepo:             rbdRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           actionUID,
		TargetSnapshotID:    previousSnapshotID,
		TargetPVCUID:        targetPVCUID,
	})

	// Act
	err = deletionJob.Perform()
	require.NoError(t, err)

	// Assert
	updatedMetadata, err := job.GetBackupMetadata(finRepo)
	require.NoError(t, err)
	assert.NotNil(t, updatedMetadata.Raw)
	assert.Equal(t, metadata.Diff[0].SnapID, updatedMetadata.Raw.SnapID)
	assert.Equal(t, metadata.Diff[0].SnapSize, updatedMetadata.Raw.SnapSize)
	assert.WithinDuration(t, metadata.Diff[0].CreatedAt, updatedMetadata.Raw.CreatedAt, 0)
	assert.Empty(t, updatedMetadata.Diff)

	for i := range partCount {
		assert.NoFileExistsf(t, nlvRepo.GetDiffPartPath(targetSnapshotID, i), "diff part-%d file should have been deleted", i)
	}

	// Assert
	// WARN: This assertion relies on the fake implementation.
	var rawImage fake.RawImage
	file, err = os.Open(nlvRepo.GetRawImagePath())
	require.NoError(t, err)
	defer func() { _ = file.Close() }()
	err = json.NewDecoder(file).Decode(&rawImage)
	require.NoError(t, err)
	assert.Equal(t, partCount, len(rawImage.AppliedDiffs), "raw image should have four applied diffs")

	// TODO: check whether the raw image file size equals the sum of the diff part sizes.

	err = finRepo.StartOrRestartAction(uuid.New().String(), model.Backup)
	assert.NoError(t, err)
}

func TestBackup_ErrorBusy(t *testing.T) {
	// CSATEST-1564
	// Description:
	//   Test deletion behavior when repository is busy with another action.
	//
	// Arrange:
	//   Repository is already locked by a different action UID.
	//
	// Act:
	//   Run the deletion process with a different action UID.
	//
	// Assert:
	//   Check if the deletion fails with ErrCantLock error.

	// Arrange
	actionUID := uuid.New().String()
	differentActionUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	err := finRepo.StartOrRestartAction(differentActionUID, model.Deletion)
	require.NoError(t, err)

	// Act
	deletionJob := NewDeletion(&DeletionInput{
		Repo:      finRepo,
		ActionUID: actionUID,
	})
	err = deletionJob.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

func TestDelete_BackupMetadataEmpty_NoAction(t *testing.T) {
	// CSATEST-1600
	// Description:
	//   Test deletion behavior when no backup metadata exists.
	//
	// Arrange:
	//   - backup metadata is empty.
	//   - A raw.img file exists.
	//
	// Act:
	//   Run the deletion process.
	//
	// Assert:
	//   - The deletion job succeeds without taking any action.
	//   - The existing raw image file remains untouched.
	//   - The action_status table should be empty.

	// Arrange
	actionUID := uuid.New().String()

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	// Create raw.img file to simulate existing backup
	rawImgPath := nlvRepo.GetRawImagePath()
	file, err := os.Create(rawImgPath)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	deletionJob := NewDeletion(&DeletionInput{
		Repo:      finRepo,
		ActionUID: actionUID,
	})

	// Act
	err = deletionJob.Perform()
	require.NoError(t, err)

	// Assert
	assert.FileExists(t, rawImgPath, "raw image file should still exist")

	err = finRepo.StartOrRestartAction(uuid.New().String(), model.Backup)
	assert.NoError(t, err)
}

func TestDelete_TargetSnapIDGreaterThanRawSnapID_Error(t *testing.T) {
	// CSATEST-1592
	// Description:
	//   Attempt to delete a backup when target snapshot ID is greater than raw snapshot ID.
	//
	// Arrange:
	//   A backup metadata with raw snapshot ID (1) and target snapshot ID (2).
	//
	// Act:
	//   Run the deletion process with target snapshot ID greater than raw snapshot ID.
	//
	// Assert:
	//   - The deletion fails.
	//   - action_status table is empty.

	// Arrange
	actionUID := uuid.New().String()
	rawSnapshotID := 1
	targetSnapshotID := 2

	targetPVCUID := uuid.New().String()

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	// Create raw.img file to simulate existing backup
	rawImgPath := nlvRepo.GetRawImagePath()
	file, err := os.Create(rawImgPath)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	metadata := &job.BackupMetadata{
		PVCUID:       targetPVCUID,
		RBDImageName: "test-image",
		Raw: &job.BackupMetadataEntry{
			SnapID:    rawSnapshotID,
			SnapName:  "test-snap",
			SnapSize:  1000,
			PartSize:  512,
			CreatedAt: time.Now(),
		},
		Diff: []*job.BackupMetadataEntry{},
	}
	require.NoError(t, job.SetBackupMetadata(finRepo, metadata))

	deletionJob := NewDeletion(&DeletionInput{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           actionUID,
		TargetSnapshotID:    targetSnapshotID,
		TargetPVCUID:        targetPVCUID,
	})

	// Act
	err = deletionJob.Perform()
	assert.Error(t, err)

	// Assert
	assert.ErrorContains(t, err, "raw.snapID is smaller than target snapshot ID")

	assert.FileExists(t, rawImgPath, "raw image file should still exist")

	err = finRepo.StartOrRestartAction(uuid.New().String(), model.Backup)
	assert.NoError(t, err)
}

func TestDelete_TargetSnapIDSmallerThanRawSnapID_NoAction(t *testing.T) {
	// CSATEST-1597
	// Description:
	//   Attempt to delete a backup when target snapshot ID is smaller than raw snapshot ID.
	//
	// Arrange:
	//   A backup metadata with raw snapshot ID (2) and target snapshot ID (1).
	//
	// Act:
	//   Run the deletion process with target snapshot ID smaller than raw snapshot ID.
	//
	// Assert:
	//   - The deletion succeeds (no error) and raw image file still exists.
	//   - action_status table should be empty.

	// Arrange
	actionUID := uuid.New().String()
	rawSnapshotID := 2
	targetSnapshotID := 1

	targetPVCUID := uuid.New().String()

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	// Create raw.img file to simulate existing backup
	rawImgPath := nlvRepo.GetRawImagePath()
	file, err := os.Create(rawImgPath)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	metadata := &job.BackupMetadata{
		PVCUID:       targetPVCUID,
		RBDImageName: "test-image",
		Raw: &job.BackupMetadataEntry{
			SnapID:    rawSnapshotID,
			SnapName:  "test-snap",
			SnapSize:  1000,
			PartSize:  512,
			CreatedAt: time.Now(),
		},
		Diff: []*job.BackupMetadataEntry{},
	}
	require.NoError(t, job.SetBackupMetadata(finRepo, metadata))

	deletionJob := NewDeletion(&DeletionInput{
		Repo:                finRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           actionUID,
		TargetSnapshotID:    targetSnapshotID,
		TargetPVCUID:        targetPVCUID,
	})

	// Act
	err = deletionJob.Perform()
	require.NoError(t, err)

	// Assert
	assert.FileExists(t, rawImgPath, "raw image file should still exist")

	err = finRepo.StartOrRestartAction(uuid.New().String(), model.Backup)
	assert.NoError(t, err)
}

func TestDelete_InvalidPVCUID_Error(t *testing.T) {
	// CSATEST-1594
	// Description:
	//   Attempt to delete a backup with mismatched PVC UID.
	//
	// Arrange:
	//   A backup metadata with a specific PVC UID.
	//
	// Act:
	//   Run the deletion process with an empty/different PVC UID.
	//
	// Assert:
	//   The deletion fails with an error about mismatched PVC UID.

	// Arrange
	actionUID := uuid.New().String()
	targetSnapshotID := 1
	targetPVCUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	metadata := &job.BackupMetadata{
		PVCUID:       targetPVCUID,
		RBDImageName: "test-image",
	}
	require.NoError(t, job.SetBackupMetadata(finRepo, metadata))

	deletionJob := NewDeletion(&DeletionInput{
		Repo:             finRepo,
		ActionUID:        actionUID,
		TargetSnapshotID: targetSnapshotID,
		TargetPVCUID:     "",
	})

	// Act
	err := deletionJob.Perform()

	// Assert
	assert.ErrorContains(t, err, "does not match the expected")
}

func TestDelete_InvalidState_RawEmpty_Error(t *testing.T) {
	// CSATEST-1593
	// Description:
	//   Attempt to delete a backup when raw entry is nil/empty in metadata.
	//
	// Arrange:
	//   A backup metadata with nil raw entry (invalid state).
	//
	// Act:
	//   Run the deletion process on this invalid state.
	//
	// Assert:
	//   Check if the deletion fails with an error.

	// Arrange
	actionUID := uuid.New().String()
	targetSnapshotID := 1
	targetPVCUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	metadata := &job.BackupMetadata{
		PVCUID: targetPVCUID,
		Raw:    nil,
	}
	require.NoError(t, job.SetBackupMetadata(finRepo, metadata))

	deletionJob := NewDeletion(&DeletionInput{
		Repo:             finRepo,
		ActionUID:        actionUID,
		TargetSnapshotID: targetSnapshotID,
		TargetPVCUID:     targetPVCUID,
	})

	// Act
	err := deletionJob.Perform()
	require.Error(t, err)

	// Assert
	assert.ErrorContains(t, err, "invalid state: raw is empty")
}

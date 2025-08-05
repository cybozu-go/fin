package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/cybozu-go/fin/test/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/equality"
)

type setupInput struct {
	fullSnapshotSize, incrementalSnapshotSize int
	maxPartSize                               int
}

type setupOutput struct {
	fullBackupInput, incrementalBackupInput *input.Backup
	k8sRepo                                 *fake.KubernetesRepository
	finRepo                                 model.FinRepository
	nlvRepo                                 model.NodeLocalVolumeRepository
	rbdRepo                                 *fake.RBDRepository
	fullSnapshot, incrementalSnapshot       *model.RBDSnapshot
	targetPVName                            string
}

func setup(t *testing.T, input *setupInput) *setupOutput {
	maxPartSize := 512
	if input.maxPartSize != 0 {
		maxPartSize = input.maxPartSize
	}

	fullSnapshotSize := 900
	if input.fullSnapshotSize != 0 {
		fullSnapshotSize = input.fullSnapshotSize
	}

	k8sRepo, rbdRepo, volumeInfo := fake.NewStorage()
	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	// Take a fake snapshot for full backup
	fullSnapshot := rbdRepo.CreateFakeSnapshot(utils.GetUniqueName("snap-"), fullSnapshotSize, time.Now())

	// Create a full backup input
	fullBackupInput := testutil.NewBackupInput(k8sRepo, volumeInfo, fullSnapshot.ID, nil, maxPartSize)
	fullBackupInput.Repo = finRepo
	fullBackupInput.KubernetesRepo = k8sRepo
	fullBackupInput.RBDRepo = rbdRepo
	fullBackupInput.NodeLocalVolumeRepo = nlvRepo

	// Take a fake snapshot for incremental backup
	incrementalSnapshotSize := 1000
	incrementalSnapshot := rbdRepo.CreateFakeSnapshot(utils.GetUniqueName("snap-"), incrementalSnapshotSize, time.Now())

	// Create an incremental backup input
	incrementalBackupInput := testutil.NewBackupInput(
		k8sRepo, volumeInfo, incrementalSnapshot.ID, &fullSnapshot.ID, maxPartSize)
	incrementalBackupInput.Repo = finRepo
	incrementalBackupInput.KubernetesRepo = k8sRepo
	incrementalBackupInput.RBDRepo = rbdRepo
	incrementalBackupInput.NodeLocalVolumeRepo = nlvRepo

	return &setupOutput{
		fullBackupInput:        fullBackupInput,
		incrementalBackupInput: incrementalBackupInput,
		k8sRepo:                k8sRepo,
		finRepo:                finRepo,
		nlvRepo:                nlvRepo,
		rbdRepo:                rbdRepo,
		fullSnapshot:           fullSnapshot,
		incrementalSnapshot:    incrementalSnapshot,
		targetPVName:           volumeInfo.PVName,
	}
}

func TestFullBackup_Success(t *testing.T) {
	// Description:
	//   Create a full backup with no error.
	//
	// Arrange:
	//   None
	//
	// Act:
	//   Run the backup process to create a full backup.
	//
	// Assert:
	//   Check if the contents of the node local volume is correct.

	// Arrange
	cfg := setup(t, &setupInput{})

	// Act
	backup := NewBackup(cfg.fullBackupInput)
	err := backup.Perform()
	require.NoError(t, err)

	// Assert
	testutil.AssertActionPrivateDataIsEmpty(t, cfg.finRepo, cfg.fullBackupInput.ActionUID)

	rawImage, err := fake.ReadRawImage(cfg.nlvRepo.GetRawImagePath())
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rawImage.AppliedDiffs))
	assert.Equal(t, 0, rawImage.AppliedDiffs[0].ReadOffset)
	assert.Equal(t, cfg.fullBackupInput.MaxPartSize, rawImage.AppliedDiffs[0].ReadLength)
	assert.Equal(t, cfg.fullBackupInput.MaxPartSize, rawImage.AppliedDiffs[1].ReadOffset)
	assert.Equal(t, cfg.fullBackupInput.MaxPartSize, rawImage.AppliedDiffs[1].ReadLength)

	fakePVC, err := cfg.k8sRepo.GetPVC(cfg.fullBackupInput.TargetPVCName, cfg.fullBackupInput.TargetPVCNamespace)
	require.NoError(t, err)
	resPVC, err := cfg.nlvRepo.GetPVC()
	assert.NoError(t, err)
	assert.True(t, equality.Semantic.DeepEqual(fakePVC, resPVC))

	fakePV, err := cfg.k8sRepo.GetPV(cfg.targetPVName)
	require.NoError(t, err)
	resPV, err := cfg.nlvRepo.GetPV()
	assert.NoError(t, err)
	assert.True(t, equality.Semantic.DeepEqual(fakePV, resPV))

	for _, diff := range rawImage.AppliedDiffs {
		assert.Equal(t, cfg.fullBackupInput.TargetRBDPoolName, diff.PoolName)
		assert.Nil(t, diff.FromSnap)
		assert.Equal(t, cfg.fullSnapshot.Name, diff.MidSnapPrefix)
		assert.Equal(t, cfg.fullBackupInput.TargetRBDImageName, diff.ImageName)
		assert.Equal(t, cfg.fullBackupInput.TargetSnapshotID, diff.SnapID)
		assert.Equal(t, cfg.fullSnapshot.Name, diff.SnapName)
		assert.Equal(t, cfg.fullSnapshot.Size, diff.SnapSize)
		assert.True(t, cfg.fullSnapshot.Timestamp.Equal(diff.SnapTimestamp))
	}

	ensureBackupMetadataCorrect(t, cfg.finRepo, &job.BackupMetadata{
		PVCUID:       cfg.fullBackupInput.TargetPVCUID,
		RBDImageName: cfg.fullBackupInput.TargetRBDImageName,
		Raw: &job.BackupMetadataEntry{
			SnapID:    cfg.fullBackupInput.TargetSnapshotID,
			SnapName:  cfg.fullSnapshot.Name,
			SnapSize:  cfg.fullSnapshot.Size,
			PartSize:  cfg.fullBackupInput.MaxPartSize,
			CreatedAt: cfg.fullSnapshot.Timestamp.Time,
		},
	})
}

func TestIncrementalBackup_Success(t *testing.T) {
	// CSATEST-1495, CSATEST-1501
	// Description:
	//   Create an incremental backup with no error.
	//
	// Arrange:
	//   A full backup, `fullBackup`.
	//
	// Act:
	//   Run the backup process to create an incremental backup,
	//   `incrementalBackup`. It's size is larger than full backup.
	//
	// Assert:
	//   Check if the contents of the incremental backup is correct.

	// Arrange
	cfg := setup(t, &setupInput{})
	finRepo := cfg.finRepo
	nlvRepo := cfg.nlvRepo

	// Create a full backup
	fullBackup := NewBackup(cfg.fullBackupInput)
	err := fullBackup.Perform()
	require.NoError(t, err)

	// Change PVC and PV annotations to non-nil values.
	// This step is necessary to ensure that pv.yaml and pvc.yaml won't be changed after the incremental backup.
	pvc, err := cfg.k8sRepo.GetPVC(cfg.incrementalBackupInput.TargetPVCName, cfg.incrementalBackupInput.TargetPVCNamespace)
	require.NoError(t, err)
	require.Nil(t, pvc.Annotations)
	pvc.Annotations = map[string]string{"test": "pvc"}
	cfg.k8sRepo.SetPVC(cfg.incrementalBackupInput.TargetPVCName, cfg.incrementalBackupInput.TargetPVCNamespace, pvc)

	pv, err := cfg.k8sRepo.GetPV(cfg.targetPVName)
	require.NoError(t, err)
	require.Nil(t, pv.Annotations)
	pv.Annotations = map[string]string{"test": "pv"}
	cfg.k8sRepo.SetPV(pv.Name, pv)

	expectedPVC, err := nlvRepo.GetPVC()
	require.NoError(t, err)
	expectedPV, err := nlvRepo.GetPV()
	require.NoError(t, err)

	// Act
	backup := NewBackup(cfg.incrementalBackupInput)
	err = backup.Perform()
	require.NoError(t, err)

	// Assert
	testutil.AssertActionPrivateDataIsEmpty(t, finRepo, cfg.fullBackupInput.ActionUID)
	numDiffParts := int(math.Ceil(float64(cfg.fullSnapshot.Size) / float64(cfg.fullBackupInput.MaxPartSize)))
	for i := range numDiffParts {
		diffFilePath := nlvRepo.GetDiffPartPath(cfg.incrementalBackupInput.TargetSnapshotID, i)
		ensureDiffFileCorrect(t, diffFilePath, &fake.ExportedDiff{
			PoolName:      cfg.fullBackupInput.TargetRBDPoolName,
			FromSnap:      &cfg.fullSnapshot.Name,
			MidSnapPrefix: cfg.incrementalSnapshot.Name,
			ImageName:     cfg.incrementalBackupInput.TargetRBDImageName,
			SnapID:        cfg.incrementalBackupInput.TargetSnapshotID,
			SnapName:      cfg.incrementalSnapshot.Name,
			SnapSize:      cfg.incrementalSnapshot.Size,
			SnapTimestamp: cfg.incrementalSnapshot.Timestamp.Time,
		})
	}

	ensureBackupMetadataCorrect(t, finRepo, &job.BackupMetadata{
		PVCUID:       cfg.fullBackupInput.TargetPVCUID,
		RBDImageName: cfg.fullBackupInput.TargetRBDImageName,
		Raw: &job.BackupMetadataEntry{
			SnapID:    cfg.fullBackupInput.TargetSnapshotID,
			SnapName:  cfg.fullSnapshot.Name,
			SnapSize:  cfg.fullSnapshot.Size,
			PartSize:  cfg.fullBackupInput.MaxPartSize,
			CreatedAt: cfg.fullSnapshot.Timestamp.Time,
		},
		Diff: []*job.BackupMetadataEntry{
			{
				SnapID:    cfg.incrementalBackupInput.TargetSnapshotID,
				SnapName:  cfg.incrementalSnapshot.Name,
				SnapSize:  cfg.incrementalSnapshot.Size,
				PartSize:  cfg.incrementalBackupInput.MaxPartSize,
				CreatedAt: cfg.incrementalSnapshot.Timestamp.Time,
			},
		},
	})

	// Ensure that the PVC and PV are not changed after the incremental backup.
	gotPVC, err := nlvRepo.GetPVC()
	assert.NoError(t, err)
	assert.Equal(t, expectedPVC, gotPVC)
	gotPV, err := nlvRepo.GetPV()
	assert.NoError(t, err)
	assert.Equal(t, expectedPV, gotPV)
}

func TestBackup_ErrorBusy(t *testing.T) {
	// Arrange
	actionUID := uuid.New().String()
	differentActionUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	err := finRepo.StartOrRestartAction(differentActionUID, model.Backup)
	require.NoError(t, err)

	// Act
	backup := NewBackup(&input.Backup{
		Repo:      finRepo,
		ActionUID: actionUID,
	})
	err = backup.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

func Test_FullBackup_Success_Resume(t *testing.T) {
	// CSATEST-1508, CSATEST-1510
	// Description:
	//   Resume full backup that was interrupted after setting nextStorePart or nextPatchPart to 1.
	//
	// Arrange:
	//   - Set backup_metadata.raw to be empty.
	//   - Set max part num to 3.
	//   - Set action_status.nextStorePart or action_status.nextPatchPart to 1.
	//
	// Act:
	//   Run the backup process to create an full backup.
	//
	// Assert:
	//   Check if the contents of the full backup is correct.

	testcases := []struct{ nextStorePart, nextPatchPart int }{
		{nextStorePart: 1 /* in progress */, nextPatchPart: 0 /* not started yet */},
		{nextStorePart: 3 /* completed */, nextPatchPart: 1 /* in progress */},
	}
	for _, tc := range testcases {
		t.Run(
			fmt.Sprintf("nextStorePart=%d nextPatchPart=%d", tc.nextStorePart, tc.nextPatchPart),
			func(t *testing.T) {
				// Arrange
				snapshotSize, maxPartSize := 1000, 400 // max part size will be 3 (= ceil(1000/400))
				cfg := setup(t, &setupInput{
					fullSnapshotSize:        snapshotSize,
					incrementalSnapshotSize: snapshotSize,
					maxPartSize:             maxPartSize,
				})

				// Set up multipart diff files
				err := cfg.nlvRepo.MakeDiffDir(cfg.fullSnapshot.ID)
				require.NoError(t, err)
				for i := 0; i < tc.nextStorePart; i++ {
					err := cfg.rbdRepo.ExportDiff(&model.ExportDiffInput{
						PoolName:       cfg.fullBackupInput.TargetRBDPoolName,
						ReadOffset:     cfg.fullBackupInput.MaxPartSize * i,
						ReadLength:     cfg.fullBackupInput.MaxPartSize,
						FromSnap:       nil,
						MidSnapPrefix:  cfg.fullSnapshot.Name,
						ImageName:      cfg.fullBackupInput.TargetRBDImageName,
						TargetSnapName: cfg.fullSnapshot.Name,
						OutputFile:     cfg.nlvRepo.GetDiffPartPath(cfg.fullBackupInput.TargetSnapshotID, i),
					})
					require.NoError(t, err)
				}

				// Set up fin.sqlite3 for the test
				err = cfg.finRepo.StartOrRestartAction(cfg.fullBackupInput.ActionUID, model.Backup)
				require.NoError(t, err)

				arrangedBackupMetadata, err := json.Marshal(job.BackupMetadata{
					PVCUID:       cfg.fullBackupInput.TargetPVCUID,
					RBDImageName: cfg.fullBackupInput.TargetRBDImageName,
					Raw:          nil, // Raw should be empty
					Diff:         nil,
				})
				require.NoError(t, err)
				err = cfg.finRepo.SetBackupMetadata(arrangedBackupMetadata)
				require.NoError(t, err)

				arrangedActionPrivateData, err := json.Marshal(backupPrivateData{
					NextStorePart: tc.nextStorePart, // Set custom value to nextStorePart
					NextPatchPart: tc.nextPatchPart, // Set custom value to nextPatchPart
					Mode:          modeFull,
				})
				require.NoError(t, err)
				err = cfg.finRepo.UpdateActionPrivateData(cfg.fullBackupInput.ActionUID, arrangedActionPrivateData)
				require.NoError(t, err)

				// Act
				backup := NewBackup(cfg.fullBackupInput)
				err = backup.Perform()
				require.NoError(t, err)

				// Assert
				testutil.AssertActionPrivateDataIsEmpty(t, cfg.finRepo, cfg.fullBackupInput.ActionUID)
				rawImage, err := fake.ReadRawImage(cfg.nlvRepo.GetRawImagePath())
				assert.NoError(t, err)

				assert.Equal(t, 3-tc.nextPatchPart, len(rawImage.AppliedDiffs))
				off := tc.nextPatchPart * maxPartSize
				for _, diff := range rawImage.AppliedDiffs {
					assert.Equal(t, cfg.fullBackupInput.TargetRBDPoolName, diff.PoolName)
					assert.Nil(t, diff.FromSnap)
					assert.Equal(t, cfg.fullSnapshot.Name, diff.MidSnapPrefix)
					assert.Equal(t, cfg.fullBackupInput.TargetRBDImageName, diff.ImageName)
					assert.Equal(t, cfg.fullBackupInput.TargetSnapshotID, diff.SnapID)
					assert.Equal(t, cfg.fullSnapshot.Name, diff.SnapName)
					assert.Equal(t, cfg.fullSnapshot.Size, diff.SnapSize)
					assert.True(t, cfg.fullSnapshot.Timestamp.Equal(diff.SnapTimestamp))

					assert.Equal(t, off, diff.ReadOffset)
					assert.Equal(t, maxPartSize, diff.ReadLength)
					off += maxPartSize
				}

				ensureBackupMetadataCorrect(t, cfg.finRepo, &job.BackupMetadata{
					PVCUID:       cfg.fullBackupInput.TargetPVCUID,
					RBDImageName: cfg.fullBackupInput.TargetRBDImageName,
					Raw: &job.BackupMetadataEntry{
						SnapID:    cfg.fullBackupInput.TargetSnapshotID,
						SnapName:  cfg.fullSnapshot.Name,
						SnapSize:  cfg.fullSnapshot.Size,
						PartSize:  cfg.fullBackupInput.MaxPartSize,
						CreatedAt: cfg.fullSnapshot.Timestamp.Time,
					},
				})
			},
		)
	}
}

func Test_IncrementalBackup_Success_Resume(t *testing.T) {
	// CSATEST-1512
	// Description:
	//   Resume an incremental backup that was interrupted after setting nextStorePart=0 or 1.
	//
	// Arrange:
	//   - Set backup_metadata.raw to be non-empty.
	//   - Set max part num to 3.
	//   - Set action_status.nextStorePart to 1.
	//
	// Act:
	//   Run the backup process to create an incremental backup,
	//   `incrementalBackup`.
	//
	// Assert:
	//   Check if the contents of the incremental backup is correct.

	// Arrange
	nextStorePart := 1
	snapshotSize, maxPartSize := 1000, 400 // max part size will be 3 (= ceil(1000/400))

	cfg := setup(t, &setupInput{
		fullSnapshotSize:        snapshotSize,
		incrementalSnapshotSize: snapshotSize,
		maxPartSize:             maxPartSize,
	})

	// Set up fin.sqlite3 for the test
	err := cfg.finRepo.StartOrRestartAction(cfg.incrementalBackupInput.ActionUID, model.Backup)
	require.NoError(t, err)

	arrangedBackupMetadata, err := json.Marshal(job.BackupMetadata{
		PVCUID:       cfg.fullBackupInput.TargetPVCUID,
		RBDImageName: cfg.fullBackupInput.TargetRBDImageName,
		Raw: &job.BackupMetadataEntry{ // Raw should be non-empty
			SnapID:    cfg.fullBackupInput.TargetSnapshotID,
			SnapName:  cfg.fullSnapshot.Name,
			SnapSize:  snapshotSize,
			PartSize:  cfg.fullBackupInput.MaxPartSize,
			CreatedAt: cfg.fullSnapshot.Timestamp.Time,
		},
		Diff: []*job.BackupMetadataEntry{},
	})
	require.NoError(t, err)
	err = cfg.finRepo.SetBackupMetadata(arrangedBackupMetadata)
	require.NoError(t, err)

	arrangedActionPrivateData, err := json.Marshal(backupPrivateData{
		NextStorePart: nextStorePart, // Set custom value to nextStorePart
		Mode:          modeIncremental,
	})
	require.NoError(t, err)
	err = cfg.finRepo.UpdateActionPrivateData(cfg.incrementalBackupInput.ActionUID, arrangedActionPrivateData)
	require.NoError(t, err)

	// Act
	backup := NewBackup(cfg.incrementalBackupInput)
	err = backup.Perform()
	require.NoError(t, err)

	// Assert
	testutil.AssertActionPrivateDataIsEmpty(t, cfg.finRepo, cfg.incrementalBackupInput.ActionUID)
	numDiffParts := int(math.Ceil(float64(snapshotSize) / float64(maxPartSize)))
	for i := range numDiffParts {
		if i < nextStorePart { // Skipped diff parts should not exist
			diffFilePath := cfg.nlvRepo.GetDiffPartPath(cfg.incrementalBackupInput.TargetSnapshotID, i)
			_, err := os.Stat(diffFilePath)
			assert.True(t, errors.Is(err, os.ErrNotExist))
			continue
		}

		diffFilePath := cfg.nlvRepo.GetDiffPartPath(cfg.incrementalBackupInput.TargetSnapshotID, i)
		ensureDiffFileCorrect(t, diffFilePath, &fake.ExportedDiff{
			PoolName:      cfg.fullBackupInput.TargetRBDPoolName,
			FromSnap:      &cfg.fullSnapshot.Name,
			MidSnapPrefix: cfg.incrementalSnapshot.Name,
			ImageName:     cfg.incrementalBackupInput.TargetRBDImageName,
			SnapID:        cfg.incrementalBackupInput.TargetSnapshotID,
			SnapName:      cfg.incrementalSnapshot.Name,
			SnapSize:      snapshotSize,
			SnapTimestamp: cfg.incrementalSnapshot.Timestamp.Time,
		})
	}

	ensureBackupMetadataCorrect(t, cfg.finRepo, &job.BackupMetadata{
		PVCUID:       cfg.fullBackupInput.TargetPVCUID,
		RBDImageName: cfg.fullBackupInput.TargetRBDImageName,
		Raw: &job.BackupMetadataEntry{
			SnapID:    cfg.fullBackupInput.TargetSnapshotID,
			SnapName:  cfg.fullSnapshot.Name,
			SnapSize:  snapshotSize,
			PartSize:  maxPartSize,
			CreatedAt: cfg.fullSnapshot.Timestamp.Time,
		},
		Diff: []*job.BackupMetadataEntry{
			{
				SnapID:    cfg.incrementalBackupInput.TargetSnapshotID,
				SnapName:  cfg.incrementalSnapshot.Name,
				SnapSize:  snapshotSize,
				PartSize:  maxPartSize,
				CreatedAt: cfg.incrementalSnapshot.Timestamp.Time,
			},
		},
	})
}

func Test_FullBackup_Success_DifferentNode(t *testing.T) {
	// CSATEST-1496
	// Description:
	//   Create a full backup with no error on a node different from its previous backup.
	//
	// Arrange:
	//   - Set SourceCandidateSnapshotID to an non-empty value.
	//   - Set backup_metadata table to be empty.
	//
	// Act:
	//   Run the backup process to create an full backup.
	//
	// Assert:
	//   Check if the contents of the full backup is correct.

	// Arrange
	cfg := setup(t, &setupInput{})
	require.NotNil(t, cfg.incrementalBackupInput.SourceCandidateSnapshotID)

	// Act
	backup := NewBackup(cfg.incrementalBackupInput /* Use incrementalBackupInput to set SourceCandidateSnapshotID */)
	err := backup.Perform()
	require.NoError(t, err)

	// Assert
	testutil.AssertActionPrivateDataIsEmpty(t, cfg.finRepo, cfg.incrementalBackupInput.ActionUID)
	rawImage, err := fake.ReadRawImage(cfg.nlvRepo.GetRawImagePath())
	assert.NoError(t, err)

	assert.Len(t, rawImage.AppliedDiffs, 2)
	for i, diff := range rawImage.AppliedDiffs {
		assert.Equal(t, cfg.incrementalBackupInput.TargetRBDPoolName, diff.PoolName)
		assert.Nil(t, diff.FromSnap)
		assert.Equal(t, cfg.incrementalSnapshot.Name, diff.MidSnapPrefix)
		assert.Equal(t, cfg.incrementalBackupInput.TargetRBDImageName, diff.ImageName)
		assert.Equal(t, cfg.incrementalBackupInput.TargetSnapshotID, diff.SnapID)
		assert.Equal(t, cfg.incrementalSnapshot.Name, diff.SnapName)
		assert.Equal(t, cfg.incrementalSnapshot.Size, diff.SnapSize)
		assert.True(t, cfg.incrementalSnapshot.Timestamp.Equal(diff.SnapTimestamp))

		assert.Equal(t, i*cfg.incrementalBackupInput.MaxPartSize, diff.ReadOffset)
		assert.Equal(t, cfg.incrementalBackupInput.MaxPartSize, diff.ReadLength)
	}

	ensureBackupMetadataCorrect(t, cfg.finRepo, &job.BackupMetadata{
		PVCUID:       cfg.incrementalBackupInput.TargetPVCUID,
		RBDImageName: cfg.incrementalBackupInput.TargetRBDImageName,
		Raw: &job.BackupMetadataEntry{
			SnapID:    cfg.incrementalBackupInput.TargetSnapshotID,
			SnapName:  cfg.incrementalSnapshot.Name,
			SnapSize:  cfg.incrementalSnapshot.Size,
			PartSize:  cfg.incrementalBackupInput.MaxPartSize,
			CreatedAt: cfg.incrementalSnapshot.Timestamp.Time,
		},
		// Diff should be empty because this is a full backup
	})
}

func Test_FullBackup_Error_NonExistentTargetPVAndPVC(t *testing.T) {
	// CSATEST-1498
	// Description:
	//   Creation of a full backup fails when the target PV and PVC do not exist.
	//
	// Arrange:
	//   - Set up a full backup input.
	//   - Delete the target PV and PVC.
	//
	// Act:
	//   Run the backup process to create an full backup.
	//
	// Assert:
	//   Check if the full backup creation fails with an error.

	// Arrange
	cfg := setup(t, &setupInput{})
	cfg.k8sRepo.DeletePV(cfg.targetPVName)
	cfg.k8sRepo.DeletePVC(cfg.fullBackupInput.TargetPVCName, cfg.fullBackupInput.TargetPVCNamespace)

	// Act
	backup := NewBackup(cfg.fullBackupInput)
	err := backup.Perform()

	// Assert
	assert.Error(t, err)
}

func Test_FullBackup_Error_DifferentRBDImageName(t *testing.T) {
	// CSATEST-1500
	// Description:
	//   Creation of a full backup fails when the target PV has a different RBD image name.
	//
	// Arrange:
	//   - Set up a full backup input.
	//   - Set a different image name from the environment variable to the target PV.
	//
	// Act:
	//   Run the backup process to create an full backup.
	//
	// Assert:
	//   Check if the full backup creation fails with an error.

	// Arrange
	cfg := setup(t, &setupInput{})

	pv, err := cfg.k8sRepo.GetPV(cfg.targetPVName)
	require.NoError(t, err)
	pv.Spec.CSI.VolumeAttributes["imageName"] = fmt.Sprintf("recreated-%s", pv.Spec.CSI.VolumeAttributes["imageName"])
	cfg.k8sRepo.SetPV(cfg.targetPVName, pv)

	// Act
	backup := NewBackup(cfg.fullBackupInput)
	err = backup.Perform()

	// Assert
	assert.Error(t, err)
}

func ensureDiffFileCorrect(t *testing.T, diffFilePath string, expected *fake.ExportedDiff) {
	t.Helper()
	diff, err := fake.ReadDiff(diffFilePath)
	require.NoError(t, err)
	assert.Equal(t, expected.PoolName, diff.PoolName)
	assert.Equal(t, expected.FromSnap, diff.FromSnap)
	assert.Equal(t, expected.MidSnapPrefix, diff.MidSnapPrefix)
	assert.Equal(t, expected.ImageName, diff.ImageName)
	assert.Equal(t, expected.SnapID, diff.SnapID)
	assert.Equal(t, expected.SnapName, diff.SnapName)
	assert.Equal(t, expected.SnapSize, diff.SnapSize)
	assert.True(t, expected.SnapTimestamp.Equal(diff.SnapTimestamp))
}

func ensureBackupMetadataCorrect(t *testing.T, repo model.FinRepository, expected *job.BackupMetadata) {
	t.Helper()

	metadata, err := job.GetBackupMetadata(repo)
	require.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, expected.PVCUID, metadata.PVCUID)
	assert.Equal(t, expected.RBDImageName, metadata.RBDImageName)

	if expected.Raw == nil {
		assert.Nil(t, metadata.Raw)
	} else {
		assert.NotNil(t, metadata.Raw)
		assert.Equal(t, expected.Raw.SnapID, metadata.Raw.SnapID)
		assert.Equal(t, expected.Raw.SnapName, metadata.Raw.SnapName)
		assert.Equal(t, expected.Raw.SnapSize, metadata.Raw.SnapSize)
		assert.Equal(t, expected.Raw.PartSize, metadata.Raw.PartSize)
		assert.True(t, expected.Raw.CreatedAt.Equal(metadata.Raw.CreatedAt))
	}

	if expected.Diff == nil {
		assert.Nil(t, metadata.Diff)
	} else {
		assert.NotNil(t, metadata.Diff)
		for i := range expected.Diff {
			assert.Equal(t, expected.Diff[i].SnapID, metadata.Diff[i].SnapID)
			assert.Equal(t, expected.Diff[i].SnapName, metadata.Diff[i].SnapName)
			assert.Equal(t, expected.Diff[i].SnapSize, metadata.Diff[i].SnapSize)
			assert.Equal(t, expected.Diff[i].PartSize, metadata.Diff[i].PartSize)
			assert.True(t, expected.Diff[i].CreatedAt.Equal(metadata.Diff[i].CreatedAt))
		}
	}
}

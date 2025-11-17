package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

type setupInput struct {
	fullSnapshotSize, incrementalSnapshotSize uint64
	maxPartSize                               uint64
}

type setupOutput struct {
	fullBackupInput, incrementalBackupInput, incrementalBackupInput2 *input.Backup
	k8sClient                                                        kubernetes.Interface
	finRepo                                                          model.FinRepository
	nlvRepo                                                          model.NodeLocalVolumeRepository
	rbdRepo                                                          *fake.RBDRepository2
	fullSnapshot, incrementalSnapshot                                *model.RBDSnapshot
	fullData, incrementalData, incrementalData2                      []byte
	targetPVName                                                     string
}

func setup(t *testing.T, input *setupInput) *setupOutput {

	maxPartSize := uint64(1024)
	if input.maxPartSize != 0 {
		maxPartSize = input.maxPartSize
	}

	fullSnapshotSize := uint64(4096)
	if input.fullSnapshotSize != 0 {
		fullSnapshotSize = input.fullSnapshotSize
	}

	k8sClient, volumeInfo := fake.NewStorage()
	rbdRepo := fake.NewRBDRepository2(volumeInfo.PoolName, volumeInfo.ImageName)
	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	// Take a fake snapshot for full backup
	fullSnapshot, fullData, err := rbdRepo.CreateSnapshotWithRandomData(utils.GetUniqueName("snap-"), fullSnapshotSize)
	require.NoError(t, err)

	// Create a full backup input
	fullBackupInput := testutil.NewBackupInput(k8sClient, volumeInfo, fullSnapshot.ID, nil, maxPartSize)
	fullBackupInput.Repo = finRepo
	fullBackupInput.K8sClient = k8sClient
	fullBackupInput.RBDRepo = rbdRepo
	fullBackupInput.NodeLocalVolumeRepo = nlvRepo

	// Take a fake snapshot for incremental backup
	incrSnapSize := fullSnapshotSize * 2
	incrlSnap, incrData, err := rbdRepo.CreateSnapshotWithRandomData(utils.GetUniqueName("snap-"), incrSnapSize)
	require.NoError(t, err)

	// Create an incremental backup input
	incrementalBackupInput := testutil.NewBackupInput(
		k8sClient, volumeInfo, incrlSnap.ID, &fullSnapshot.ID, maxPartSize)
	incrementalBackupInput.Repo = finRepo
	incrementalBackupInput.K8sClient = k8sClient
	incrementalBackupInput.RBDRepo = rbdRepo
	incrementalBackupInput.NodeLocalVolumeRepo = nlvRepo

	// Take a second fake snapshot for second incremental backup
	incrSnapSize2 := fullSnapshotSize * 3
	incrSnap2, incrData2, err := rbdRepo.CreateSnapshotWithRandomData(utils.GetUniqueName("snap-"), incrSnapSize2)
	require.NoError(t, err)

	// Create a second incremental backup input
	incrementalBackupInput2 := testutil.NewBackupInput(
		k8sClient, volumeInfo, incrSnap2.ID, &incrlSnap.ID, maxPartSize)
	incrementalBackupInput2.Repo = finRepo
	incrementalBackupInput2.K8sClient = k8sClient
	incrementalBackupInput2.RBDRepo = rbdRepo
	incrementalBackupInput2.NodeLocalVolumeRepo = nlvRepo

	return &setupOutput{
		fullBackupInput:         fullBackupInput,
		incrementalBackupInput:  incrementalBackupInput,
		incrementalBackupInput2: incrementalBackupInput2,
		k8sClient:               k8sClient,
		finRepo:                 finRepo,
		nlvRepo:                 nlvRepo,
		rbdRepo:                 rbdRepo,
		fullSnapshot:            fullSnapshot,
		incrementalSnapshot:     incrlSnap,
		fullData:                fullData,
		incrementalData:         incrData,
		incrementalData2:        incrData2,
		targetPVName:            volumeInfo.PVName,
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
	fullData, err := os.ReadFile(cfg.nlvRepo.GetRawImagePath())
	require.Equal(t, len(cfg.fullData), len(fullData))
	require.NoError(t, err)
	require.Equal(t, cfg.fullData, fullData)

	ctx := context.Background()
	fakePVC, err := cfg.k8sClient.CoreV1().
		PersistentVolumeClaims(cfg.fullBackupInput.TargetPVCNamespace).
		Get(ctx, cfg.fullBackupInput.TargetPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	resPVC, err := cfg.nlvRepo.GetPVC()
	require.NoError(t, err)
	assert.True(t, equality.Semantic.DeepEqual(fakePVC, resPVC))

	fakePV, err := cfg.k8sClient.CoreV1().PersistentVolumes().Get(ctx, cfg.targetPVName, metav1.GetOptions{})
	require.NoError(t, err)
	resPV, err := cfg.nlvRepo.GetPV()
	require.NoError(t, err)
	assert.True(t, equality.Semantic.DeepEqual(fakePV, resPV))

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
	ctx := context.Background()
	pvc, err := cfg.k8sClient.CoreV1().
		PersistentVolumeClaims(cfg.incrementalBackupInput.TargetPVCNamespace).
		Get(ctx, cfg.incrementalBackupInput.TargetPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Nil(t, pvc.Annotations)
	pvc.Annotations = map[string]string{"test": "pvc"}
	_, err = cfg.k8sClient.CoreV1().
		PersistentVolumeClaims(cfg.incrementalBackupInput.TargetPVCNamespace).
		Update(ctx, pvc, metav1.UpdateOptions{})
	require.NoError(t, err)

	pv, err := cfg.k8sClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Nil(t, pv.Annotations)
	pv.Annotations = map[string]string{"test": "pv"}
	_, err = cfg.k8sClient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	require.NoError(t, err)

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
		require.FileExists(t, diffFilePath, "diff part file should be created")
		// TODO: Check the contents of each diff part
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
				snapshotSize, maxPartSize := uint64(3072), uint64(1024)
				cfg := setup(t, &setupInput{
					fullSnapshotSize:        snapshotSize,
					incrementalSnapshotSize: snapshotSize,
					maxPartSize:             maxPartSize,
				})

				// Set up multipart diff files
				err := cfg.nlvRepo.MakeDiffDir(cfg.fullSnapshot.ID)
				require.NoError(t, err)
				for i := 0; i < tc.nextStorePart; i++ {
					stream, err := cfg.rbdRepo.ExportDiff(&model.ExportDiffInput{
						PoolName:       cfg.fullBackupInput.TargetRBDPoolName,
						ReadOffset:     cfg.fullBackupInput.MaxPartSize * uint64(i),
						ReadLength:     cfg.fullBackupInput.MaxPartSize,
						FromSnap:       nil,
						MidSnapPrefix:  cfg.fullSnapshot.Name,
						ImageName:      cfg.fullBackupInput.TargetRBDImageName,
						TargetSnapName: cfg.fullSnapshot.Name,
					})
					require.NoError(t, err)
					err = WriteDiffPartAndCloseStream(
						stream,
						cfg.nlvRepo.GetDiffPartPath(cfg.fullBackupInput.TargetSnapshotID, i),
						cfg.nlvRepo.GetDiffChecksumPath(cfg.fullBackupInput.TargetSnapshotID, i),
						cfg.fullBackupInput.DiffChecksumChunkSize,
					)
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
				fullData, err := os.ReadFile(cfg.nlvRepo.GetRawImagePath())
				require.NoError(t, err)
				require.Equal(t, int(cfg.incrementalBackupInput.ExpansionUnitSize), len(fullData))

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
	snapshotSize, maxPartSize := uint64(3072), uint64(1024)

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
		// TODO: Check the contents of each diff part
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
				SnapSize:  cfg.incrementalSnapshot.Size,
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
	testutil.AssertActionPrivateDataIsEmpty(t, cfg.finRepo, cfg.fullBackupInput.ActionUID)
	fullData, err := os.ReadFile(cfg.nlvRepo.GetRawImagePath())
	require.NoError(t, err)
	require.Equal(t, len(cfg.incrementalData), len(fullData))
	// TODO: Check the contents of each diff part

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
	ctx := context.Background()
	err := cfg.k8sClient.CoreV1().PersistentVolumes().Delete(ctx, cfg.targetPVName, metav1.DeleteOptions{})
	require.NoError(t, err)
	err = cfg.k8sClient.CoreV1().
		PersistentVolumeClaims(cfg.fullBackupInput.TargetPVCNamespace).
		Delete(ctx, cfg.fullBackupInput.TargetPVCName, metav1.DeleteOptions{})
	require.NoError(t, err)

	// Act
	backup := NewBackup(cfg.fullBackupInput)
	err = backup.Perform()

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
	ctx := context.Background()
	pv, err := cfg.k8sClient.CoreV1().PersistentVolumes().Get(ctx, cfg.targetPVName, metav1.GetOptions{})
	require.NoError(t, err)
	pv.Spec.CSI.VolumeAttributes["imageName"] = fmt.Sprintf("recreated-%s", pv.Spec.CSI.VolumeAttributes["imageName"])
	_, err = cfg.k8sClient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	require.NoError(t, err)

	// Act
	backup := NewBackup(cfg.fullBackupInput)
	err = backup.Perform()

	// Assert
	assert.Error(t, err)
}

func Test_FullBackup_Error_DifferentPVCUID(t *testing.T) {
	// CSATEST-1499
	// Description:
	//   Creation of a full backup fails when the target PVC has a different UID.
	//
	// Arrange:
	//   - Set up a full backup input.
	//   - Set a different PVC UID from the environment variable to the target PVC.
	//
	// Act:
	//   Run the backup process to create an full backup.
	//
	// Assert:
	//   Check if the full backup creation fails with an error.

	// Arrange
	cfg := setup(t, &setupInput{})
	ctx := context.Background()
	pvc, err := cfg.k8sClient.CoreV1().
		PersistentVolumeClaims(cfg.fullBackupInput.TargetPVCNamespace).
		Get(ctx, cfg.fullBackupInput.TargetPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	pvc.UID = types.UID(fmt.Sprintf("recreated-%s", pvc.UID))
	_, err = cfg.k8sClient.CoreV1().
		PersistentVolumeClaims(cfg.fullBackupInput.TargetPVCNamespace).
		Update(ctx, pvc, metav1.UpdateOptions{})
	require.NoError(t, err)

	// Act
	backup := NewBackup(cfg.fullBackupInput)
	err = backup.Perform()

	// Assert
	assert.Error(t, err)
}

func Test_IncrementalBackup_Error_WrongSourceCandidateSnapshotID(t *testing.T) {
	// CSATEST-1504
	// Description:
	//   Creation of an incremental backup fails when the source candidate
	//   snapshot ID does not match with the environment variable.
	//
	// Arrange:
	//   - Set up an incremental backup input.
	//   - Create a record in backup_metadata table.
	//   - Set a different snapshot ID to the source candidate snapshot ID.
	//
	// Act:
	//   Run the backup process to create an incremental backup.
	//
	// Assert:
	//   Check if the incremental backup creation fails with an error.

	// Arrange
	cfg := setup(t, &setupInput{})

	// Create a full backup to create a record in backup_metadata table
	fullBackup := NewBackup(cfg.fullBackupInput)
	err := fullBackup.Perform()
	require.NoError(t, err)

	// Set a different snapshot ID to the source candidate snapshot ID.
	// Use a clearly invalid/fake snapshot ID to ensure mismatch.
	cfg.incrementalBackupInput.SourceCandidateSnapshotID = ptr.To(math.MaxInt)
	metadata, err := job.GetBackupMetadata(cfg.finRepo)
	require.NoError(t, err)
	require.NotEqual(t, metadata.Raw.SnapID, *cfg.incrementalBackupInput.SourceCandidateSnapshotID)

	// Act
	backup := NewBackup(cfg.incrementalBackupInput)
	err = backup.Perform()

	// Assert
	assert.Error(t, err)
}

func Test_IncrementalBackup_Error_NonEmptyDiff(t *testing.T) {
	// CSATEST-1503
	// Description:
	//   Creation of an incremental backup fails when diff field is not empty
	//   i.e., there is already an incremental backup.
	//
	// Arrange:
	//   - Set up an incremental backup input.
	//   - Create a record in backup_metadata table.
	//   - Set a non-empty diff field in the backup metadata.
	//
	// Act:
	//   Run the backup process to create an incremental backup.
	//
	// Assert:
	//   Check if the incremental backup creation fails with an error.

	// Arrange
	cfg := setup(t, &setupInput{})

	// Create a full backup to create a record in backup_metadata table
	err := NewBackup(cfg.fullBackupInput).Perform()
	require.NoError(t, err)

	// Create an incremental backup to set a non-empty diff field in the backup metadata
	err = NewBackup(cfg.incrementalBackupInput).Perform()
	require.NoError(t, err)

	// Act
	backup := NewBackup(cfg.incrementalBackupInput2)
	err = backup.Perform()

	// Assert
	assert.Error(t, err)
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
		assert.Equal(t, int(expected.Raw.SnapSize), int(metadata.Raw.SnapSize))
		assert.Equal(t, int(expected.Raw.PartSize), int(metadata.Raw.PartSize))
		assert.True(t, expected.Raw.CreatedAt.Equal(metadata.Raw.CreatedAt))
	}

	if expected.Diff == nil {
		assert.Nil(t, metadata.Diff)
	} else {
		assert.NotNil(t, metadata.Diff)
		for i := range expected.Diff {
			assert.Equal(t, expected.Diff[i].SnapID, metadata.Diff[i].SnapID)
			assert.Equal(t, expected.Diff[i].SnapName, metadata.Diff[i].SnapName)
			assert.Equal(t, int(expected.Diff[i].SnapSize), int(metadata.Diff[i].SnapSize))
			assert.Equal(t, int(expected.Diff[i].PartSize), int(metadata.Diff[i].PartSize))
			assert.True(t, expected.Diff[i].CreatedAt.Equal(metadata.Diff[i].CreatedAt))
		}
	}
}

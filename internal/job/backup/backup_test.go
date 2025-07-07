package backup_test

import (
	"math"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

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
	backupInput := testutil.NewBackupInputTemplate(1, 512)
	targetSnapshotName := "test-snap"
	targetSnapshotSize := 1000
	ts, err := time.Parse(testutil.SnapshotTimeFormat, "Mon Jan  2 15:04:05 2006")
	require.NoError(t, err)
	targetSnapshotTimestamp := model.NewRBDTimeStamp(ts)
	targetPVName := "test-pv"

	fakePVC := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(backupInput.TargetPVCUID),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: targetPVName,
		},
	}
	fakePV := corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeAttributes: map[string]string{
						"imageName": backupInput.TargetRBDImageName,
					},
				},
			},
		},
	}

	k8sRepo := fake.NewKubernetesRepository(
		map[types.NamespacedName]*corev1.PersistentVolumeClaim{
			{Name: backupInput.TargetPVCName, Namespace: backupInput.TargetPVCNamespace}: &fakePVC,
		},
		map[string]*corev1.PersistentVolume{
			targetPVName: &fakePV,
		},
	)

	rbdRepo := fake.NewRBDRepository(map[fake.PoolImageName][]*model.RBDSnapshot{
		{PoolName: backupInput.TargetRBDPoolName, ImageName: backupInput.TargetRBDImageName}: {
			{
				ID:        backupInput.TargetSnapshotID,
				Name:      targetSnapshotName,
				Size:      targetSnapshotSize,
				Timestamp: targetSnapshotTimestamp,
			},
		},
	})

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	backupInput.Repo = finRepo
	backupInput.KubernetesRepo = k8sRepo
	backupInput.RBDRepo = rbdRepo
	backupInput.NodeLocalVolumeRepo = nlvRepo

	// Act
	backup := backup.NewBackup(backupInput)
	err = backup.Perform()
	require.NoError(t, err)

	// Assert
	testutil.AssertActionPrivateDataIsEmpty(t, finRepo, backupInput.ActionUID)

	rawImage, err := fake.ReadRawImage(nlvRepo.GetRawImagePath())
	assert.NoError(t, err)
	assert.Equal(t, targetSnapshotSize, rawImage.Size)
	assert.Equal(t, 2, len(rawImage.AppliedDiffs))
	assert.Equal(t, 0, rawImage.AppliedDiffs[0].ReadOffset)
	assert.Equal(t, backupInput.MaxPartSize, rawImage.AppliedDiffs[0].ReadLength)
	assert.Equal(t, backupInput.MaxPartSize, rawImage.AppliedDiffs[1].ReadOffset)
	assert.Equal(t, backupInput.MaxPartSize, rawImage.AppliedDiffs[1].ReadLength)

	resPVC, err := nlvRepo.GetPVC()
	assert.NoError(t, err)
	assert.True(t, equality.Semantic.DeepEqual(&fakePVC, resPVC))
	resPV, err := nlvRepo.GetPV()
	assert.NoError(t, err)
	assert.True(t, equality.Semantic.DeepEqual(&fakePV, resPV))

	for _, diff := range rawImage.AppliedDiffs {
		assert.Equal(t, backupInput.TargetRBDPoolName, diff.PoolName)
		assert.Nil(t, diff.FromSnap)
		assert.Equal(t, targetSnapshotName, diff.MidSnapPrefix)
		assert.Equal(t, backupInput.TargetRBDImageName, diff.ImageName)
		assert.Equal(t, backupInput.TargetSnapshotID, diff.SnapID)
		assert.Equal(t, targetSnapshotName, diff.SnapName)
		assert.Equal(t, targetSnapshotSize, diff.SnapSize)
		assert.Equal(t, targetSnapshotTimestamp.Time, diff.SnapTimestamp)
	}

	metadata, err := job.GetBackupMetadata(finRepo)
	require.NoError(t, err)
	assert.Equal(t, backupInput.TargetPVCUID, metadata.PVCUID)
	assert.Equal(t, backupInput.TargetRBDImageName, metadata.RBDImageName)
	assert.NotNil(t, metadata.Raw)
	assert.Equal(t, backupInput.TargetSnapshotID, metadata.Raw.SnapID)
	assert.Equal(t, targetSnapshotName, metadata.Raw.SnapName)
	assert.Equal(t, targetSnapshotSize, metadata.Raw.SnapSize)
	assert.Equal(t, backupInput.MaxPartSize, metadata.Raw.PartSize)
	assert.Equal(t, targetSnapshotTimestamp.Time, metadata.Raw.CreatedAt)
	assert.Empty(t, metadata.Diff)
}

func TestIncrementalBackup_Success(t *testing.T) {
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
	fullBackupInput := testutil.NewBackupInputTemplate(1, 512)
	fullSnapshotName := "test-snap1"
	fullSnapshotSize := 900
	fts, err := time.Parse(testutil.SnapshotTimeFormat, "Mon Jan  2 15:03:05 2006")
	require.NoError(t, err)
	fullSnapshotTimestamp := model.NewRBDTimeStamp(fts)
	targetPVName := "test-pv"

	incrementalBackupInput := testutil.NewIncrementalBackupInputTemplate(fullBackupInput, 2)
	incrementalSnapshotName := "test-snap2"
	incrementalSnapshotSize := 1000
	its, err := time.Parse(testutil.SnapshotTimeFormat, "Mon Jan  2 15:04:05 2006")
	require.NoError(t, err)
	incrementalSnapshotTimestamp := model.NewRBDTimeStamp(its)

	k8sRepo := fake.NewKubernetesRepository(
		map[types.NamespacedName]*corev1.PersistentVolumeClaim{
			{Name: fullBackupInput.TargetPVCName, Namespace: fullBackupInput.TargetPVCNamespace}: {
				ObjectMeta: metav1.ObjectMeta{
					UID: types.UID(fullBackupInput.TargetPVCUID),
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					VolumeName: targetPVName,
				},
			},
		},
		map[string]*corev1.PersistentVolume{
			targetPVName: {
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeAttributes: map[string]string{
								"imageName": fullBackupInput.TargetRBDImageName,
							},
						},
					},
				},
			},
		},
	)

	rbdRepo := fake.NewRBDRepository(map[fake.PoolImageName][]*model.RBDSnapshot{
		{PoolName: fullBackupInput.TargetRBDPoolName, ImageName: fullBackupInput.TargetRBDImageName}: {
			{
				ID:        fullBackupInput.TargetSnapshotID,
				Name:      fullSnapshotName,
				Size:      fullSnapshotSize,
				Timestamp: fullSnapshotTimestamp,
			},
			{
				ID:        incrementalBackupInput.TargetSnapshotID,
				Name:      incrementalSnapshotName,
				Size:      incrementalSnapshotSize,
				Timestamp: incrementalSnapshotTimestamp,
			},
		},
	})

	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	fullBackupInput.Repo = finRepo
	fullBackupInput.KubernetesRepo = k8sRepo
	fullBackupInput.RBDRepo = rbdRepo
	fullBackupInput.NodeLocalVolumeRepo = nlvRepo

	incrementalBackupInput.Repo = finRepo
	incrementalBackupInput.KubernetesRepo = k8sRepo
	incrementalBackupInput.RBDRepo = rbdRepo
	incrementalBackupInput.NodeLocalVolumeRepo = nlvRepo

	// Create a full backup
	fullBackup := backup.NewBackup(fullBackupInput)
	err = fullBackup.Perform()
	require.NoError(t, err)

	// Act
	backup := backup.NewBackup(incrementalBackupInput)
	err = backup.Perform()
	require.NoError(t, err)

	// Assert
	testutil.AssertActionPrivateDataIsEmpty(t, finRepo, fullBackupInput.ActionUID)
	numDiffParts := int(math.Ceil(float64(fullSnapshotSize) / float64(fullBackupInput.MaxPartSize)))
	for i := range numDiffParts {
		diffFilePath := nlvRepo.GetDiffPartPath(incrementalBackupInput.TargetSnapshotID, i)
		diff, err := fake.ReadDiff(diffFilePath)
		require.NoError(t, err)
		assert.Equal(t, fullBackupInput.TargetRBDPoolName, diff.PoolName)
		assert.Equal(t, fullSnapshotName, *diff.FromSnap)
		assert.Equal(t, incrementalSnapshotName, diff.MidSnapPrefix)
		assert.Equal(t, incrementalBackupInput.TargetRBDImageName, diff.ImageName)
		assert.Equal(t, incrementalBackupInput.TargetSnapshotID, diff.SnapID)
		assert.Equal(t, incrementalSnapshotName, diff.SnapName)
		assert.Equal(t, incrementalSnapshotSize, diff.SnapSize)
		assert.Equal(t, incrementalSnapshotTimestamp.Time, diff.SnapTimestamp)
	}

	metadata, err := job.GetBackupMetadata(finRepo)
	require.NoError(t, err)
	assert.Equal(t, fullBackupInput.TargetPVCUID, metadata.PVCUID)
	assert.Equal(t, fullBackupInput.TargetRBDImageName, metadata.RBDImageName)
	assert.NotNil(t, metadata.Raw)
	assert.Equal(t, fullBackupInput.TargetSnapshotID, metadata.Raw.SnapID)
	assert.Equal(t, fullSnapshotName, metadata.Raw.SnapName)
	assert.Equal(t, fullSnapshotSize, metadata.Raw.SnapSize)
	assert.Equal(t, fullBackupInput.MaxPartSize, metadata.Raw.PartSize)
	assert.Equal(t, fullSnapshotTimestamp.Time, metadata.Raw.CreatedAt)
	assert.NotNil(t, metadata.Diff)
	assert.Len(t, metadata.Diff, 1)
	assert.Equal(t, incrementalBackupInput.TargetSnapshotID, metadata.Diff[0].SnapID)
	assert.Equal(t, incrementalSnapshotName, metadata.Diff[0].SnapName)
	assert.Equal(t, incrementalSnapshotSize, metadata.Diff[0].SnapSize)
	assert.Equal(t, incrementalBackupInput.MaxPartSize, metadata.Diff[0].PartSize)
	assert.Equal(t, incrementalSnapshotTimestamp.Time, metadata.Diff[0].CreatedAt)
}

func TestBackup_ErrorBusy(t *testing.T) {
	// Arrange
	actionUID := uuid.New().String()
	differentActionUID := uuid.New().String()

	_, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

	err := finRepo.StartOrRestartAction(differentActionUID, model.Backup)
	require.NoError(t, err)

	// Act
	backup := backup.NewBackup(&backup.BackupInput{
		Repo:      finRepo,
		ActionUID: actionUID,
	})
	err = backup.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

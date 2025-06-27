package backup_test

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/infrastructure/sqlite"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestFullBackup_Success(t *testing.T) {
	// Arrange
	processUID := uuid.New().String()
	targetFinBackupUID := uuid.New().String()
	targetRBDPoolName := "test-pool"
	targetRBDImageName := "test-image"
	targetSnapshotID := 1
	targetSnapshotName := "test-snap"
	targetSnapshotSize := 1000
	targetSnapshotTimestamp := "Mon Jan  2 15:04:05 2006"
	targetPVCName := "test-pvc"
	targetPVCNamespace := "test-namespace"
	targetPVCUID := uuid.New().String()
	targetPVName := "test-pv"
	maxPartSize := 512

	k8sRepo := fake.NewKubernetesRepository(
		map[types.NamespacedName]*corev1.PersistentVolumeClaim{
			{Name: targetPVCName, Namespace: targetPVCNamespace}: {
				ObjectMeta: metav1.ObjectMeta{
					UID: types.UID(targetPVCUID),
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
								"imageName": targetRBDImageName,
							},
						},
					},
				},
			},
		},
	)

	rbdRepo := fake.NewRBDRepository(map[fake.PoolImageName][]*model.RBDSnapshot{
		{PoolName: targetRBDPoolName, ImageName: targetRBDImageName}: {
			{
				ID:        targetSnapshotID,
				Name:      targetSnapshotName,
				Size:      targetSnapshotSize,
				Timestamp: targetSnapshotTimestamp,
			},
		},
	})

	nlvRepo := createNLVForTest(t)

	finRepo, err := sqlite.New(getFinSqlite3DSN(nlvRepo.GetRootPath()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = finRepo.Close() })

	// Act
	backup := backup.NewBackup(&backup.BackupInput{
		Repo:                      finRepo,
		KubernetesRepo:            k8sRepo,
		RBDRepo:                   rbdRepo,
		NodeLocalVolumeRepo:       nlvRepo,
		RetryInterval:             1 * time.Second,
		ProcessUID:                processUID,
		TargetFinBackupUID:        targetFinBackupUID,
		TargetRBDPoolName:         targetRBDPoolName,
		TargetRBDImageName:        targetRBDImageName,
		TargetSnapshotID:          targetSnapshotID,
		SourceCandidateSnapshotID: nil,
		TargetPVCName:             targetPVCName,
		TargetPVCNamespace:        targetPVCNamespace,
		TargetPVCUID:              targetPVCUID,
		MaxPartSize:               maxPartSize,
	})
	err = backup.Perform()
	assert.NoError(t, err)

	// Assert
	assertActionPrivateDataIsEmpty(t, finRepo, processUID)
	assertDiffDirDoesNotExist(t, nlvRepo, getDiffDirPath(targetSnapshotID))

	rawImage, err := fake.ReadRawImage(filepath.Join(nlvRepo.GetRootPath(), getRawImagePath()))
	assert.NoError(t, err)
	assert.Equal(t, targetSnapshotSize, rawImage.Size)
	assert.Equal(t, 2, len(rawImage.AppliedDiffs))
	assert.Equal(t, 0, rawImage.AppliedDiffs[0].ReadOffset)
	assert.Equal(t, maxPartSize, rawImage.AppliedDiffs[0].ReadLength)
	assert.Equal(t, maxPartSize, rawImage.AppliedDiffs[1].ReadOffset)
	assert.Equal(t, maxPartSize, rawImage.AppliedDiffs[1].ReadLength)

	_, err = os.Stat(filepath.Join(nlvRepo.GetRootPath(), "pvc.yaml"))
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(nlvRepo.GetRootPath(), "pv.yaml"))
	assert.NoError(t, err)

	for _, diff := range rawImage.AppliedDiffs {
		assert.Equal(t, targetRBDPoolName, diff.PoolName)
		assert.Nil(t, diff.FromSnap)
		assert.Equal(t, targetFinBackupUID, diff.MidSnapPrefix)
		assert.Equal(t, targetRBDImageName, diff.ImageName)
		assert.Equal(t, targetSnapshotID, diff.SnapID)
		assert.Equal(t, targetSnapshotName, diff.SnapName)
		assert.Equal(t, targetSnapshotSize, diff.SnapSize)
		assert.Equal(t, targetSnapshotTimestamp, diff.SnapTimestamp)
	}

	metadata, err := job.GetBackupMetadata(finRepo)
	require.NoError(t, err)
	assert.Equal(t, targetPVCUID, metadata.PVCUID)
	assert.Equal(t, targetRBDImageName, metadata.RBDImageName)
	assert.NotNil(t, metadata.Raw)
	assert.Equal(t, targetSnapshotID, metadata.Raw.SnapID)
	assert.Equal(t, targetSnapshotName, metadata.Raw.SnapName)
	assert.Equal(t, targetSnapshotSize, metadata.Raw.SnapSize)
	assert.Equal(t, maxPartSize, metadata.Raw.PartSize)
	assert.Equal(t, targetSnapshotTimestamp, metadata.Raw.CreatedAt.Format(time.ANSIC))
	assert.Empty(t, metadata.Diff)
}

func TestIncrementalBackup_Success(t *testing.T) {
	// Arrange
	processUID := uuid.New().String()
	targetRBDPoolName := "test-pool"
	targetRBDImageName := "test-image"
	targetPVCName := "test-pvc"
	targetPVCNamespace := "test-namespace"
	targetPVCUID := uuid.New().String()
	targetPVName := "test-pv"
	maxPartSize := 512

	previousFinBackupUID := uuid.New().String()
	previousSnapshotID := 1
	previousSnapshotName := "test-snap1"
	previousSnapshotSize := 900
	previousSnapshotTimestamp := "Mon Jan  2 15:03:05 2006"

	targetFinBackupUID := uuid.New().String()
	targetSnapshotID := 2
	targetSnapshotName := "test-snap2"
	targetSnapshotSize := 1000
	targetSnapshotTimestamp := "Mon Jan  2 15:04:05 2006"

	k8sRepo := fake.NewKubernetesRepository(
		map[types.NamespacedName]*corev1.PersistentVolumeClaim{
			{Name: targetPVCName, Namespace: targetPVCNamespace}: {
				ObjectMeta: metav1.ObjectMeta{
					UID: types.UID(targetPVCUID),
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
								"imageName": targetRBDImageName,
							},
						},
					},
				},
			},
		},
	)

	rbdRepo := fake.NewRBDRepository(map[fake.PoolImageName][]*model.RBDSnapshot{
		{PoolName: targetRBDPoolName, ImageName: targetRBDImageName}: {
			{
				ID:        previousSnapshotID,
				Name:      previousSnapshotName,
				Size:      previousSnapshotSize,
				Timestamp: previousSnapshotTimestamp,
			},
			{
				ID:        targetSnapshotID,
				Name:      targetSnapshotName,
				Size:      targetSnapshotSize,
				Timestamp: targetSnapshotTimestamp,
			},
		},
	})

	nlvRepo := createNLVForTest(t)

	finRepo, err := sqlite.New(getFinSqlite3DSN(nlvRepo.GetRootPath()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = finRepo.Close() })

	// Create a previous backup to simulate an incremental backup scenario
	previousBackup := backup.NewBackup(&backup.BackupInput{
		Repo:                      finRepo,
		KubernetesRepo:            k8sRepo,
		RBDRepo:                   rbdRepo,
		NodeLocalVolumeRepo:       nlvRepo,
		RetryInterval:             1 * time.Second,
		ProcessUID:                processUID,
		TargetFinBackupUID:        previousFinBackupUID,
		TargetRBDPoolName:         targetRBDPoolName,
		TargetRBDImageName:        targetRBDImageName,
		TargetSnapshotID:          previousSnapshotID,
		SourceCandidateSnapshotID: nil,
		TargetPVCName:             targetPVCName,
		TargetPVCNamespace:        targetPVCNamespace,
		TargetPVCUID:              targetPVCUID,
		MaxPartSize:               maxPartSize,
	})
	err = previousBackup.Perform()
	require.NoError(t, err)

	// Act
	backup := backup.NewBackup(&backup.BackupInput{
		Repo:                      finRepo,
		KubernetesRepo:            k8sRepo,
		RBDRepo:                   rbdRepo,
		NodeLocalVolumeRepo:       nlvRepo,
		RetryInterval:             1 * time.Second,
		ProcessUID:                processUID,
		TargetFinBackupUID:        targetFinBackupUID,
		TargetRBDPoolName:         targetRBDPoolName,
		TargetRBDImageName:        targetRBDImageName,
		TargetSnapshotID:          targetSnapshotID,
		SourceCandidateSnapshotID: &previousSnapshotID,
		TargetPVCName:             targetPVCName,
		TargetPVCNamespace:        targetPVCNamespace,
		TargetPVCUID:              targetPVCUID,
		MaxPartSize:               maxPartSize,
	})
	err = backup.Perform()
	require.NoError(t, err)

	// Assert
	assertActionPrivateDataIsEmpty(t, finRepo, processUID)
	numDiffParts := int(math.Ceil(float64(targetSnapshotSize) / float64(maxPartSize)))
	for i := range numDiffParts {
		diffFilePath := filepath.Join(nlvRepo.GetRootPath(), getDiffDirPath(targetSnapshotID), fmt.Sprintf("part-%d", i))
		diff, err := fake.ReadDiff(diffFilePath)
		require.NoError(t, err)
		assert.Equal(t, targetRBDPoolName, diff.PoolName)
		assert.Equal(t, previousSnapshotName, *diff.FromSnap)
		assert.Equal(t, targetFinBackupUID, diff.MidSnapPrefix)
		assert.Equal(t, targetRBDImageName, diff.ImageName)
		assert.Equal(t, targetSnapshotID, diff.SnapID)
		assert.Equal(t, targetSnapshotName, diff.SnapName)
		assert.Equal(t, targetSnapshotSize, diff.SnapSize)
		assert.Equal(t, targetSnapshotTimestamp, diff.SnapTimestamp)
	}

	metadata, err := job.GetBackupMetadata(finRepo)
	require.NoError(t, err)
	assert.Equal(t, targetPVCUID, metadata.PVCUID)
	assert.Equal(t, targetRBDImageName, metadata.RBDImageName)
	assert.NotNil(t, metadata.Raw)
	assert.Equal(t, previousSnapshotID, metadata.Raw.SnapID)
	assert.Equal(t, previousSnapshotName, metadata.Raw.SnapName)
	assert.Equal(t, previousSnapshotSize, metadata.Raw.SnapSize)
	assert.Equal(t, maxPartSize, metadata.Raw.PartSize)
	assert.Equal(t, previousSnapshotTimestamp, metadata.Raw.CreatedAt.Format(time.ANSIC))
	assert.NotNil(t, metadata.Diff)
	assert.Len(t, metadata.Diff, 1)
	assert.Equal(t, targetSnapshotID, metadata.Diff[0].SnapID)
	assert.Equal(t, targetSnapshotName, metadata.Diff[0].SnapName)
	assert.Equal(t, targetSnapshotSize, metadata.Diff[0].SnapSize)
	assert.Equal(t, maxPartSize, metadata.Diff[0].PartSize)
	assert.Equal(t, targetSnapshotTimestamp, metadata.Diff[0].CreatedAt.Format(time.ANSIC))
}

func TestBackup_ErrorBusy(t *testing.T) {
	// Arrange
	processUID := uuid.New().String()
	differentProcessUID := uuid.New().String()

	nlvRepo := createNLVForTest(t)

	finRepo, err := sqlite.New(getFinSqlite3DSN(nlvRepo.GetRootPath()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = finRepo.Close() })

	err = finRepo.StartOrRestartAction(differentProcessUID, model.Backup)
	require.NoError(t, err)

	// Act
	backup := backup.NewBackup(&backup.BackupInput{
		Repo:       finRepo,
		ProcessUID: processUID,
	})
	err = backup.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

func createNLVForTest(t *testing.T) *nlv.NodeLocalVolumeRepository {
	t.Helper()

	rootPath, err := os.MkdirTemp("", "fin-fake-nlv")
	require.NoError(t, err)

	repo := nlv.NewNodeLocalVolumeRepository(rootPath)
	t.Cleanup(repo.Cleanup)
	return repo
}

func assertActionPrivateDataIsEmpty(t *testing.T, finRepo model.FinRepository, processUID string) {
	t.Helper()
	_, err := finRepo.GetActionPrivateData(processUID)
	assert.ErrorIs(t, err, model.ErrNotFound)
}

func assertDiffDirDoesNotExist(t *testing.T, nlvRepo model.NodeLocalVolumeRepository, diffDir string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(nlvRepo.GetRootPath(), diffDir))
	assert.True(t, os.IsNotExist(err))
}

func getDiffDirPath(snapshotID int) string {
	return filepath.Join("diff", fmt.Sprintf("%d", snapshotID))
}

func getRawImagePath() string {
	return "raw.img"
}

func getFinSqlite3DSN(rootPath string) string {
	return fmt.Sprintf("file:%s?_txlock=exclusive", filepath.Join(rootPath, "fin.sqlite3"))
}

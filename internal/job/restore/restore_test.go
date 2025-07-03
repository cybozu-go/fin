package restore_test

import (
	"bytes"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/infrastructure/sqlite"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/restore"
	"github.com/cybozu-go/fin/internal/job/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestoreFromFullBackup_Success(t *testing.T) {
	// Description:
	//  Restore from the full backup to the restore file.
	//
	// Arrange:
	//   - A full backup, `backup`, consists of two chunks.
	//   - raw.img filled with the random data.
	//   - The restore file.
	// Act:
	//   Run the restore process.
	//
	// Assert:
	//   Check if the contents of the restore file is
	//   the same as raw.img.

	// Arrange

	// Create a full backup data.
	processUID := uuid.New().String()
	targetFinBackupUID := uuid.New().String()
	targetRBDPoolName := "test-pool"
	targetRBDImageName := "test-image"
	targetSnapshotID := 1
	targetSnapshotName := "test-snap"
	rawImageChunkSize := 4096
	targetSnapshotSize := rawImageChunkSize * 2
	targetSnapshotTimestamp := "Mon Jan  2 15:04:05 2006"
	targetPVCName := "test-pvc"
	targetPVCNamespace := "test-namespace"
	targetPVCUID := uuid.New().String()
	targetPVName := "test-pv"
	maxPartSize := 4096

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

	nlvRepo := testutil.CreateNLVForTest(t)

	finRepo, err := sqlite.New(testutil.GetFinSqlite3DSN(nlvRepo.GetRootPath()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = finRepo.Close() })

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
	require.NoError(t, err)

	// Update raw.img filled with random data. Although this file has some data
	// stored by fake backup process, we can replace them here because we won't
	// use the original data in the restore process.
	buf := make([]byte, targetSnapshotSize)
	_, err = rand.Read(buf)
	require.NoError(t, err)
	raw, err := os.Create(nlvRepo.GetRawImagePath())
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()
	_, err = raw.Write(buf)
	require.NoError(t, err)
	err = raw.Close()
	require.NoError(t, err)

	// Create the restore file
	restorePath := testutil.CreateRestoreFileForTest(t, int64(targetSnapshotSize))
	rRepo := fake.NewRestoreRepository(restorePath)

	// Act
	r := restore.NewRestore(&restore.RestoreInput{
		Repo:                finRepo,
		KubernetesRepo:      k8sRepo,
		NodeLocalVolumeRepo: nlvRepo,
		RestoreRepo:         rRepo,
		RetryInterval:       1 * time.Second,
		ProcessUID:          processUID,
		TargetSnapshotID:    targetSnapshotID,
		RawImageChunkSize:   int64(rawImageChunkSize),
		TargetPVCName:       targetPVCName,
		TargetPVCNamespace:  targetPVCNamespace,
		TargetPVCUID:        targetPVCUID,
	})
	err = r.Perform()
	require.NoError(t, err)

	// Assert

	// Verify the contents of the restore file.
	buf2 := make([]byte, targetSnapshotSize)
	restoreFile, err := os.Open(restorePath)
	require.NoError(t, err)
	rn, err := restoreFile.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, targetSnapshotSize, rn)
	require.True(t, bytes.Equal(buf, buf2))

	// Verify the contents of the metadata
	testutil.AssertActionPrivateDataIsEmpty(t, finRepo, processUID)
	require.Zero(t, len(rRepo.AppliedDiffs()))
}

func TestRestoreFromIncrementalBackup_Success(t *testing.T) {
	// Description:
	//   Restore from the incremental backup to the restore file.
	//
	// Arrange:
	//   - A full backup, `backup`, consists of 2 chunks.
	//   - An incremental backup, `previousBackup`, consists of 3 chunks.
	//   - raw.img filled with the random data.
	//     It's size is the same as the full backup's one.
	//   - The restore file. It's size is the same
	//     as the incremental backup's one.
	//
	// Act:
	//   Run the restore process about the incremental backup.
	//
	// Assert:
	//   Check if the contents of the restore target file is as follows.
	//     chunk0, 1: the same as the fake raw.img.
	//     chunk2: zero filled.

	// Arrange
	rawImageChunkSize := 4096
	processUID := uuid.New().String()
	targetRBDPoolName := "test-pool"
	targetRBDImageName := "test-image"
	targetPVCName := "test-pvc"
	targetPVCNamespace := "test-namespace"
	targetPVCUID := uuid.New().String()
	targetPVName := "test-pv"
	maxPartSize := rawImageChunkSize

	previousFinBackupUID := uuid.New().String()
	previousSnapshotID := 1
	previousSnapshotName := "test-snap1"
	previousSnapshotSize := rawImageChunkSize * 2
	previousSnapshotTimestamp := "Mon Jan  2 15:03:05 2006"

	targetFinBackupUID := uuid.New().String()
	targetSnapshotID := 2
	targetSnapshotName := "test-snap2"
	targetSnapshotSize := rawImageChunkSize * 3
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

	nlvRepo := testutil.CreateNLVForTest(t)

	finRepo, err := sqlite.New(testutil.GetFinSqlite3DSN(nlvRepo.GetRootPath()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = finRepo.Close() })

	// Create a full backup
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

	// Create an incremental backup
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

	// Update raw.img filled with random data. Although this file has some data
	// stored by fake backup process, we can replace them here because we won't
	// use the original data in the restore process.
	buf := make([]byte, previousSnapshotSize)
	_, err = rand.Read(buf)
	require.NoError(t, err)
	raw, err := os.Create(nlvRepo.GetRawImagePath())
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()
	_, err = raw.Write(buf)
	require.NoError(t, err)
	err = raw.Close()
	require.NoError(t, err)

	// Create the restore file. It's size must be the same as the incremental backup's one.
	restorePath := testutil.CreateRestoreFileForTest(t, int64(targetSnapshotSize))
	rRepo := fake.NewRestoreRepository(restorePath)

	// Act
	r := restore.NewRestore(&restore.RestoreInput{
		Repo:                finRepo,
		KubernetesRepo:      k8sRepo,
		NodeLocalVolumeRepo: nlvRepo,
		RestoreRepo:         rRepo,
		RetryInterval:       1 * time.Second,
		ProcessUID:          processUID,
		TargetSnapshotID:    targetSnapshotID,
		RawImageChunkSize:   int64(rawImageChunkSize),
		TargetPVCName:       targetPVCName,
		TargetPVCNamespace:  targetPVCNamespace,
		TargetPVCUID:        targetPVCUID,
	})
	err = r.Perform()
	require.NoError(t, err)

	// Assert
	testutil.AssertActionPrivateDataIsEmpty(t, finRepo, processUID)

	buf2 := make([]byte, targetSnapshotSize)
	restoreFile, err := os.Open(rRepo.GetPath())
	require.NoError(t, err)
	rn, err := restoreFile.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, targetSnapshotSize, rn)
	require.True(t, bytes.Equal(buf, buf2[:previousSnapshotSize]))
	zeroBuf := make([]byte, targetSnapshotSize-previousSnapshotSize)
	require.True(t, bytes.Equal(buf2[previousSnapshotSize:], zeroBuf))

	assert.Equal(t, 3, len(rRepo.AppliedDiffs()))
	assert.Equal(t, 0, rRepo.AppliedDiffs()[0].ReadOffset)
	assert.Equal(t, maxPartSize, rRepo.AppliedDiffs()[0].ReadLength)
	assert.Equal(t, maxPartSize, rRepo.AppliedDiffs()[1].ReadOffset)
	assert.Equal(t, maxPartSize, rRepo.AppliedDiffs()[1].ReadLength)
	assert.Equal(t, maxPartSize*2, rRepo.AppliedDiffs()[2].ReadOffset)
	assert.Equal(t, maxPartSize, rRepo.AppliedDiffs()[2].ReadLength)
}

func TestRestore_ErrorBusy(t *testing.T) {
	// Arrange
	processUID := uuid.New().String()
	differentProcessUID := uuid.New().String()

	nlvRepo := testutil.CreateNLVForTest(t)

	finRepo, err := sqlite.New(testutil.GetFinSqlite3DSN(nlvRepo.GetRootPath()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = finRepo.Close() })

	err = finRepo.StartOrRestartAction(differentProcessUID, model.Backup)
	require.NoError(t, err)

	// Act
	restore := restore.NewRestore(&restore.RestoreInput{
		Repo:       finRepo,
		ProcessUID: processUID,
	})
	err = restore.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

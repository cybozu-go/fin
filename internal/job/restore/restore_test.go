package restore_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
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
	//   Restore from the full backup to the restore file.
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
	backupInput := testutil.NewBackupInputTemplate(1, 4096)
	targetSnapshotName := "test-snap"
	rawImageChunkSize := 4096
	targetSnapshotSize := rawImageChunkSize * 2
	targetSnapshotTimestamp := "Mon Jan  2 15:04:05 2006"
	targetPVName := "test-pv"

	k8sRepo := fake.NewKubernetesRepository(
		map[types.NamespacedName]*corev1.PersistentVolumeClaim{
			{Name: backupInput.TargetPVCName, Namespace: backupInput.TargetPVCNamespace}: {
				ObjectMeta: metav1.ObjectMeta{
					UID: types.UID(backupInput.TargetPVCUID),
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
								"imageName": backupInput.TargetRBDImageName,
							},
						},
					},
				},
			},
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

	nlvRepo, finRepo := testutil.CreateNLVAndFinRepoForTest(t)

	backupInput.Repo = finRepo
	backupInput.KubernetesRepo = k8sRepo
	backupInput.RBDRepo = rbdRepo
	backupInput.NodeLocalVolumeRepo = nlvRepo

	backup := backup.NewBackup(backupInput)
	err := backup.Perform()
	require.NoError(t, err)

	// Fill raw.img with random data. Although this file has some data
	// stored by fake backup process, we can replace them here because we won't
	// use the original data in the restore process.
	buf := testutil.FillRawImageWithRandomData(t, nlvRepo.GetRawImagePath(), targetSnapshotSize)

	// Create the restore file
	restorePath := testutil.CreateRestoreFileForTest(t, int64(targetSnapshotSize))
	rVol := fake.NewRestoreVolume(restorePath)

	// Act
	r := restore.NewRestore(testutil.NewRestoreInputTemplate(
		backupInput, rVol, rawImageChunkSize, backupInput.TargetSnapshotID))

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
	testutil.AssertActionPrivateDataIsEmpty(t, finRepo, backupInput.ProcessUID)
	require.Zero(t, len(rVol.AppliedDiffs()))
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
	fullBackupInput := testutil.NewBackupInputTemplate(1, 4096)
	fullSnapshotName := "test-snap1"
	rawImageChunkSize := 4096
	fullSnapshotSize := rawImageChunkSize * 2
	fullSnapshotTimestamp := "Mon Jan  2 15:03:05 2006"
	targetPVName := "test-pv"

	incrementalBackupInput := testutil.NewIncrementalBackupInputTemplate(fullBackupInput, 2)
	incrementalBackupInput.SourceCandidateSnapshotID = &fullBackupInput.TargetSnapshotID
	incrementalSnapshotName := "test-snap2"
	incrementalSnapshotSize := rawImageChunkSize * 3
	incrementalSnapshotTimestamp := "Mon Jan  2 15:04:05 2006"

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

	nlvRepo, finRepo := testutil.CreateNLVAndFinRepoForTest(t)

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
	err := fullBackup.Perform()
	require.NoError(t, err)

	// Create an incremental backup
	incrementalBackup := backup.NewBackup(incrementalBackupInput)
	err = incrementalBackup.Perform()
	require.NoError(t, err)

	// Fill raw.img with random data. Although this file has some data
	// stored by fake backup process, we can replace them here because we won't
	// use the original data in the restore process.
	buf := testutil.FillRawImageWithRandomData(t, nlvRepo.GetRawImagePath(), fullSnapshotSize)

	// Create the restore file
	restorePath := testutil.CreateRestoreFileForTest(t, int64(incrementalSnapshotSize))
	rVol := fake.NewRestoreVolume(restorePath)

	// Act
	r := restore.NewRestore(testutil.NewRestoreInputTemplate(
		incrementalBackupInput, rVol, rawImageChunkSize, incrementalBackupInput.TargetSnapshotID))
	err = r.Perform()
	require.NoError(t, err)

	// Assert
	testutil.AssertActionPrivateDataIsEmpty(t, finRepo, fullBackupInput.ProcessUID)

	buf2 := make([]byte, incrementalSnapshotSize)
	restoreFile, err := os.Open(rVol.GetPath())
	require.NoError(t, err)
	rn, err := restoreFile.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, incrementalSnapshotSize, rn)
	require.True(t, bytes.Equal(buf, buf2[:fullSnapshotSize]))
	zeroBuf := make([]byte, incrementalSnapshotSize-fullSnapshotSize)
	require.True(t, bytes.Equal(buf2[fullSnapshotSize:], zeroBuf))

	assert.Equal(t, 3, len(rVol.AppliedDiffs()))
	assert.Equal(t, 0, rVol.AppliedDiffs()[0].ReadOffset)
	assert.Equal(t, fullBackupInput.MaxPartSize, rVol.AppliedDiffs()[0].ReadLength)
	assert.Equal(t, fullBackupInput.MaxPartSize, rVol.AppliedDiffs()[1].ReadOffset)
	assert.Equal(t, fullBackupInput.MaxPartSize, rVol.AppliedDiffs()[1].ReadLength)
	assert.Equal(t, fullBackupInput.MaxPartSize*2, rVol.AppliedDiffs()[2].ReadOffset)
	assert.Equal(t, fullBackupInput.MaxPartSize, rVol.AppliedDiffs()[2].ReadLength)
}

func TestRestore_ErrorBusy(t *testing.T) {
	// Arrange
	processUID := uuid.New().String()
	differentProcessUID := uuid.New().String()

	_, finRepo := testutil.CreateNLVAndFinRepoForTest(t)

	err := finRepo.StartOrRestartAction(differentProcessUID, model.Backup)
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

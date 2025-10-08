package verification

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/db"
	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/cybozu-go/fin/test/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

const (
	// pathXFSMount must be an XFS filesystem mount point.
	// This value should be the same as TEST_XFS_MOUNT in the Makefile.
	pathXFSMount = "../../../_test/test-xfs-mnt"
)

type countingRBDRepository2 struct {
	*fake.RBDRepository2
	applyDiffCount int
}

var _ model.RBDRepository = (*countingRBDRepository2)(nil)

func (r *countingRBDRepository2) ApplyDiffToRawImage(
	rawImagePath, diffPath, sourceSnapshotName, targetSnapshotName string,
) error {
	r.applyDiffCount++
	return r.RBDRepository2.ApplyDiffToRawImage(
		rawImagePath, diffPath, sourceSnapshotName, targetSnapshotName)
}

func TestMain(m *testing.M) {
	if os.Getenv("FIN_RAW_IMG_EXPANSION_UNIT_SIZE") == "" {
		if err := os.Setenv("FIN_RAW_IMG_EXPANSION_UNIT_SIZE", "4096"); err != nil {
			panic("failed to set env: " + err.Error())
		}
	}
	m.Run()
}

type setupInput struct {
	volumeMode *corev1.PersistentVolumeMode
}

type setupOutput struct {
	fullBackupInput, incrementalBackupInput *input.Backup
	k8sClient                               kubernetes.Interface
	finRepo                                 model.FinRepository
	nlvRepo                                 *nlv.NodeLocalVolumeRepository
	rbdRepo                                 *countingRBDRepository2
	fullSnapshot, incrementalSnapshot       *model.RBDSnapshot
	fullVolume, incrementalVolume           []byte
}

func setup(t *testing.T, input *setupInput) *setupOutput {
	k8sClient, _, volumeInfo := fake.NewStorage()
	pvc, err := k8sClient.CoreV1().PersistentVolumeClaims(volumeInfo.Namespace).
		Get(t.Context(), volumeInfo.PVCName, metav1.GetOptions{})
	require.NoError(t, err)
	pvc.Spec.VolumeMode = input.volumeMode
	_, err = k8sClient.CoreV1().PersistentVolumeClaims(volumeInfo.Namespace).
		Update(t.Context(), pvc, metav1.UpdateOptions{})
	require.NoError(t, err)

	rbdRepo := fake.NewRBDRepository2(volumeInfo.PoolName, volumeInfo.ImageName)

	nlvDir, err := os.MkdirTemp(pathXFSMount, "fin-test-verification-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(nlvDir) })
	nlvRepo, err := nlv.NewNodeLocalVolumeRepository(nlvDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nlvRepo.Close() })

	finRepo, err := db.New(nlvRepo.GetDBPath())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = finRepo.Close()
		_ = os.Remove(nlvRepo.GetDBPath())
	})

	fullSnapshot, fullVolume, err := rbdRepo.CreateSnapshotWithRandomData(utils.GetUniqueName("snap-"), 4096*2)
	require.NoError(t, err)

	maxPartSize := uint64(4096)

	fullBackupInput := testutil.NewBackupInput(k8sClient, volumeInfo, fullSnapshot.ID, nil, maxPartSize)
	fullBackupInput.Repo = finRepo
	fullBackupInput.K8sClient = k8sClient
	fullBackupInput.RBDRepo = rbdRepo
	fullBackupInput.NodeLocalVolumeRepo = nlvRepo

	incrementalSnapshot, incrementalVolume, err := rbdRepo.CreateSnapshotWithRandomData(
		utils.GetUniqueName("snap-"), 4096*2)
	require.NoError(t, err)

	incrementalBackupInput := testutil.NewBackupInput(
		k8sClient, volumeInfo, incrementalSnapshot.ID, &fullSnapshot.ID, maxPartSize)
	incrementalBackupInput.Repo = finRepo
	incrementalBackupInput.K8sClient = k8sClient
	incrementalBackupInput.RBDRepo = rbdRepo
	incrementalBackupInput.NodeLocalVolumeRepo = nlvRepo

	return &setupOutput{
		fullBackupInput:        fullBackupInput,
		incrementalBackupInput: incrementalBackupInput,
		k8sClient:              k8sClient,
		finRepo:                finRepo,
		nlvRepo:                nlvRepo,
		rbdRepo:                &countingRBDRepository2{rbdRepo, 0},
		fullSnapshot:           fullSnapshot,
		incrementalSnapshot:    incrementalSnapshot,
		fullVolume:             fullVolume,
		incrementalVolume:      incrementalVolume,
	}
}

func ensureVerificationCompleted(
	t *testing.T,
	finRepo model.FinRepository,
	nlvRepo *nlv.NodeLocalVolumeRepository,
	actionUID string,
) {
	t.Helper()

	var err error
	_, err = finRepo.GetActionPrivateData(actionUID)
	require.ErrorIs(t, err, model.ErrNotFound)

	_, err = os.Stat(nlvRepo.GetInstantVerifyImagePath())
	require.ErrorIs(t, err, os.ErrNotExist)
}

func createFakeE2fsck(t *testing.T, expectedVolumeData []byte) {
	t.Helper()

	binDir := filepath.Join(t.TempDir(), "bin")
	err := os.MkdirAll(binDir, 0755)
	require.NoError(t, err)

	e2fsck, err := os.Create(filepath.Join(binDir, "e2fsck"))
	require.NoError(t, err)
	defer func() { _ = e2fsck.Close() }()
	err = e2fsck.Chmod(0755)
	require.NoError(t, err)
	_, err = fmt.Fprintf(e2fsck, `#!/bin/sh
if [ "$1" != "-fn" ]; then
  echo "Invalid argument: $1" >&2
  exit 1
fi
shift

expected=/tmp/fin-fake-e2fsck-expected
echo -n "%x" | xxd -r -p > ${expected}
cmp "$1" ${expected}
result=$?
rm ${expected}
exit ${result}
`, expectedVolumeData)
	require.NoError(t, err)
	err = e2fsck.Close()
	require.NoError(t, err)

	t.Setenv("PATH", fmt.Sprintf("%s:%s", binDir, os.Getenv("PATH")))
}

func TestVerification_Success_FullBackup(t *testing.T) {
	// Description:
	//   Perform verification after a full backup and check it succeeds.
	//
	// Arrange:
	//   Set up a PVC and create a full backup of it.
	//
	// Act:
	//   Perform verification on the backed-up data.
	//
	// Assert:
	//   - Check that verification completes successfully without errors.
	//   - Ensure that the verification action is marked as completed in the Fin repository.
	//   - Verify that the instant verification image is removed after verification.

	// Arrange
	cfg := setup(t, &setupInput{})

	backup := backup.NewBackup(cfg.fullBackupInput)
	err := backup.Perform()
	require.NoError(t, err)

	createFakeE2fsck(t, cfg.fullVolume)

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          cfg.nlvRepo,
		ActionUID:        cfg.fullBackupInput.ActionUID,
		TargetSnapshotID: cfg.fullBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.fullBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	require.NoError(t, err)
	ensureVerificationCompleted(t, cfg.finRepo, cfg.nlvRepo, cfg.fullBackupInput.ActionUID)
}

func TestVerification_Success_IncrementalBackup(t *testing.T) {
	// Description:
	//   Perform verification after an incremental backup and check it succeeds.
	//
	// Arrange:
	//   Set up a PVC and create a full backup followed by an incremental backup of it.
	//
	// Act:
	//   Perform verification on the backed-up data.
	//
	// Assert:
	//   - Check that verification completes successfully without errors.
	//   - Ensure that the verification action is marked as completed in the Fin repository.
	//   - Verify that the instant verification image is removed after verification.

	// Arrange
	cfg := setup(t, &setupInput{})

	fullBackup := backup.NewBackup(cfg.fullBackupInput)
	err := fullBackup.Perform()
	require.NoError(t, err)
	incrementalBackup := backup.NewBackup(cfg.incrementalBackupInput)
	err = incrementalBackup.Perform()
	require.NoError(t, err)

	createFakeE2fsck(t, cfg.incrementalVolume)

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          cfg.nlvRepo,
		ActionUID:        cfg.incrementalBackupInput.ActionUID,
		TargetSnapshotID: cfg.incrementalBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.incrementalBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	require.NoError(t, err)
	ensureVerificationCompleted(t, cfg.finRepo, cfg.nlvRepo, cfg.incrementalBackupInput.ActionUID)
}

func TestVerification_Success_VolumeModeBlock(t *testing.T) {
	// Description:
	//   Perform verification on a PVC with VolumeMode set to Block and check it succeeds.
	//
	// Arrange:
	//   Set up a PVC with VolumeMode set to Block and create a full backup of it.
	//
	// Act:
	//   Perform verification on the backed-up data.
	//
	// Assert:
	//   - Check that verification completes successfully without errors.
	//   - Ensure that the verification action is marked as completed in the Fin repository.
	//   - Verify that the instant verification image is removed after verification.

	// Arrange
	cfg := setup(t, &setupInput{volumeMode: ptr.To(corev1.PersistentVolumeBlock)})

	backup := backup.NewBackup(cfg.fullBackupInput)
	err := backup.Perform()
	require.NoError(t, err)

	createFakeE2fsck(t, cfg.fullVolume)

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          cfg.nlvRepo,
		ActionUID:        cfg.fullBackupInput.ActionUID,
		TargetSnapshotID: cfg.fullBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.fullBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	require.NoError(t, err)
	ensureVerificationCompleted(t, cfg.finRepo, cfg.nlvRepo, cfg.fullBackupInput.ActionUID)
}

func TestVerification_Success_Resume(t *testing.T) {
	// Description:
	//   Perform verification after an incremental backup with interruption and check it succeeds.
	//
	// Arrange:
	//   Set up a PVC and create a full backup followed by an incremental backup of it.
	//   Then, emulate an interruption during the verification process after applying the first diff part.
	//
	// Act:
	//   Perform verification on the backed-up data, resuming from the interruption.
	//
	// Assert:
	//   - Check that verification completes successfully without errors.
	//   - Confirm that the expected number of diff parts were applied during the verification process.
	//   - Ensure that the verification action is marked as completed in the Fin repository.
	//   - Verify that the instant verification image is removed after verification.

	// Arrange
	cfg := setup(t, &setupInput{})

	fullBackup := backup.NewBackup(cfg.fullBackupInput)
	err := fullBackup.Perform()
	require.NoError(t, err)
	incrementalBackup := backup.NewBackup(cfg.incrementalBackupInput)
	err = incrementalBackup.Perform()
	require.NoError(t, err)

	require.Equal(t, 2, calculateMaxPartNumber(&job.BackupMetadataEntry{
		SnapSize: cfg.incrementalSnapshot.Size,
		PartSize: cfg.incrementalBackupInput.MaxPartSize,
	}))

	createFakeE2fsck(t, cfg.incrementalVolume)

	// Emulate interruption after applying the first diff part
	err = cfg.finRepo.StartOrRestartAction(cfg.incrementalBackupInput.ActionUID, model.Verification)
	require.NoError(t, err)
	err = cfg.nlvRepo.ReflinkRawImageToInstantVerifyImage()
	require.NoError(t, err)
	sourceSnapshotName, targetSnapshotName :=
		job.CalcSnapshotNamesWithOffset(
			cfg.fullSnapshot.Name,
			cfg.incrementalSnapshot.Name,
			0,
			2,
			cfg.incrementalBackupInput.MaxPartSize,
		)
	err = cfg.rbdRepo.ApplyDiffToRawImage(
		cfg.nlvRepo.GetInstantVerifyImagePath(),
		cfg.nlvRepo.GetDiffPartPath(cfg.incrementalSnapshot.ID, 0),
		sourceSnapshotName,
		targetSnapshotName,
	)
	require.NoError(t, err)
	err = cfg.finRepo.UpdateActionPrivateData(cfg.incrementalBackupInput.ActionUID, []byte("{\"nextDiffPart\":1}"))
	require.NoError(t, err)

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          cfg.nlvRepo,
		ActionUID:        cfg.incrementalBackupInput.ActionUID,
		TargetSnapshotID: cfg.incrementalBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.incrementalBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	require.NoError(t, err)
	require.Equal(t, 2, cfg.rbdRepo.applyDiffCount)
	ensureVerificationCompleted(t, cfg.finRepo, cfg.nlvRepo, cfg.incrementalBackupInput.ActionUID)
}

func TestBackup_ErrorBusy(t *testing.T) {
	// Description:
	//   Attempt to perform verification when another action is already in progress and check it fails.
	//
	// Arrange:
	//   Start another action to simulate a busy state.
	//
	// Act:
	//   Attempt to perform verification.
	//
	// Assert:
	//   Check that verification fails with job.ErrCantLock.

	// Arrange
	cfg := setup(t, &setupInput{})

	differentActionUID := uuid.New().String()
	require.NotEqual(t, cfg.fullBackupInput.ActionUID, differentActionUID)

	err := cfg.finRepo.StartOrRestartAction(differentActionUID, model.Backup)
	require.NoError(t, err)

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          cfg.nlvRepo,
		ActionUID:        cfg.fullBackupInput.ActionUID,
		TargetSnapshotID: cfg.fullBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.fullBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	assert.ErrorIs(t, err, job.ErrCantLock)
}

func TestVerification_Error_FsckFailure(t *testing.T) {
	// Description:
	//   Perform verification after a full backup and simulate fsck failure.
	//
	// Arrange:
	//   Set up a PVC and create a full backup.
	//
	// Act:
	//   Perform verification on the backed-up data, expecting fsck to fail.
	//
	// Assert:
	//   - Check that verification fails with ErrFsckFailed.

	// Arrange
	cfg := setup(t, &setupInput{})

	backup := backup.NewBackup(cfg.fullBackupInput)
	err := backup.Perform()
	require.NoError(t, err)

	createFakeE2fsck(t, []byte("") /* wrong data */)

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          cfg.nlvRepo,
		ActionUID:        cfg.fullBackupInput.ActionUID,
		TargetSnapshotID: cfg.fullBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.fullBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	require.ErrorIs(t, err, ErrFsckFailed)
}

func TestVerification_Error_FsckCommandNotFound(t *testing.T) {
	// Description:
	//   Perform verification after a full backup and emulate fsck command not found.
	//
	// Arrange:
	//   Set up a PVC and create a full backup. Don't create a fake e2fsck command.
	//
	// Act:
	//   Perform verification on the backed-up data, expecting fsck not to be found.
	//
	// Assert:
	//   - Check that verification fails with an error other than ErrFsckFailed.

	// Arrange
	cfg := setup(t, &setupInput{})

	backup := backup.NewBackup(cfg.fullBackupInput)
	err := backup.Perform()
	require.NoError(t, err)

	t.Setenv("PATH", "") // Emulate command not found

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          cfg.nlvRepo,
		ActionUID:        cfg.fullBackupInput.ActionUID,
		TargetSnapshotID: cfg.fullBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.fullBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrFsckFailed)
}

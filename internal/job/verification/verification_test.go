package verification

import (
	"io"
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/testutil"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/cybozu-go/fin/test/utils"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type NoReflinkNLVRepo struct {
	*nlv.NodeLocalVolumeRepository
}

func (r *NoReflinkNLVRepo) ReflinkRawImageToInstantVerifyImage() error {
	// physically copy the file instead of reflink
	raw, err := os.Open(r.GetRawImagePath())
	if err != nil {
		return err
	}
	defer func() { _ = raw.Close() }()

	verify, err := os.Create(r.GetInstantVerifyImagePath())
	if err != nil {
		return err
	}
	defer func() { _ = verify.Close() }()

	_, err = io.Copy(verify, raw)
	if err != nil {
		return err
	}

	return nil
}

func TestMain(m *testing.M) {
	_ = os.Setenv("FIN_RAW_IMG_EXPANSION_UNIT_SIZE", "4096")
	m.Run()
}

type setupOutput struct {
	fullBackupInput, incrementalBackupInput *input.Backup
	k8sClient                               kubernetes.Interface
	finRepo                                 model.FinRepository
	nlvRepo                                 *nlv.NodeLocalVolumeRepository
	rbdRepo                                 *fake.RBDRepository2
	fullSnapshot, incrementalSnapshot       *model.RBDSnapshot
	fullVolume, incrementalVolume           []byte
}

func setup(t *testing.T) *setupOutput {
	k8sClient, _, volumeInfo := fake.NewStorage()
	pvc, err := k8sClient.CoreV1().PersistentVolumeClaims(volumeInfo.Namespace).
		Get(t.Context(), volumeInfo.PVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Nil(t, pvc.Spec.VolumeMode)

	rbdRepo := fake.NewRBDRepository2(volumeInfo.PoolName, volumeInfo.ImageName)
	nlvRepo, finRepo, _ := testutil.CreateNLVAndFinRepoForTest(t)

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
		rbdRepo:                rbdRepo,
		fullSnapshot:           fullSnapshot,
		incrementalSnapshot:    incrementalSnapshot,
		fullVolume:             fullVolume,
		incrementalVolume:      incrementalVolume,
	}
}

func TestVerification_Success_FullBackup(t *testing.T) {
	// Arrange
	cfg := setup(t)

	backup := backup.NewBackup(cfg.fullBackupInput)
	err := backup.Perform()
	require.NoError(t, err)

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          &NoReflinkNLVRepo{cfg.nlvRepo},
		FsckRepo:         fake.NewFsckRepository(cfg.fullVolume),
		ActionUID:        cfg.fullBackupInput.ActionUID,
		TargetSnapshotID: cfg.fullBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.fullBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	require.NoError(t, err)

	_, err = cfg.finRepo.GetActionPrivateData(cfg.fullBackupInput.ActionUID)
	require.ErrorIs(t, err, model.ErrNotFound)

	_, err = os.Stat(cfg.nlvRepo.GetInstantVerifyImagePath())
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestVerification_Success_IncrementalBackup(t *testing.T) {
	// Arrange
	cfg := setup(t)

	fullBackup := backup.NewBackup(cfg.fullBackupInput)
	err := fullBackup.Perform()
	require.NoError(t, err)
	incrementalBackup := backup.NewBackup(cfg.incrementalBackupInput)
	err = incrementalBackup.Perform()
	require.NoError(t, err)

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          &NoReflinkNLVRepo{cfg.nlvRepo},
		FsckRepo:         fake.NewFsckRepository(cfg.incrementalVolume),
		ActionUID:        cfg.incrementalBackupInput.ActionUID,
		TargetSnapshotID: cfg.incrementalBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.incrementalBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	require.NoError(t, err)

	_, err = cfg.finRepo.GetActionPrivateData(cfg.incrementalBackupInput.ActionUID)
	require.ErrorIs(t, err, model.ErrNotFound)

	_, err = os.Stat(cfg.nlvRepo.GetInstantVerifyImagePath())
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestVerification_Error_FsckFailure(t *testing.T) {
	// Arrange
	cfg := setup(t)

	backup := backup.NewBackup(cfg.fullBackupInput)
	err := backup.Perform()
	require.NoError(t, err)

	// Act
	verification := NewVerification(&input.Verification{
		Repo:             cfg.finRepo,
		RBDRepo:          cfg.rbdRepo,
		NLVRepo:          &NoReflinkNLVRepo{cfg.nlvRepo},
		FsckRepo:         fake.NewFsckRepository([]byte{}), // wrong expected data
		ActionUID:        cfg.fullBackupInput.ActionUID,
		TargetSnapshotID: cfg.fullBackupInput.TargetSnapshotID,
		TargetPVCUID:     cfg.fullBackupInput.TargetPVCUID,
	})
	err = verification.Perform()

	// Assert
	require.ErrorIs(t, err, ErrFsckFailed)
}

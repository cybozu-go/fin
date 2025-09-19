package testutil

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/ceph"
	"github.com/cybozu-go/fin/internal/infrastructure/db"
	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const SnapshotTimeFormat = "Mon Jan  2 15:04:05 2006"

// NewBackupInput creates a BackupInput for testing using a KubernetesRepository and a fake.VolumeInfo.
func NewBackupInput(k8sRepo model.KubernetesRepository, volume *fake.VolumeInfo,
	targetSnapID int, sourceSnapID *int, maxPartSize uint64) *input.Backup {
	pvc, err := k8sRepo.GetPVC(volume.PVCName, volume.Namespace)
	if err != nil {
		panic(fmt.Sprintf("failed to get PVC: %v", err))
	}
	pv, err := k8sRepo.GetPV(volume.PVName)
	if err != nil {
		panic(fmt.Sprintf("failed to get PV: %v", err))
	}

	return &input.Backup{
		ActionUID:                 uuid.New().String(),
		TargetRBDPoolName:         pv.Spec.CSI.VolumeAttributes["pool"],
		TargetRBDImageName:        pv.Spec.CSI.VolumeAttributes["imageName"],
		TargetSnapshotID:          targetSnapID,
		SourceCandidateSnapshotID: sourceSnapID,
		TargetPVCName:             pvc.Name,
		TargetPVCNamespace:        pvc.Namespace,
		TargetPVCUID:              string(pvc.UID),
		MaxPartSize:               maxPartSize,
	}
}

func NewRestoreInputTemplate(bi *input.Backup,
	rVol model.RestoreVolume, chunkSize uint64, snapID int) *input.Restore {
	return &input.Restore{
		Repo: bi.Repo,
		// Restore module uses only ApplyDiffToBlockDevice, so fake is not needed here.
		RBDRepo:             ceph.NewRBDRepository(),
		NodeLocalVolumeRepo: bi.NodeLocalVolumeRepo,
		RestoreVol:          rVol,
		RawImageChunkSize:   chunkSize,
		TargetSnapshotID:    snapID,
		ActionUID:           bi.ActionUID,
		TargetPVCUID:        bi.TargetPVCUID,
	}
}

func FillFileRandomData(t *testing.T, path string, size uint64) []byte {
	t.Helper()
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	raw, err := os.Create(path)
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()
	_, err = raw.Write(buf)
	require.NoError(t, err)
	return buf
}

func AssertActionPrivateDataIsEmpty(t *testing.T, finRepo model.FinRepository, actionUID string) {
	t.Helper()
	_, err := finRepo.GetActionPrivateData(actionUID)
	assert.ErrorIs(t, err, model.ErrNotFound)
}

func CreateNLVAndFinRepoForTest(t *testing.T) (*nlv.NodeLocalVolumeRepository, model.FinRepository, string) {
	t.Helper()

	tempDir := t.TempDir()
	nlvRepo, err := nlv.NewNodeLocalVolumeRepository(tempDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nlvRepo.Close() })
	repo, err := db.New(nlvRepo.GetDBPath())
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close(); _ = os.Remove(nlvRepo.GetDBPath()) })

	return nlvRepo, repo, tempDir
}

func CreateLoopDevice(t *testing.T, size uint64) string {
	t.Helper()

	loopbackFile, err := os.CreateTemp("", "fake-restore-*.img")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.Remove(loopbackFile.Name())
	})
	defer func() { _ = loopbackFile.Close() }()

	err = loopbackFile.Truncate(int64(size))
	require.NoError(t, err)

	loopbackFilePath := loopbackFile.Name()
	_, err = executeSudo("losetup", "-f", loopbackFilePath)
	require.NoError(t, err, "failed to create loop device")
	// get loop device path
	stdout, err := executeSudo("losetup", "-j", loopbackFilePath)
	require.NoError(t, err, "failed to get loop device path")
	devicePath := strings.Split(string(stdout), ":")[0]
	require.NotEmpty(t, devicePath, "loop device path should not be empty")
	t.Cleanup(func() {
		_, err = executeSudo("losetup", "-d", devicePath)
		require.NoError(t, err, "failed to delete loop device")
	})

	_, err = executeSudo("chmod", "666", devicePath)
	require.NoError(t, err, "failed to change permissions of loop device")
	t.Cleanup(func() {
		_, err := executeSudo("chmod", "660", devicePath)
		require.NoError(t, err, "failed to change permissions of loop device back")
	})

	return devicePath
}

func CreateFakeRawImgFileForTest(t *testing.T, nlvRepo model.NodeLocalVolumeRepository, size uint64) string {
	t.Helper()

	buf := make([]byte, size)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	f, err := os.CreateTemp("", "fake-restore-*.img")
	require.NoError(t, err)
	filePath := f.Name()
	defer func() { _ = f.Close() }()
	t.Cleanup(func() { _ = os.Remove(filePath) })
	_, err = f.Write(buf)
	require.NoError(t, err)
	return filePath
}

func executeSudo(args ...string) ([]byte, error) {
	cmd := exec.Command("sudo", args...)

	var stdoutBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf

	err := cmd.Run()
	return stdoutBuf.Bytes(), err
}

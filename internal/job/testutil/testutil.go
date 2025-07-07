package testutil

import (
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/infrastructure/sqlite"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/restore"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewBackupInputTemplate(snapID, maxPartSize int) *backup.BackupInput {
	return &backup.BackupInput{
		RetryInterval:             1 * time.Second,
		ProcessUID:                uuid.New().String(),
		TargetFinBackupUID:        uuid.New().String(),
		TargetRBDPoolName:         "test-pool",
		TargetRBDImageName:        "test-image",
		TargetSnapshotID:          snapID,
		SourceCandidateSnapshotID: nil,
		TargetPVCName:             "test-pvc",
		TargetPVCNamespace:        "test-namespace",
		TargetPVCUID:              uuid.New().String(),
		MaxPartSize:               maxPartSize,
	}
}

func NewIncrementalBackupInputTemplate(src *backup.BackupInput, snapID int) *backup.BackupInput {
	ret := *src
	parentSnapID := ret.TargetSnapshotID
	ret.TargetSnapshotID = snapID
	ret.SourceCandidateSnapshotID = &parentSnapID
	ret.ProcessUID = uuid.New().String()
	ret.TargetFinBackupUID = uuid.New().String()
	return &ret
}

func NewRestoreInputTemplate(bi *backup.BackupInput,
	rVol model.RestoreVolume, chunkSize, snapID int) *restore.RestoreInput {
	return &restore.RestoreInput{
		Repo:                bi.Repo,
		KubernetesRepo:      bi.KubernetesRepo,
		NodeLocalVolumeRepo: bi.NodeLocalVolumeRepo,
		RestoreVol:          rVol,
		RawImageChunkSize:   int64(chunkSize),
		TargetSnapshotID:    snapID,
		RetryInterval:       bi.RetryInterval,
		ProcessUID:          bi.ProcessUID,
		TargetPVCUID:        bi.TargetPVCUID,
	}
}

func FillRawImageWithRandomData(t *testing.T, rawImagePath string, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	raw, err := os.Create(rawImagePath)
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()
	_, err = raw.Write(buf)
	require.NoError(t, err)
	return buf
}

func AssertActionPrivateDataIsEmpty(t *testing.T, finRepo model.FinRepository, processUID string) {
	t.Helper()
	_, err := finRepo.GetActionPrivateData(processUID)
	assert.ErrorIs(t, err, model.ErrNotFound)
}

func CreateNLVAndFinRepoForTest(t *testing.T) (*nlv.NodeLocalVolumeRepository, model.FinRepository) {
	t.Helper()

	tempDir := t.TempDir()
	nlvRepo, err := nlv.NewNodeLocalVolumeRepository(tempDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nlvRepo.Close() })
	require.NoError(t, err)
	repo, err := sqlite.New(nlvRepo.GetDBPath())
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	return nlvRepo, repo
}

func CreateRestoreFileForTest(t *testing.T, size int64) string {
	t.Helper()

	restoreFile, err := os.CreateTemp("", "fake-restore-*.img")
	require.NoError(t, err)
	defer func() { _ = restoreFile.Close() }()
	restorePath := restoreFile.Name()
	t.Cleanup(func() { _ = os.Remove(restorePath) })
	err = restoreFile.Truncate(size)
	require.NoError(t, err)
	return restorePath
}

func CreateFakeRawImgFileForTest(t *testing.T, nlvRepo model.NodeLocalVolumeRepository, size int64) string {
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

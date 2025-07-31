package testutil

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/db"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

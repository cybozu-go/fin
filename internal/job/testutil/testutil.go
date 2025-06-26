package testutil

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func AssertActionPrivateDataIsEmpty(t *testing.T, finRepo model.FinRepository, processUID string) {
	t.Helper()
	_, err := finRepo.GetActionPrivateData(processUID)
	assert.ErrorIs(t, err, model.ErrNotFound)
}

func AssertDiffDirDoesNotExist(t *testing.T, nlvRepo model.NodeLocalVolumeRepository, diffDir string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(nlvRepo.GetRootPath(), diffDir))
	assert.True(t, os.IsNotExist(err))
}

func GetDiffDirPath(snapshotID int) string {
	return filepath.Join("diff", fmt.Sprintf("%d", snapshotID))
}

func GetRawImagePath() string {
	return "raw.img"
}

func GetFinSqlite3DSN(rootPath string) string {
	return fmt.Sprintf("file:%s?_txlock=exclusive", filepath.Join(rootPath, "fin.sqlite3"))
}

func CreateNLVForTest(t *testing.T) *nlv.NodeLocalVolumeRepository {
	t.Helper()

	rootPath, err := os.MkdirTemp("", "fin-fake-nlv")
	require.NoError(t, err)

	repo := nlv.NewNodeLocalVolumeRepository(rootPath)
	t.Cleanup(repo.Cleanup)
	return repo
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

package testutil

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/infrastructure/sqlite"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func AssertActionPrivateDataIsEmpty(t *testing.T, finRepo model.FinRepository, processUID string) {
	t.Helper()
	_, err := finRepo.GetActionPrivateData(processUID)
	assert.ErrorIs(t, err, model.ErrNotFound)
}

func CreateNLVAndFinRepoForTest(t *testing.T) (*nlv.NodeLocalVolumeRepository, model.FinRepository) {
	t.Helper()

	rootPath, err := os.MkdirTemp("", "fin-fake-nlv")
	require.NoError(t, err)
	nlv := nlv.NewNodeLocalVolumeRepository(rootPath)
	t.Cleanup(nlv.Cleanup)
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_txlock=exclusive", nlv.GetDBPath()))
	t.Cleanup(func() { _ = db.Close })
	require.NoError(t, err)
	repo, err := sqlite.New(db)
	require.NoError(t, err)

	return nlv, repo
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

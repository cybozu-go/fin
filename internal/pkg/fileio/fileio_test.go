package fileio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0644))

	require.NoError(t, SyncData(path))
}

func TestSyncData_MissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")

	err := SyncData(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorContains(t, err, path)
}

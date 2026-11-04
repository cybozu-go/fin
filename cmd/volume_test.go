package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenVolume_CreatesVolumeAndDB(t *testing.T) {
	root := filepath.Join(t.TempDir(), "ns", "pvc")

	vol, err := openVolume(root)
	require.NoError(t, err)
	t.Cleanup(vol.Close)

	assert.DirExists(t, root)
	assert.FileExists(t, vol.nlvRepo.GetDBPath())
	require.NoError(t, vol.finRepo.StartOrRestartAction("uid", "backup"))
}

func TestOpenVolume_RootIsNotDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(root, nil, 0o644))

	vol, err := openVolume(root)
	require.Error(t, err)
	assert.Nil(t, vol)
	assert.ErrorContains(t, err, "failed to create NodeLocalVolumeRepository")
}

func TestOpenVolume_DBInitFailure(t *testing.T) {
	root := t.TempDir()
	// A database file that is not a database makes db.New fail after the volume is opened.
	require.NoError(t, os.WriteFile(filepath.Join(root, "fin.sqlite3"), []byte("not a sqlite database, long enough to be read"), 0o644))

	vol, err := openVolume(root)
	require.Error(t, err)
	assert.Nil(t, vol)
	assert.ErrorContains(t, err, "failed to create repository")
}

func TestPerformUntilLocked(t *testing.T) {
	retryInterval = time.Millisecond
	t.Cleanup(func() { retryInterval = job.RetryInterval })

	t.Run("retries only while the lock is not available", func(t *testing.T) {
		calls := 0
		err := performUntilLocked(func() error {
			calls++
			if calls < 3 {
				return job.ErrCantLock
			}
			return nil
		}, "done", "failed")
		require.NoError(t, err)
		assert.Equal(t, 3, calls)
	})

	t.Run("returns other errors without retrying", func(t *testing.T) {
		boom := errors.New("boom")
		calls := 0
		err := performUntilLocked(func() error {
			calls++
			return boom
		}, "done", "failed to perform x")
		require.ErrorIs(t, err, boom)
		assert.EqualError(t, err, "failed to perform x: boom")
		assert.Equal(t, 1, calls)
	})
}

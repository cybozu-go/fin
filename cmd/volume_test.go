package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
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

type openResult struct {
	vol *volume
	err error
}

func openVolumeAsync(root string) <-chan openResult {
	ch := make(chan openResult, 1)
	go func() {
		vol, err := openVolume(root)
		ch <- openResult{vol: vol, err: err}
	}()
	return ch
}

func requireStillWaiting(t *testing.T, ch <-chan openResult) {
	t.Helper()
	require.Never(t, func() bool {
		select {
		case r := <-ch:
			if r.vol != nil {
				r.vol.Close()
			}
			return true
		default:
			return false
		}
	}, 300*time.Millisecond, 10*time.Millisecond, "openVolume must wait while another opener holds the volume")
}

func requireOpened(t *testing.T, ch <-chan openResult) *volume {
	t.Helper()
	select {
	case r := <-ch:
		require.NoError(t, r.err)
		return r.vol
	case <-time.After(10 * time.Second):
		require.FailNow(t, "openVolume did not proceed after the volume was released")
		return nil
	}
}

func setFastRetry(t *testing.T) {
	t.Helper()
	retryInterval = 10 * time.Millisecond
	t.Cleanup(func() { retryInterval = job.RetryInterval })
}

func TestOpenVolume_ExcludesAnotherOpenerWhileHeld(t *testing.T) {
	setFastRetry(t)
	root := t.TempDir()

	first, err := openVolume(root)
	require.NoError(t, err)

	// The wait does not depend on the action UID: the second opener has none yet,
	// so a same-UID restart and a different-UID job are held back alike.
	second := openVolumeAsync(root)
	requireStillWaiting(t, second)

	first.Close()
	requireOpened(t, second).Close()
}

func TestOpenVolume_CompleteActionDoesNotReleaseTheVolume(t *testing.T) {
	setFastRetry(t)
	root := t.TempDir()

	first, err := openVolume(root)
	require.NoError(t, err)
	require.NoError(t, first.finRepo.StartOrRestartAction("uid", "backup"))
	require.NoError(t, first.finRepo.CompleteAction("uid"))

	second := openVolumeAsync(root)
	requireStillWaiting(t, second)

	first.Close()
	requireOpened(t, second).Close()
}

func TestOpenVolume_DBInitFailureReleasesTheVolume(t *testing.T) {
	setFastRetry(t)
	root := t.TempDir()
	dbPath := filepath.Join(root, "fin.sqlite3")
	require.NoError(t, os.WriteFile(dbPath, []byte("not a sqlite database, long enough to be read"), 0o644))

	_, err := openVolume(root)
	require.Error(t, err)

	require.NoError(t, os.Remove(dbPath))
	requireOpened(t, openVolumeAsync(root)).Close()
}

// TestHelperHoldVolume is not a test. It is the child process of
// TestOpenVolume_KilledHolderReleasesTheVolume.
func TestHelperHoldVolume(t *testing.T) {
	root := os.Getenv("FIN_TEST_HOLD_VOLUME")
	if root == "" {
		t.Skip("helper process only")
	}
	vol, err := openVolume(root)
	require.NoError(t, err)
	defer vol.Close()
	fmt.Println("held")
	time.Sleep(time.Hour)
}

func TestOpenVolume_KilledHolderReleasesTheVolume(t *testing.T) {
	setFastRetry(t)
	root := t.TempDir()

	child := exec.Command(os.Args[0], "-test.run=TestHelperHoldVolume")
	child.Env = append(os.Environ(), "FIN_TEST_HOLD_VOLUME="+root)
	stdout, err := child.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, child.Start())
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })

	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "held\n", line)

	waiter := openVolumeAsync(root)
	requireStillWaiting(t, waiter)

	require.NoError(t, child.Process.Signal(syscall.SIGKILL))
	requireOpened(t, waiter).Close()
}

func TestRunWithVolume(t *testing.T) {
	retryInterval = time.Millisecond
	t.Cleanup(func() { retryInterval = job.RetryInterval })

	t.Run("retries only while the lock is not available", func(t *testing.T) {
		root := t.TempDir()
		calls := 0
		err := runWithVolume(root, func(*volume) error {
			calls++
			if calls < 3 {
				return job.ErrCantLock
			}
			return nil
		}, "done", "failed")
		require.NoError(t, err)
		assert.Equal(t, 3, calls)
	})

	t.Run("opens the volume again for each attempt", func(t *testing.T) {
		root := t.TempDir()
		var seen []*volume
		err := runWithVolume(root, func(vol *volume) error {
			seen = append(seen, vol)
			if len(seen) < 2 {
				return job.ErrCantLock
			}
			return nil
		}, "done", "failed")
		require.NoError(t, err)
		require.Len(t, seen, 2)
		assert.NotSame(t, seen[0], seen[1])
	})

	t.Run("does not hold the volume while waiting to retry", func(t *testing.T) {
		// Another job's action_status row makes perform report ErrCantLock. That
		// job can only finish if the volume is free, so holding it across the
		// wait would leave both stuck.
		retryInterval = 500 * time.Millisecond
		t.Cleanup(func() { retryInterval = time.Millisecond })
		root := t.TempDir()

		inPerform := make(chan struct{})
		acquired := make(chan error, 1)
		go func() {
			<-inPerform
			repo, err := nlv.NewNodeLocalVolumeRepository(root)
			if err != nil {
				acquired <- err
				return
			}
			defer func() { _ = repo.Close() }()
			deadline := time.Now().Add(300 * time.Millisecond)
			for {
				unlock, err := repo.TryLock()
				if err == nil {
					acquired <- unlock()
					return
				}
				if time.Now().After(deadline) {
					acquired <- err
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()

		calls := 0
		err := runWithVolume(root, func(*volume) error {
			calls++
			if calls == 1 {
				close(inPerform)
				return job.ErrCantLock
			}
			return nil
		}, "done", "failed")
		require.NoError(t, err)
		require.NoError(t, <-acquired, "the volume must be free while the retry waits")
	})

	t.Run("returns other errors without retrying", func(t *testing.T) {
		root := t.TempDir()
		boom := errors.New("boom")
		calls := 0
		err := runWithVolume(root, func(*volume) error {
			calls++
			return boom
		}, "done", "failed to perform x")
		require.ErrorIs(t, err, boom)
		assert.EqualError(t, err, "failed to perform x: boom")
		assert.Equal(t, 1, calls)
	})

	t.Run("returns an open error without calling perform", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(root, nil, 0o644))
		err := runWithVolume(root, func(*volume) error {
			t.Fatal("perform must not be called")
			return nil
		}, "done", "failed")
		assert.ErrorContains(t, err, "failed to create NodeLocalVolumeRepository")
	})
}

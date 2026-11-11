package nlv

import (
	"errors"
	"fmt"
	"os"

	"github.com/cybozu-go/fin/internal/model"
	"golang.org/x/sys/unix"
)

const lockFilePath = "fin.lock"

// TryLock takes an exclusive lock on the volume without blocking and returns
// a function that releases it. If another holder has it, it returns
// model.ErrBusy.
//
// The lock is a flock(2) on a file in the volume. It is tied to the open file,
// so the kernel releases it when the holder exits, including by SIGKILL. That is
// why it is used for who may touch the volume instead of the action_status row,
// which stays behind when a holder dies and is deleted before the holder exits.
// flock(2) only excludes holders on the same node, which is the only place the
// node-local volume can be mounted. The lock file is never removed, because
// removing it would let two holders lock different files at the same path.
func (r *NodeLocalVolumeRepository) TryLock() (unlock func() error, err error) {
	f, err := r.root.OpenFile(lockFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file %s: %w", lockFilePath, err)
	}

	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, model.ErrBusy
		}
		return nil, fmt.Errorf("failed to lock %s: %w", lockFilePath, err)
	}
	return f.Close, nil
}

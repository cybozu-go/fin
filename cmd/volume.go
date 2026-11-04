package cmd

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/db"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/model"
)

var retryInterval = job.RetryInterval

// volume is the node-local volume of a PVC and the Fin database stored in it.
// It is held exclusively from openVolume until Close.
type volume struct {
	nlvRepo *nlv.NodeLocalVolumeRepository
	finRepo *db.FinRepository
	unlock  func() error
}

// openVolume waits until no other job has the volume, then opens it.
//
// The exclusion is taken before the database is initialized and kept until
// Close, i.e. until the job's files are no longer touched. The action_status
// row cannot do this: it lets a restart with the same UID through, and it is
// deleted by CompleteAction while the job is still running.
func openVolume(rootPath string) (*volume, error) {
	nlvRepo, err := nlv.NewNodeLocalVolumeRepository(rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create NodeLocalVolumeRepository: %w", err)
	}

	unlock, err := lockVolume(nlvRepo)
	if err != nil {
		_ = nlvRepo.Close()
		return nil, err
	}

	finRepo, err := db.New(nlvRepo.GetDBPath())
	if err != nil {
		_ = unlock()
		_ = nlvRepo.Close()
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}

	return &volume{nlvRepo: nlvRepo, finRepo: finRepo, unlock: unlock}, nil
}

func lockVolume(nlvRepo *nlv.NodeLocalVolumeRepository) (func() error, error) {
	for {
		unlock, err := nlvRepo.TryLock()
		if err == nil {
			return unlock, nil
		}
		if !errors.Is(err, model.ErrBusy) {
			return nil, fmt.Errorf("failed to lock the volume: %w", err)
		}
		slog.Warn("the volume is in use by another job, will retry.")
		time.Sleep(retryInterval)
	}
}

func (v *volume) Close() {
	_ = v.finRepo.Close()
	_ = v.unlock()
	_ = v.nlvRepo.Close()
}

// performUntilLocked calls perform until it stops reporting job.ErrCantLock.
func performUntilLocked(perform func() error, completedMessage, failureContext string) error {
	for {
		err := perform()
		if err == nil {
			slog.Info(completedMessage)
			return nil
		}
		if !errors.Is(err, job.ErrCantLock) {
			return fmt.Errorf("%s: %w", failureContext, err)
		}
		slog.Warn("failed to acquire lock, will retry.")
		time.Sleep(retryInterval)
	}
}

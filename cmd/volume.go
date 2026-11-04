package cmd

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/db"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job"
)

var retryInterval = job.RetryInterval

// volume is the node-local volume of a PVC and the Fin database stored in it.
type volume struct {
	nlvRepo *nlv.NodeLocalVolumeRepository
	finRepo *db.FinRepository
}

func openVolume(rootPath string) (*volume, error) {
	nlvRepo, err := nlv.NewNodeLocalVolumeRepository(rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create NodeLocalVolumeRepository: %w", err)
	}

	finRepo, err := db.New(nlvRepo.GetDBPath())
	if err != nil {
		_ = nlvRepo.Close()
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}

	return &volume{nlvRepo: nlvRepo, finRepo: finRepo}, nil
}

func (v *volume) Close() {
	_ = v.finRepo.Close()
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

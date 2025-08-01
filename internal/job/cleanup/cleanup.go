package cleanup

import (
	"errors"
	"fmt"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/model"
)

type Cleanup struct {
	repo                model.FinRepository
	nodeLocalVolumeRepo model.NodeLocalVolumeRepository
	actionUID           string
	targetSnapshotID    int
	targetPVCUID        string
}

func NewCleanup(in *input.Cleanup) *Cleanup {
	return &Cleanup{
		repo:                in.Repo,
		nodeLocalVolumeRepo: in.NodeLocalVolumeRepo,
		actionUID:           in.ActionUID,
		targetSnapshotID:    in.TargetSnapshotID,
		targetPVCUID:        in.TargetPVCUID,
	}
}

// Perform executes the cleanup process. If it can't get lock, it returns ErrCantLock.
func (b *Cleanup) Perform() error {
	err := b.repo.StartOrRestartAction(b.actionUID, model.Backup)
	if err != nil {
		if errors.Is(err, model.ErrBusy) {
			return job.ErrCantLock
		}
		return fmt.Errorf("failed to start or restart action: %w", err)
	}
	if err := b.doCleanup(); err != nil {
		return fmt.Errorf("failed to perform cleanup: %w", err)
	}
	if err := b.repo.CompleteAction(b.actionUID); err != nil {
		return fmt.Errorf("failed to complete action: %w", err)
	}
	return nil
}

func (b *Cleanup) doCleanup() error {
	metadata, err := job.GetBackupMetadata(b.repo)
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			if err := b.nodeLocalVolumeRepo.RemoveOngoingFullBackupFiles(b.targetSnapshotID); err != nil {
				return fmt.Errorf("failed to remove ongoing full backup files: %w", err)
			}
			return nil
		}
		return err
	}

	if b.targetPVCUID != metadata.PVCUID {
		return fmt.Errorf("target PVC UID (%s) does not match the expected one (%s)",
			b.targetPVCUID, metadata.PVCUID)
	}

	raw := metadata.Raw
	var diff *job.BackupMetadataEntry
	if len(metadata.Diff) > 0 {
		diff = metadata.Diff[0]
	}

	if raw == nil {
		if diff == nil {
			return fmt.Errorf("something wrong happened. If raw is nil, diff must not be nil")
		}
		if err := b.nodeLocalVolumeRepo.RemoveOngoingFullBackupFiles(b.targetSnapshotID); err != nil {
			return fmt.Errorf("failed to remove ongoing full backup files: %w", err)
		}

		if err := b.repo.DeleteBackupMetadata(); err != nil {
			return fmt.Errorf("failed to delete backup metadata: %w", err)
		}

		return nil
	}

	if diff == nil {
		err := b.nodeLocalVolumeRepo.RemoveDiffDirRecursively(b.targetSnapshotID)
		if err != nil && !errors.Is(err, model.ErrNotFound) {
			return fmt.Errorf("failed to remove diff directory of snapid %d: %w",
				b.targetSnapshotID, err)
		}
	}

	return nil
}

package deletion

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/model"
)

// ErrInvalidSnapshotOrder is returned when raw.snapID is smaller than target snapshot ID
var ErrInvalidSnapshotOrder = errors.New("raw.snapID is smaller than target snapshot ID")

type Deletion struct {
	repo                model.FinRepository
	rbdRepo             model.RBDRepository
	nodeLocalVolumeRepo model.NodeLocalVolumeRepository
	actionUID           string
	targetSnapshotID    int
	targetPVCUID        string
}

func NewDeletion(in *input.Deletion) *Deletion {
	return &Deletion{
		repo:                in.Repo,
		rbdRepo:             in.RBDRepo,
		nodeLocalVolumeRepo: in.NodeLocalVolumeRepo,
		actionUID:           in.ActionUID,
		targetSnapshotID:    in.TargetSnapshotID,
		targetPVCUID:        in.TargetPVCUID,
	}
}

// Perform executes the delete process. If it can't get lock, it returns ErrCantLock.
func (d *Deletion) Perform() error {
	err := d.repo.StartOrRestartAction(d.actionUID, model.Deletion)
	if err != nil {
		if errors.Is(err, model.ErrBusy) {
			return job.ErrCantLock
		}
		return fmt.Errorf("failed to start or restart action: %w", err)
	}

	if err := d.doDeletion(); err != nil {
		if errors.Is(err, ErrInvalidSnapshotOrder) {
			if completeErr := d.repo.CompleteAction(d.actionUID); completeErr != nil {
				err = errors.Join(err, completeErr)
			}
			return err
		}
		return fmt.Errorf("failed to perform delete: %w", err)
	}

	if err := d.repo.CompleteAction(d.actionUID); err != nil {
		return fmt.Errorf("failed to complete action: %w", err)
	}
	return nil
}

func (d *Deletion) doDeletion() error {
	metadata, err := job.GetBackupMetadata(d.repo)
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("failed to get backup metadata: %w", err)
	}
	if metadata.PVCUID != d.targetPVCUID {
		return fmt.Errorf("target PVC UID %s does not match the expected %s", d.targetPVCUID, metadata.PVCUID)
	}
	if metadata.Raw == nil {
		return fmt.Errorf("invalid state: raw is empty")
	}

	if metadata.Raw.SnapID > d.targetSnapshotID {
		return nil
	}
	if metadata.Raw.SnapID < d.targetSnapshotID {
		return ErrInvalidSnapshotOrder
	}

	if len(metadata.Diff) == 0 {
		err := d.nodeLocalVolumeRepo.RemoveRawImage()
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("failed to delete raw image: %w", err)
		}
		if err := d.repo.DeleteBackupMetadata(); err != nil {
			return fmt.Errorf("failed to delete backup metadata: %w", err)
		}
		return nil
	}

	if err := d.applyDiffToRawImage(metadata.Raw, metadata.Diff[0]); err != nil {
		return fmt.Errorf("failed to apply diff to the raw image: %w", err)
	}
	metadata.Raw = metadata.Diff[0]
	metadata.Diff = []*job.BackupMetadataEntry{}

	// TODO(skoya76): Set annotation to the oldest FinBackup here.

	return job.SetBackupMetadata(d.repo, metadata)
}

func (d *Deletion) applyDiffToRawImage(raw, diff *job.BackupMetadataEntry) error {
	if err := d.applyAllDiffParts(raw, diff); err != nil {
		return fmt.Errorf("failed to apply diff parts: %w", err)
	}
	err := d.nodeLocalVolumeRepo.RemoveDiffDirRecursively(diff.SnapID)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to remove dir: %w", err)
	}
	return nil
}

func (d *Deletion) applyAllDiffParts(raw, diff *job.BackupMetadataEntry) error {
	privateData, err := getDeletePrivateData(d.repo, d.actionUID)
	if err != nil {
		return fmt.Errorf("failed to get private data: %w", err)
	}

	partCount := int(math.Ceil(float64(diff.SnapSize) / float64(diff.PartSize)))
	for i := privateData.NextPatchPart; i < partCount; i++ {
		sourceSnapshotName, targetSnapshotName :=
			job.CalcSnapshotNamesWithOffset(raw.SnapName, diff.SnapName, i, partCount, diff.PartSize)
		diffPartPath := d.nodeLocalVolumeRepo.GetDiffPartPath(diff.SnapID, i)
		if err := d.rbdRepo.ApplyDiffToRawImage(
			d.nodeLocalVolumeRepo.GetRawImagePath(),
			diffPartPath,
			sourceSnapshotName,
			targetSnapshotName,
		); err != nil {
			return fmt.Errorf("failed to apply diff part %d: %w", i, err)
		}

		// Update the next patch number for retry purposes
		privateData.NextPatchPart = i + 1
		if err := setDeletePrivateData(d.repo, d.actionUID, privateData); err != nil {
			return fmt.Errorf("failed to set nextPatchPart to %d: %w", i, err)
		}
	}
	return nil
}

type deletePrivateData struct {
	NextPatchPart int `json:"nextPatchPart,omitempty"`
}

func getDeletePrivateData(repo model.FinRepository, actionUID string) (*deletePrivateData, error) {
	privateData, err := repo.GetActionPrivateData(actionUID)
	if err != nil {
		return nil, err
	}

	if len(privateData) == 0 {
		return &deletePrivateData{}, nil
	}

	var data deletePrivateData
	if err := json.Unmarshal(privateData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal private data: %w", err)
	}
	return &data, nil
}

func setDeletePrivateData(repo model.FinRepository, actionUID string, data *deletePrivateData) error {
	privateData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal private data: %w", err)
	}
	if err := repo.UpdateActionPrivateData(actionUID, privateData); err != nil {
		return fmt.Errorf("failed to update private data: %w", err)
	}
	return nil
}

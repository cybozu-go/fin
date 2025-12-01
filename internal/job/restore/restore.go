package restore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/model"
)

type Restore struct {
	repo                  model.FinRepository
	rbdRepo               model.RBDRepository
	nodeLocalVolumeRepo   model.NodeLocalVolumeRepository
	restoreVol            model.RestoreVolume
	actionUID             string
	targetSnapshotID      int
	rawImageChunkSize     uint64
	diffChecksumChunkSize uint64
	enableChecksumVerify  bool
	targetPVCUID          string
}

func NewRestore(in *input.Restore) *Restore {
	return &Restore{
		repo:                  in.Repo,
		rbdRepo:               in.RBDRepo,
		nodeLocalVolumeRepo:   in.NodeLocalVolumeRepo,
		restoreVol:            in.RestoreVol,
		actionUID:             in.ActionUID,
		targetSnapshotID:      in.TargetSnapshotID,
		rawImageChunkSize:     in.RawImageChunkSize,
		diffChecksumChunkSize: in.DiffChecksumChunkSize,
		enableChecksumVerify:  in.EnableChecksumVerify,
		targetPVCUID:          in.TargetPVCUID,
	}
}

// Perform executes the restore process. If it can't get lock, it returns ErrCantLock.
func (r *Restore) Perform() error {
	err := r.repo.StartOrRestartAction(r.actionUID, model.Restore)
	if err != nil {
		if errors.Is(err, model.ErrBusy) {
			return job.ErrCantLock
		}
		return fmt.Errorf("failed to start or restart action: %w", err)
	}
	if err := r.doRestore(); err != nil {
		return fmt.Errorf("failed to perform restore: %w", err)
	}
	if err := r.repo.CompleteAction(r.actionUID); err != nil {
		return fmt.Errorf("failed to complete action: %w", err)
	}
	return nil
}

func (r *Restore) doRestore() error {
	metadata, err := job.GetBackupMetadata(r.repo)
	if err != nil {
		return fmt.Errorf("failed to get backup metadata: %w", err)
	}

	if metadata.PVCUID != r.targetPVCUID {
		return fmt.Errorf("PVC UID in metadata table (%s) does not match the expected one (%s)",
			metadata.PVCUID, r.targetPVCUID)
	}

	switch len(metadata.Diff) {
	case 0:
		if r.targetSnapshotID != metadata.Raw.SnapID {
			return fmt.Errorf("the target snapshot ID must be %d, but found %d",
				metadata.Raw.SnapID, r.targetSnapshotID)
		}
	case 1:
		if r.targetSnapshotID == metadata.Raw.SnapID ||
			r.targetSnapshotID == metadata.Diff[0].SnapID {
			break
		}
		return fmt.Errorf("the target snapshot ID is invalid %d", r.targetSnapshotID)
	default:
		return fmt.Errorf("the number of diffs is %d but must be <= 1", len(metadata.Diff))
	}

	d, err := getRestorePrivateData(r.repo, r.actionUID)
	if err != nil {
		return fmt.Errorf("failed to get private data: %w", err)
	}

	if err := r.doInitialPhase(d); err != nil {
		return fmt.Errorf("initial phase failed: %w", err)
	}

	if err := r.doDiscardPhase(d); err != nil {
		return fmt.Errorf("discard phase failed: %w", err)
	}

	if err := r.doRestoreRawImagePhase(d, metadata.Raw, uint64(metadata.RawChecksumChunkSize), r.enableChecksumVerify); err != nil {
		return fmt.Errorf("restore raw image phase failed: %w", err)
	}

	if err := r.doRestoreDiffPhase(d, metadata); err != nil {
		return fmt.Errorf("restore diff phase failed: %w", err)
	}

	return nil
}

func (r *Restore) doInitialPhase(privateData *restorePrivateData) error {
	if privateData.Phase != "" {
		return nil
	}
	privateData.Phase = Discard
	return setRestorePrivateData(r.repo, r.actionUID, privateData)
}

func (r *Restore) doDiscardPhase(privateData *restorePrivateData) error {
	if privateData.Phase != Discard {
		return nil
	}
	if err := r.restoreVol.ZeroOut(); err != nil {
		return fmt.Errorf("failed to zero out restore volume: %w", err)
	}
	privateData.Phase = RestoreRawImage
	return setRestorePrivateData(r.repo, r.actionUID, privateData)
}

func (r *Restore) doRestoreRawImagePhase(privateData *restorePrivateData, raw *job.BackupMetadataEntry, rawChecksumChunkSize uint64, enableChecksumVerify bool) error {
	if privateData.Phase != RestoreRawImage {
		return nil
	}
	err := r.loopCopyChunk(privateData, raw.SnapSize, rawChecksumChunkSize, enableChecksumVerify)
	if err != nil {
		return fmt.Errorf("failed to restore raw image: %w", err)
	}

	if r.targetSnapshotID == raw.SnapID {
		privateData.Phase = Completed
		return setRestorePrivateData(r.repo, r.actionUID, privateData)
	}

	privateData.Phase = RestoreDiff
	return setRestorePrivateData(r.repo, r.actionUID, privateData)
}

func (r *Restore) loopCopyChunk(privateData *restorePrivateData, rawImageSize uint64, rawChecksumChunkSize uint64, enableChecksumVerify bool) error {
	chunkCount := int(math.Ceil(float64(rawImageSize) / float64(r.rawImageChunkSize)))
	for i := privateData.NextRawImageChunk; i < chunkCount; i++ {
		rawImagePath := r.nodeLocalVolumeRepo.GetRawImagePath()
		restoreVolPath := r.restoreVol.GetPath()
		if err := r.restoreVol.CopyChunk(rawImagePath, i, r.rawImageChunkSize, rawChecksumChunkSize, enableChecksumVerify); err != nil {
			return fmt.Errorf("failed to copy chunk %d: %w", i, err)
		}

		if err := job.SyncData(restoreVolPath); err != nil {
			return fmt.Errorf("failed to sync %q: %w", restoreVolPath, err)
		}

		privateData.NextRawImageChunk = i + 1
		if err := setRestorePrivateData(r.repo, r.actionUID, privateData); err != nil {
			return fmt.Errorf("failed to set nextRawImageChunk to %d: %w",
				privateData.NextRawImageChunk, err)
		}
	}
	return nil
}

func (r *Restore) doRestoreDiffPhase(privateData *restorePrivateData, metadata *job.BackupMetadata) error {
	if privateData.Phase != RestoreDiff {
		return nil
	}

	switch len(metadata.Diff) {
	case 0:
		// No diffs to apply, just return.
	case 1:
		err := r.loopApplyDiff(privateData, metadata.Raw, metadata.Diff[0])
		if err != nil {
			return fmt.Errorf("failed to apply diffs: %w", err)
		}
	default:
		return errors.New("multiple diffs are not currently supported")
	}

	privateData.Phase = Completed
	return setRestorePrivateData(r.repo, r.actionUID, privateData)
}

func (r *Restore) loopApplyDiff(
	privateData *restorePrivateData,
	source *job.BackupMetadataEntry,
	target *job.BackupMetadataEntry) error {
	partCount := int(math.Ceil(float64(target.SnapSize) / float64(target.PartSize)))
	for i := privateData.NextDiffPart; i < partCount; i++ {
		sourceSnapshotName, targetSnapshotName :=
			job.CalcSnapshotNamesWithOffset(source.SnapName, target.SnapName, i, partCount, target.PartSize)
		diffPartPath := r.nodeLocalVolumeRepo.GetDiffPartPath(target.SnapID, i)
		restoreVolPath := r.restoreVol.GetPath()
		if err := r.rbdRepo.ApplyDiffToBlockDevice(
			restoreVolPath,
			diffPartPath,
			sourceSnapshotName,
			targetSnapshotName,
			r.diffChecksumChunkSize,
		); err != nil {
			return fmt.Errorf("failed to apply diff part %d: %w", i, err)
		}

		if err := job.SyncData(restoreVolPath); err != nil {
			return fmt.Errorf("failed to sync %q: %w", restoreVolPath, err)
		}

		privateData.NextDiffPart = i + 1
		if err := setRestorePrivateData(r.repo, r.actionUID, privateData); err != nil {
			return fmt.Errorf("failed to set nextDiffPart to %d: %w", privateData.NextDiffPart, err)
		}
	}
	return nil
}

const (
	Discard         = "discard"
	RestoreRawImage = "restore_raw_image"
	RestoreDiff     = "restore_diff"
	Completed       = "completed"
)

type restorePrivateData struct {
	NextRawImageChunk int    `json:"nextRawImageChunk,omitempty"`
	NextDiffPart      int    `json:"nextDiffPart,omitempty"`
	Phase             string `json:"phase,omitempty"`
}

func getRestorePrivateData(repo model.FinRepository, actionUID string) (*restorePrivateData, error) {
	privateData, err := repo.GetActionPrivateData(actionUID)
	if err != nil {
		return nil, err
	}

	if len(privateData) == 0 {
		return &restorePrivateData{}, nil
	}

	var data restorePrivateData
	if err := json.Unmarshal(privateData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal private data: %w", err)
	}
	return &data, nil
}

func setRestorePrivateData(repo model.FinRepository, actionUID string, data *restorePrivateData) error {
	privateData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal private data: %w", err)
	}
	if err := repo.UpdateActionPrivateData(actionUID, privateData); err != nil {
		return fmt.Errorf("failed to update private data: %w", err)
	}
	return nil
}

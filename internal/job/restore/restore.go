package restore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/model"
)

type Restore struct {
	repo                model.FinRepository
	kubernetesRepo      model.KubernetesRepository
	nodeLocalVolumeRepo model.NodeLocalVolumeRepository
	restoreVol          model.RestoreVolume
	retryInterval       time.Duration
	processUID          string
	targetSnapshotID    int
	rawImageChunkSize   int64
	targetPVCUID        string
}

type RestoreInput struct {
	Repo                model.FinRepository
	KubernetesRepo      model.KubernetesRepository
	NodeLocalVolumeRepo model.NodeLocalVolumeRepository
	RestoreVol          model.RestoreVolume
	RetryInterval       time.Duration
	ProcessUID          string
	TargetSnapshotID    int
	RawImageChunkSize   int64
	TargetPVCUID        string
}

func NewRestore(in *RestoreInput) *Restore {
	return &Restore{
		repo:                in.Repo,
		kubernetesRepo:      in.KubernetesRepo,
		nodeLocalVolumeRepo: in.NodeLocalVolumeRepo,
		restoreVol:          in.RestoreVol,
		retryInterval:       in.RetryInterval,
		processUID:          in.ProcessUID,
		targetSnapshotID:    in.TargetSnapshotID,
		rawImageChunkSize:   in.RawImageChunkSize,
		targetPVCUID:        in.TargetPVCUID,
	}
}

// Perform executes the restore process. If it can't get lock, it returns ErrCantLock.
func (r *Restore) Perform() error {
	err := r.repo.StartOrRestartAction(r.processUID, model.Restore)
	if err != nil {
		if errors.Is(err, model.ErrBusy) {
			return job.ErrCantLock
		}
		return fmt.Errorf("failed to start or restart action: %w", err)
	}
	if err := r.doRestore(); err != nil {
		return fmt.Errorf("failed to perform restore: %w", err)
	}
	if err := r.repo.CompleteAction(r.processUID); err != nil {
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

	d, err := getRestorePrivateData(r.repo, r.processUID)
	if err != nil {
		return fmt.Errorf("failed to get private data: %w", err)
	}

	if err := r.doInitialPhase(d); err != nil {
		return fmt.Errorf("initial phase failed: %w", err)
	}

	if err := r.doDiscardPhase(d); err != nil {
		return fmt.Errorf("discard phase failed: %w", err)
	}

	if err := r.doRestoreRawImagePhase(d, metadata.Raw); err != nil {
		return fmt.Errorf("restore raw image phase failed: %w", err)
	}

	if err := r.doRestoreDiffPhase(d, metadata.Diff); err != nil {
		return fmt.Errorf("restore diff phase failed: %w", err)
	}

	return nil
}

func (r *Restore) doInitialPhase(privateData *restorePrivateData) error {
	if privateData.Phase != "" {
		return nil
	}
	privateData.Phase = Discard
	return setRestorePrivateData(r.repo, r.processUID, privateData)
}

func (r *Restore) doDiscardPhase(privateData *restorePrivateData) error {
	if privateData.Phase != Discard {
		return nil
	}
	if err := r.restoreVol.BlkDiscard(); err != nil {
		return fmt.Errorf("blkdiscard failed: %w", err)
	}
	privateData.Phase = RestoreRawImage
	return setRestorePrivateData(r.repo, r.processUID, privateData)
}

func (r *Restore) doRestoreRawImagePhase(privateData *restorePrivateData, raw *job.BackupMetadataEntry) error {
	if privateData.Phase != RestoreRawImage {
		return nil
	}
	err := r.loopCopyChunk(privateData, raw.SnapSize)
	if err != nil {
		return fmt.Errorf("failed to restore raw image: %w", err)
	}

	if r.targetSnapshotID == raw.SnapID {
		privateData.Phase = Completed
		return setRestorePrivateData(r.repo, r.processUID, privateData)
	}

	privateData.Phase = RestoreDiff
	return setRestorePrivateData(r.repo, r.processUID, privateData)
}

func (r *Restore) loopCopyChunk(privateData *restorePrivateData, rawImageSize int) error {
	chunkCount := int(math.Ceil(float64(rawImageSize) / float64(r.rawImageChunkSize)))
	for i := privateData.NextRawImageChunk; i < chunkCount; i++ {
		if err := r.restoreVol.CopyChunk(r.nodeLocalVolumeRepo.GetRawImagePath(), i, r.rawImageChunkSize); err != nil {
			return fmt.Errorf("failed to copy chunk %d: %w", i, err)
		}

		privateData.NextRawImageChunk = i + 1
		if err := setRestorePrivateData(r.repo, r.processUID, privateData); err != nil {
			return fmt.Errorf("failed to set nextRawImageChunk to %d: %w",
				privateData.NextRawImageChunk, err)
		}
	}
	return nil
}

func (r *Restore) doRestoreDiffPhase(privateData *restorePrivateData, diffs []*job.BackupMetadataEntry) error {
	if privateData.Phase != RestoreDiff {
		return nil
	}

	for _, diff := range diffs {
		err := r.loopApplyDiff(privateData, diff)
		if err != nil {
			return fmt.Errorf("failed to apply diffs: %w", err)
		}
		if diff.SnapID == r.targetSnapshotID {
			break
		}
	}

	privateData.Phase = Completed
	return setRestorePrivateData(r.repo, r.processUID, privateData)
}

func (r *Restore) loopApplyDiff(privateData *restorePrivateData, diff *job.BackupMetadataEntry) error {
	partCount := int(math.Ceil(float64(diff.SnapSize) / float64(diff.PartSize)))
	for i := privateData.NextDiffPart; i < partCount; i++ {
		if err := r.restoreVol.ApplyDiff(
			r.nodeLocalVolumeRepo.GetDiffPartPath(diff.SnapID, i),
		); err != nil {
			return fmt.Errorf("failed to apply diff: %w", err)
		}

		privateData.NextDiffPart = i + 1
		if err := setRestorePrivateData(r.repo, r.processUID, privateData); err != nil {
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

func getRestorePrivateData(repo model.FinRepository, processUID string) (*restorePrivateData, error) {
	privateData, err := repo.GetActionPrivateData(processUID)
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

func setRestorePrivateData(repo model.FinRepository, processUID string, data *restorePrivateData) error {
	privateData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal private data: %w", err)
	}
	if err := repo.UpdateActionPrivateData(processUID, privateData); err != nil {
		return fmt.Errorf("failed to update private data: %w", err)
	}
	return nil
}

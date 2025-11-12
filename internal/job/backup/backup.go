package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	modeFull        = "full"
	modeIncremental = "incremental"
)

type Backup struct {
	repo                      model.FinRepository
	k8sClient                 kubernetes.Interface
	rbdRepo                   model.RBDRepository
	nodeLocalVolumeRepo       model.NodeLocalVolumeRepository
	actionUID                 string
	targetRBDPool             string
	targetRBDImageName        string
	targetSnapshotID          int
	sourceCandidateSnapshotID *int
	targetPVCName             string
	targetPVCNamespace        string
	targetPVCUID              string
	maxPartSize               uint64
	expansionUnitSize         uint64
	rawChecksumChunkSize      uint64
	diffChecksumChunkSize     uint64
	disableChecksumVerify     bool
}

func NewBackup(in *input.Backup) *Backup {
	return &Backup{
		repo:                      in.Repo,
		k8sClient:                 in.K8sClient,
		rbdRepo:                   in.RBDRepo,
		nodeLocalVolumeRepo:       in.NodeLocalVolumeRepo,
		actionUID:                 in.ActionUID,
		targetRBDPool:             in.TargetRBDPoolName,
		targetRBDImageName:        in.TargetRBDImageName,
		targetSnapshotID:          in.TargetSnapshotID,
		sourceCandidateSnapshotID: in.SourceCandidateSnapshotID,
		targetPVCName:             in.TargetPVCName,
		targetPVCNamespace:        in.TargetPVCNamespace,
		targetPVCUID:              in.TargetPVCUID,
		maxPartSize:               in.MaxPartSize,
		expansionUnitSize:         in.ExpansionUnitSize,
		rawChecksumChunkSize:      in.RawChecksumChunkSize,
		diffChecksumChunkSize:     in.DiffChecksumChunkSize,
		disableChecksumVerify:     in.DisableChecksumVerify,
	}
}

// Perform executes the backup process. If it can't get lock, it returns ErrCantLock.
func (b *Backup) Perform() error {
	err := b.repo.StartOrRestartAction(b.actionUID, model.Backup)
	if err != nil {
		if errors.Is(err, model.ErrBusy) {
			return job.ErrCantLock
		}
		return fmt.Errorf("failed to start or restart action: %w", err)
	}
	if err := b.doBackup(); err != nil {
		return fmt.Errorf("failed to perform backup: %w", err)
	}
	if err := b.repo.CompleteAction(b.actionUID); err != nil {
		return fmt.Errorf("failed to complete action: %w", err)
	}
	return nil
}

func (b *Backup) doBackup() error {
	privateData, err := getBackupPrivateData(b.repo, b.actionUID)
	if err != nil {
		return fmt.Errorf("failed to get private data: %w", err)
	}

	if privateData.Mode == "" {
		tmpMode, err := b.decideBackupMode()
		if err != nil {
			return fmt.Errorf("failed to decide backup mode: %w", err)
		}

		switch tmpMode {
		case modeFull:
			if err := b.prepareFullBackup(); err != nil {
				return fmt.Errorf("failed to prepare full backup: %w", err)
			}
		case modeIncremental:
			if err := b.prepareIncrementalBackup(); err != nil {
				return fmt.Errorf("failed to prepare incremental backup: %w", err)
			}
		}

		privateData.Mode = tmpMode
		if err := setBackupPrivateData(b.repo, b.actionUID, privateData); err != nil {
			return fmt.Errorf("failed to set private data: %w", err)
		}
	}

	targetSnapshot, err := getSnapshot(
		b.rbdRepo, b.targetRBDPool, b.targetRBDImageName, b.targetSnapshotID)
	if err != nil {
		return fmt.Errorf("failed to get target snapshot: %w", err)
	}

	if err := b.nodeLocalVolumeRepo.MakeDiffDir(b.targetSnapshotID); err != nil &&
		!errors.Is(err, model.ErrAlreadyExists) {
		return fmt.Errorf("failed to create diff directory: %w", err)
	}

	var sourceSnapshotName *string
	if privateData.Mode == modeIncremental {
		sourceSnapshot, err := getSnapshot(b.rbdRepo, b.targetRBDPool, b.targetRBDImageName, *b.sourceCandidateSnapshotID)
		if err != nil {
			return fmt.Errorf("failed to get source snapshot: %w", err)
		}
		sourceSnapshotName = &sourceSnapshot.Name
	}

	if err := b.loopExportDiff(privateData, targetSnapshot, sourceSnapshotName); err != nil {
		return fmt.Errorf("failed to loop export diff: %w", err)
	}

	if err := b.declareStoringCompleted(targetSnapshot); err != nil {
		return fmt.Errorf("failed to declare storing finished: %w", err)
	}

	// FIXME: We need to verify the backup.

	if privateData.Mode == modeFull {
		if err := b.loopApplyDiff(privateData, targetSnapshot); err != nil {
			return fmt.Errorf("failed to loop apply diff: %w", err)
		}
		if err := b.declareFullBackupApplicationCompleted(targetSnapshot); err != nil {
			return fmt.Errorf("failed to declare full backup application completed: %w", err)
		}
		if err := b.nodeLocalVolumeRepo.RemoveDiffDirRecursively(b.targetSnapshotID); err != nil {
			return fmt.Errorf("failed to remove diff directory: %w", err)
		}
	}

	return nil
}

func (b *Backup) decideBackupMode() (string, error) {
	if b.sourceCandidateSnapshotID == nil {
		return modeFull, nil
	}
	if _, err := job.GetBackupMetadata(b.repo); err != nil {
		if !errors.Is(err, model.ErrNotFound) {
			return "", fmt.Errorf("failed to get backup metadata: %w", err)
		}
		return modeFull, nil
	}
	return modeIncremental, nil
}

func (b *Backup) prepareFullBackup() error {
	if _, err := job.GetBackupMetadata(b.repo); err == nil {
		return fmt.Errorf("backup metadata already exists: %w", err)
	} else if !errors.Is(err, model.ErrNotFound) {
		return fmt.Errorf("failed to get backup metadata: %w", err)
	}

	ctx := context.Background()
	targetPVC, err := b.k8sClient.CoreV1().
		PersistentVolumeClaims(b.targetPVCNamespace).
		Get(ctx, b.targetPVCName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get target PVC: %w", err)
	}
	if string(targetPVC.GetUID()) != b.targetPVCUID {
		return fmt.Errorf("target PVC UID (%s) does not match the expected one (%s)", targetPVC.GetUID(), b.targetPVCUID)
	}

	targetPV, err := b.k8sClient.CoreV1().PersistentVolumes().Get(ctx, targetPVC.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get target PV: %w", err)
	}
	if targetPV.Spec.CSI.VolumeAttributes["imageName"] != b.targetRBDImageName {
		return fmt.Errorf("target PV image name (%s) does not match the expected one %s",
			targetPV.Spec.CSI.VolumeAttributes["imageName"], b.targetRBDImageName)
	}

	if err = b.nodeLocalVolumeRepo.PutPVC(targetPVC); err != nil {
		return fmt.Errorf("failed to store target PVC: %w", err)
	}

	if err = b.nodeLocalVolumeRepo.PutPV(targetPV); err != nil {
		return fmt.Errorf("failed to store target PV: %w", err)
	}

	return nil
}

func (b *Backup) prepareIncrementalBackup() error {
	metadata, err := job.GetBackupMetadata(b.repo)
	if err != nil {
		return fmt.Errorf("failed to get backup metadata: %w", err)
	}
	if metadata.PVCUID != b.targetPVCUID {
		return fmt.Errorf("PVC UID in metadata table (%s) does not match the expected one (%s)",
			metadata.PVCUID, b.targetPVCUID)
	}
	if metadata.RBDImageName != b.targetRBDImageName {
		return fmt.Errorf("RBD image name in metadata table (%s) does not match the expected one (%s)",
			metadata.RBDImageName, b.targetRBDImageName)
	}
	if len(metadata.Diff) != 0 {
		return fmt.Errorf("diff metadata already exists for PVC %s", b.targetPVCUID)
	}
	if metadata.Raw == nil {
		return fmt.Errorf("raw backup metadata does not exist for PVC %s", b.targetPVCUID)
	}
	if metadata.Raw.SnapID != *b.sourceCandidateSnapshotID {
		return fmt.Errorf("snapshot ID in metadata table (%d) does not match the expected one (%d)",
			metadata.Raw.SnapID, *b.sourceCandidateSnapshotID)
	}

	return nil
}

func getSnapshot(repo model.RBDRepository, poolName, imageName string, snapshotID int) (*model.RBDSnapshot, error) {
	snapshots, err := repo.ListSnapshots(poolName, imageName)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	targetSnapshotIndex := slices.IndexFunc(snapshots, func(s *model.RBDSnapshot) bool {
		return s.ID == snapshotID
	})
	if targetSnapshotIndex == -1 {
		return nil, fmt.Errorf("target snapshot ID %d not found", snapshotID)
	}
	return snapshots[targetSnapshotIndex], nil
}

func (b *Backup) loopExportDiff(
	privateData *backupPrivateData,
	targetSnapshot *model.RBDSnapshot,
	sourceSnapshotName *string,
) error {
	partCount := int(math.Ceil(float64(targetSnapshot.Size) / float64(b.maxPartSize)))
	for i := privateData.NextStorePart; i < partCount; i++ {
		diffPartPath := b.nodeLocalVolumeRepo.GetDiffPartPath(b.targetSnapshotID, i)
		if err := b.rbdRepo.ExportDiff(&model.ExportDiffInput{
			PoolName:              b.targetRBDPool,
			ReadOffset:            b.maxPartSize * uint64(i),
			ReadLength:            b.maxPartSize,
			FromSnap:              sourceSnapshotName,
			MidSnapPrefix:         targetSnapshot.Name,
			ImageName:             b.targetRBDImageName,
			TargetSnapName:        targetSnapshot.Name,
			OutputFile:            diffPartPath,
			DiffChecksumChunkSize: b.diffChecksumChunkSize,
		}); err != nil {
			return fmt.Errorf("failed to export diff: %w", err)
		}

		if err := job.SyncData(diffPartPath); err != nil {
			return fmt.Errorf("failed to sync %q: %w", diffPartPath, err)
		}

		privateData.NextStorePart = i + 1
		if err := setBackupPrivateData(b.repo, b.actionUID, privateData); err != nil {
			return fmt.Errorf("failed to set nextStorePart to %d: %w", privateData.NextStorePart, err)
		}
	}
	return nil
}

func (b *Backup) declareStoringCompleted(targetSnapshot *model.RBDSnapshot) error {
	var metadata *job.BackupMetadata
	metadata, err := job.GetBackupMetadata(b.repo)
	if err != nil {
		if !errors.Is(err, model.ErrNotFound) {
			return fmt.Errorf("failed to get backup metadata: %w", err)
		}
		metadata = &job.BackupMetadata{}
	}

	metadata.PVCUID = b.targetPVCUID
	metadata.RBDImageName = b.targetRBDImageName
	if len(metadata.Diff) == 0 {
		metadata.Diff = []*job.BackupMetadataEntry{{}}
	}
	metadata.Diff[0].SnapID = targetSnapshot.ID
	metadata.Diff[0].SnapName = targetSnapshot.Name
	metadata.Diff[0].SnapSize = targetSnapshot.Size
	metadata.Diff[0].PartSize = b.maxPartSize
	metadata.Diff[0].CreatedAt = targetSnapshot.Timestamp.Time

	return job.SetBackupMetadata(b.repo, metadata)
}

func (b *Backup) loopApplyDiff(privateData *backupPrivateData, targetSnapshot *model.RBDSnapshot) error {
	partCount := int(math.Ceil(float64(targetSnapshot.Size) / float64(b.maxPartSize)))
	for i := privateData.NextPatchPart; i < partCount; i++ {
		sourceSnapshotName, targetSnapshotName :=
			job.CalcSnapshotNamesWithOffset("", targetSnapshot.Name, i, partCount, b.maxPartSize)
		rawImagePath := b.nodeLocalVolumeRepo.GetRawImagePath()
		diffPartPath := b.nodeLocalVolumeRepo.GetDiffPartPath(b.targetSnapshotID, i)
		if err := b.rbdRepo.ApplyDiffToRawImage(
			rawImagePath,
			diffPartPath,
			sourceSnapshotName,
			targetSnapshotName,
			b.expansionUnitSize,
		); err != nil {
			return fmt.Errorf("failed to apply diff: %w", err)
		}

		if err := job.SyncData(rawImagePath); err != nil {
			return fmt.Errorf("failed to sync %q: %w", rawImagePath, err)
		}

		privateData.NextPatchPart = i + 1
		if err := setBackupPrivateData(b.repo, b.actionUID, privateData); err != nil {
			return fmt.Errorf("failed to set nextPatchPart to %d: %w", privateData.NextPatchPart, err)
		}
	}
	return nil
}

func (b *Backup) declareFullBackupApplicationCompleted(targetSnapshot *model.RBDSnapshot) error {
	metadata, err := job.GetBackupMetadata(b.repo)
	if err != nil {
		return fmt.Errorf("failed to get backup metadata: %w", err)
	}

	metadata.PVCUID = b.targetPVCUID
	metadata.RBDImageName = b.targetRBDImageName
	metadata.Diff = []*job.BackupMetadataEntry{}
	metadata.Raw = &job.BackupMetadataEntry{
		SnapID:    targetSnapshot.ID,
		SnapName:  targetSnapshot.Name,
		SnapSize:  targetSnapshot.Size,
		PartSize:  b.maxPartSize,
		CreatedAt: targetSnapshot.Timestamp.Time,
	}

	return job.SetBackupMetadata(b.repo, metadata)
}

type backupPrivateData struct {
	NextStorePart int    `json:"nextStorePart,omitempty"`
	NextPatchPart int    `json:"nextPatchPart,omitempty"`
	Mode          string `json:"mode,omitempty"`
}

func getBackupPrivateData(repo model.FinRepository, actionUID string) (*backupPrivateData, error) {
	privateData, err := repo.GetActionPrivateData(actionUID)
	if err != nil {
		return nil, err
	}

	if len(privateData) == 0 {
		return &backupPrivateData{}, nil
	}

	var data backupPrivateData
	if err := json.Unmarshal(privateData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal private data: %w", err)
	}
	return &data, nil
}

func setBackupPrivateData(repo model.FinRepository, actionUID string, data *backupPrivateData) error {
	privateData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal private data: %w", err)
	}
	if err := repo.UpdateActionPrivateData(actionUID, privateData); err != nil {
		return fmt.Errorf("failed to update private data: %w", err)
	}
	return nil
}

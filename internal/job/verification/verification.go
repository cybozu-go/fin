package verification

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"os/exec"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/model"
	corev1 "k8s.io/api/core/v1"
)

type Verification struct {
	repo             model.FinRepository
	nlvRepo          model.NodeLocalVolumeRepository
	rbdRepo          model.RBDRepository
	actionUID        string
	targetSnapshotID int
	targetPVCUID     string
}

func NewVerification(in *input.Verification) *Verification {
	return &Verification{
		repo:             in.Repo,
		nlvRepo:          in.NLVRepo,
		rbdRepo:          in.RBDRepo,
		actionUID:        in.ActionUID,
		targetSnapshotID: in.TargetSnapshotID,
		targetPVCUID:     in.TargetPVCUID,
	}
}

var (
	ErrFsckFailed = errors.New("fsck failed")
)

func (v *Verification) Perform() error {
	if err := v.repo.StartOrRestartAction(v.actionUID, model.Verification); err != nil {
		if errors.Is(err, model.ErrBusy) {
			return job.ErrCantLock
		}
		return fmt.Errorf("failed to start or restart action: %w", err)
	}

	if err := v.doVerify(); err != nil {
		return fmt.Errorf("failed to verify: %w", err)
	}

	if err := v.repo.CompleteAction(v.actionUID); err != nil {
		return fmt.Errorf("failed to complete action: %w", err)
	}

	return nil
}

func (v *Verification) doVerify() error {
	slog.Info("getting backup metadata")
	raw, diff0, pvcUID, err := v.getBackupMetadata()
	if err != nil {
		return fmt.Errorf("failed to check backup metadata: %w", err)
	}
	if pvcUID != v.targetPVCUID {
		return fmt.Errorf("backup target PVC UID %q does not match the current target PVC UID %q", pvcUID, v.targetPVCUID)
	}
	if raw == nil {
		return fmt.Errorf("no raw found in backup metadata")
	}

	slog.Info("checking PVC volume mode")
	isBlock, err := v.checkPVCVolumeModeIsBlock()
	if err != nil {
		return fmt.Errorf("failed to check PVC volume mode: %w", err)
	}
	if isBlock {
		slog.Info("skipping verification for Block volume")
		return nil
	}

	slog.Info("getting private data")
	privateData, err := v.getPrivateData()
	if err != nil {
		return fmt.Errorf("failed to get private data: %w", err)
	}

	nextDiffPart := 0
	if privateData.NextDiffPart == nil {
		slog.Info("creating instant_verify.img by reflink")
		if err := v.nlvRepo.ReflinkRawImageToInstantVerifyImage(); err != nil {
			return fmt.Errorf("failed to copy reflink: %w", err)
		}
	} else {
		nextDiffPart = *privateData.NextDiffPart
		slog.Info("resuming verification from private data", slog.Int("nextDiffPart", nextDiffPart))

		instantVerifyImageExists, err := v.doesInstantVerifyImageExist()
		if err != nil {
			return fmt.Errorf("failed to check instant_verify.img existence: %w", err)
		}

		if !instantVerifyImageExists {
			if diff0 == nil {
				return fmt.Errorf("no diff found in backup metadata")
			}
			if nextDiffPart != calculateMaxPartNumber(diff0) {
				return fmt.Errorf("instant_verify.img is missing")
			}
			// already applied all diffs and cleaned up instant_verify.img, so
			// let's skip fsck.
			slog.Info("skipping fsck because instant_verify.img is missing but all diffs are already applied")
			return nil
		}
	}

	if diff0 != nil {
		slog.Info("applying diffs", slog.Int("nextDiffPart", nextDiffPart))
		if err := v.loopApplyDiff(nextDiffPart, raw, diff0); err != nil {
			return fmt.Errorf("failed to apply diff: %w", err)
		}
	}

	slog.Info("running fsck")
	if err := runFsck(v.nlvRepo.GetInstantVerifyImagePath()); err != nil {
		return fmt.Errorf("failed to run fsck: %w", err)
	}

	slog.Info("removing instant_verify.img")
	if err := v.nlvRepo.RemoveInstantVerifyImage(); err != nil {
		return fmt.Errorf("failed to clean up: %w", err)
	}

	return nil
}

func (v *Verification) getBackupMetadata() (
	*job.BackupMetadataEntry,
	*job.BackupMetadataEntry,
	string,
	error,
) {
	metadata, err := job.GetBackupMetadata(v.repo)
	if err != nil {
		return nil, nil, "", err
	}
	if len(metadata.Diff) == 0 {
		return metadata.Raw, nil, metadata.PVCUID, nil
	}
	return metadata.Raw, metadata.Diff[0], metadata.PVCUID, nil
}

func (v *Verification) checkPVCVolumeModeIsBlock() (bool, error) {
	pvc, err := v.nlvRepo.GetPVC()
	if err != nil {
		return false, err
	}
	if pvc.Spec.VolumeMode == nil {
		return false, nil
	}
	isBlock := *pvc.Spec.VolumeMode == corev1.PersistentVolumeBlock
	return isBlock, nil
}

type verificationPrivateData struct {
	NextDiffPart *int `json:"nextDiffPart"`
}

func (v *Verification) getPrivateData() (*verificationPrivateData, error) {
	privateData, err := v.repo.GetActionPrivateData(v.actionUID)
	if err != nil {
		return nil, err
	}

	if len(privateData) == 0 {
		return &verificationPrivateData{}, nil
	}

	var data verificationPrivateData
	if err := json.Unmarshal(privateData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal private data: %w", err)
	}
	return &data, nil
}

func (v *Verification) setPrivateData(nextDiffPart int) error {
	privateData, err := json.Marshal(verificationPrivateData{
		NextDiffPart: &nextDiffPart,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal private data: %w", err)
	}
	if err := v.repo.UpdateActionPrivateData(v.actionUID, privateData); err != nil {
		return fmt.Errorf("failed to update private data: %w", err)
	}
	return nil
}

func (v *Verification) doesInstantVerifyImageExist() (bool, error) {
	fi, err := os.Stat(v.nlvRepo.GetInstantVerifyImagePath())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat instant_verify.img: %w", err)
	}
	if fi.IsDir() {
		return false, fmt.Errorf("instant_verify.img is a directory")
	}
	return true, nil
}

func calculateMaxPartNumber(diff0 *job.BackupMetadataEntry) int {
	return int(math.Ceil(float64(diff0.SnapSize) / float64(diff0.PartSize)))
}

func (v *Verification) loopApplyDiff(
	nextDiffPart int,
	raw *job.BackupMetadataEntry,
	diff0 *job.BackupMetadataEntry,
) error {
	partCount := calculateMaxPartNumber(diff0)
	for i := nextDiffPart; i < partCount; i++ {
		sourceSnapshotName, targetSnapshotName :=
			job.CalcSnapshotNamesWithOffset(raw.SnapName, diff0.SnapName, i, partCount, diff0.PartSize)
		if err := v.rbdRepo.ApplyDiffToRawImage(
			v.nlvRepo.GetInstantVerifyImagePath(),
			v.nlvRepo.GetDiffPartPath(v.targetSnapshotID, i),
			sourceSnapshotName,
			targetSnapshotName,
		); err != nil {
			return fmt.Errorf("failed to apply diff: %w", err)
		}

		if err := v.setPrivateData(i + 1); err != nil {
			return fmt.Errorf("failed to set private data: %w", err)
		}
	}
	return nil
}

func runFsck(imagePath string) error {
	output, err := exec.Command("e2fsck", "-fn", imagePath).CombinedOutput()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return fmt.Errorf("%w: e2fsck failed: %s: %s", ErrFsckFailed, exitError.String(), output)
		}
		return fmt.Errorf("failed to run e2fsck: %w: %s", err, output)
	}
	return nil
}

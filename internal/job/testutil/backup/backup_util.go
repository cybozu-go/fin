package util

import (
	"fmt"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/google/uuid"
)

// NewBackupInput creates a BackupInput for testing using a KubernetesRepository and a fake.VolumeInfo.
func NewBackupInput(k8sRepo model.KubernetesRepository, volume *fake.VolumeInfo,
	targetSnapID int, sourceSnapID *int, maxPartSize int) *backup.BackupInput {
	pvc, err := k8sRepo.GetPVC(volume.PVCName, volume.Namespace)
	if err != nil {
		panic(fmt.Sprintf("failed to get PVC: %v", err))
	}
	pv, err := k8sRepo.GetPV(volume.PVName)
	if err != nil {
		panic(fmt.Sprintf("failed to get PV: %v", err))
	}

	return &backup.BackupInput{
		RetryInterval:             1 * time.Second,
		ActionUID:                 uuid.New().String(),
		TargetRBDPoolName:         pv.Spec.CSI.VolumeAttributes["pool"],
		TargetRBDImageName:        pv.Spec.CSI.VolumeAttributes["imageName"],
		TargetSnapshotID:          targetSnapID,
		SourceCandidateSnapshotID: sourceSnapID,
		TargetPVCName:             pvc.Name,
		TargetPVCNamespace:        pvc.Namespace,
		TargetPVCUID:              string(pvc.UID),
		MaxPartSize:               maxPartSize,
	}
}

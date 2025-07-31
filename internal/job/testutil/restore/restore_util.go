package restore

import (
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/restore"
	"github.com/cybozu-go/fin/internal/model"
)

func NewRestoreInputTemplate(bi *backup.BackupInput,
	rVol model.RestoreVolume, chunkSize, snapID int) *restore.RestoreInput {
	return &restore.RestoreInput{
		Repo:                bi.Repo,
		KubernetesRepo:      bi.KubernetesRepo,
		NodeLocalVolumeRepo: bi.NodeLocalVolumeRepo,
		RestoreVol:          rVol,
		RawImageChunkSize:   int64(chunkSize),
		TargetSnapshotID:    snapID,
		RetryInterval:       bi.RetryInterval,
		ActionUID:           bi.ActionUID,
		TargetPVCUID:        bi.TargetPVCUID,
	}
}

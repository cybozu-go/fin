package input

import (
	"github.com/cybozu-go/fin/internal/model"
)

type Restore struct {
	Repo                 model.FinRepository
	RBDRepo              model.RBDRepository
	NodeLocalVolumeRepo  model.NodeLocalVolumeRepository
	RestoreVol           model.RestoreVolume
	ActionUID            string
	TargetSnapshotID     int
	RawImageChunkSize    uint64
	EnableChecksumVerify bool
	TargetPVCUID         string
}

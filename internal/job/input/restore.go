package input

import (
	"time"

	"github.com/cybozu-go/fin/internal/model"
)

type Restore struct {
	Repo                model.FinRepository
	NodeLocalVolumeRepo model.NodeLocalVolumeRepository
	RestoreVol          model.RestoreVolume
	RetryInterval       time.Duration
	ActionUID           string
	TargetSnapshotID    int
	RawImageChunkSize   int64
	TargetPVCUID        string
}

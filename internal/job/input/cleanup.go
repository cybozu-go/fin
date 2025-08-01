package input

import "github.com/cybozu-go/fin/internal/model"

type Cleanup struct {
	Repo                model.FinRepository
	NodeLocalVolumeRepo model.NodeLocalVolumeRepository
	ActionUID           string
	TargetSnapshotID    int
	TargetPVCUID        string
}

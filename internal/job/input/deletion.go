package input

import "github.com/cybozu-go/fin/internal/model"

type Deletion struct {
	Repo                model.FinRepository
	RBDRepo             model.RBDRepository
	NodeLocalVolumeRepo model.NodeLocalVolumeRepository
	ActionUID           string
	TargetSnapshotID    int
	TargetPVCUID        string
}

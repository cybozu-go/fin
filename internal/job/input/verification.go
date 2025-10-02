package input

import "github.com/cybozu-go/fin/internal/model"

type Verification struct {
	Repo             model.FinRepository
	RBDRepo          model.RBDRepository
	NLVRepo          model.NodeLocalVolumeRepository
	FsckRepo         model.FsckRepository
	ActionUID        string
	TargetSnapshotID int
	TargetPVCUID     string
}

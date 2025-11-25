package input

import "github.com/cybozu-go/fin/internal/model"

type Verification struct {
	Repo                 model.FinRepository
	RBDRepo              model.RBDRepository
	NLVRepo              model.NodeLocalVolumeRepository
	ActionUID            string
	TargetSnapshotID     int
	TargetPVCUID         string
	ExpansionUnitSize    uint64
	EnableChecksumVerify bool
}

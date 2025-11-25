package input

import (
	"github.com/cybozu-go/fin/internal/model"
	"k8s.io/client-go/kubernetes"
)

type Backup struct {
	Repo                      model.FinRepository
	K8sClient                 kubernetes.Interface
	RBDRepo                   model.RBDRepository
	NodeLocalVolumeRepo       model.NodeLocalVolumeRepository
	ActionUID                 string
	TargetRBDPoolName         string
	TargetRBDImageName        string
	TargetSnapshotID          int
	SourceCandidateSnapshotID *int
	TargetPVCName             string
	TargetPVCNamespace        string
	TargetPVCUID              string
	MaxPartSize               uint64
	ExpansionUnitSize         uint64
	RawChecksumChunkSize      uint64
	DiffChecksumChunkSize     uint64
	EnableChecksumVerify      bool
}

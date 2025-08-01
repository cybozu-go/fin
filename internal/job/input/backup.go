package input

import (
	"time"

	"github.com/cybozu-go/fin/internal/model"
)

type Backup struct {
	Repo                      model.FinRepository
	KubernetesRepo            model.KubernetesRepository
	RBDRepo                   model.RBDRepository
	NodeLocalVolumeRepo       model.NodeLocalVolumeRepository
	RetryInterval             time.Duration
	ActionUID                 string
	TargetRBDPoolName         string
	TargetRBDImageName        string
	TargetSnapshotID          int
	SourceCandidateSnapshotID *int
	TargetPVCName             string
	TargetPVCNamespace        string
	TargetPVCUID              string
	MaxPartSize               int
}

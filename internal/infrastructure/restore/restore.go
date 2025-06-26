package restore

import (
	"errors"

	"github.com/cybozu-go/fin/internal/model"
)

type RestoreRepository struct {
	path string
}

var _ model.RestoreRepository = &RestoreRepository{}

func NewRestoreRepository(devPath string) *RestoreRepository {
	return &RestoreRepository{
		path: devPath,
	}
}

func (r *RestoreRepository) GetPath() string {
	return r.path
}

func (r *RestoreRepository) BlkDiscard() error {
	return errors.New("not implemented")
}

func (r *RestoreRepository) ApplyDiff(diffPath string) error {
	return errors.New("not implemented")
}

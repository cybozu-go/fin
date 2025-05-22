package ceph

import (
	"errors"

	"github.com/cybozu-go/fin/internal/model"
)

type RBDRepository struct{}

var _ model.RBDRepository = &RBDRepository{}

func (r *RBDRepository) ListSnapshots(poolName, imageName string) ([]*model.RBDSnapshot, error) {
	return nil, errors.New("not implemented")
}

func (r *RBDRepository) ExportDiff(input *model.ExportDiffInput) error {
	return errors.New("not implemented")
}

func (r *RBDRepository) ApplyDiff(rawImageFilePath, diffFilePath string) error {
	return errors.New("not implemented")
}

func (r *RBDRepository) CreateEmptyRawImage(filePath string, size int) error {
	return errors.New("not implemented")
}

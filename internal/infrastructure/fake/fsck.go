package fake

import (
	"bytes"
	"os"

	"github.com/cybozu-go/fin/internal/model"
)

type FsckRepository struct {
	expectedVolumeData []byte
}

var _ model.FsckRepository = (*FsckRepository)(nil)

func NewFsckRepository(expectedVolumeData []byte) *FsckRepository {
	return &FsckRepository{
		expectedVolumeData: expectedVolumeData,
	}
}

func (r *FsckRepository) Run(imagePath string) error {
	volumeData, err := os.ReadFile(imagePath)
	if err != nil {
		return err
	}
	if bytes.Equal(r.expectedVolumeData, volumeData) {
		return nil
	}
	return model.ErrFsckFailed
}

package fsck

import (
	"errors"
	"fmt"
	"os/exec"

	"github.com/cybozu-go/fin/internal/model"
)

type FsckRepository struct {
}

var _ model.FsckRepository = (*FsckRepository)(nil)

func NewFsckRepository() *FsckRepository {
	return &FsckRepository{}
}

func (r *FsckRepository) Run(imagePath string) error {
	output, err := exec.Command("e2fsck", "-fn", imagePath).CombinedOutput()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return fmt.Errorf("%w: e2fsck failed: %s: %s", model.ErrFsckFailed, exitError.String(), output)
		}
		return fmt.Errorf("failed to run e2fsck: %w: %s", err, output)
	}
	return nil
}

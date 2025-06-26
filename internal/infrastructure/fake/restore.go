package fake

import (
	"fmt"

	"github.com/cybozu-go/fin/internal/model"
)

type restoreRepository struct {
	path         string
	appliedDiffs []*ExportedDiff
}

var _ model.RestoreRepository = &restoreRepository{}

func NewRestoreRepository(
	path string,
) *restoreRepository {
	return &restoreRepository{
		path:         path,
		appliedDiffs: make([]*ExportedDiff, 0),
	}
}

func (r *restoreRepository) ApplyDiff(diffFilePath string) error {
	diff, err := ReadDiff(diffFilePath)
	if err != nil {
		return fmt.Errorf("failed to decode diff: %w", err)
	}

	// fake the diff apply
	r.appliedDiffs = append(r.appliedDiffs, diff)

	return nil
}

func (r *restoreRepository) GetPath() string {
	return r.path
}

func (r *restoreRepository) BlkDiscard() error {
	return nil
}

func (r *restoreRepository) AppliedDiffs() []*ExportedDiff {
	return r.appliedDiffs
}

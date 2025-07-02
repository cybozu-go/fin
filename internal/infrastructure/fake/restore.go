package fake

import (
	"fmt"

	r "github.com/cybozu-go/fin/internal/infrastructure/restore"
	"github.com/cybozu-go/fin/internal/model"
)

type restoreRepository struct {
	real         *r.RestoreRepository
	appliedDiffs []*ExportedDiff
}

var _ model.RestoreRepository = &restoreRepository{}

func NewRestoreRepository(
	path string,
) *restoreRepository {
	return &restoreRepository{
		real:         r.NewRestoreRepository(path),
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
	return r.real.GetPath()
}

func (r *restoreRepository) BlkDiscard() error {
	return nil
}

func (r *restoreRepository) AppliedDiffs() []*ExportedDiff {
	return r.appliedDiffs
}

func (r *restoreRepository) CopyChunk(rawPath string, index int, chunkSize int64) error {
	return r.real.CopyChunk(rawPath, index, chunkSize)
}

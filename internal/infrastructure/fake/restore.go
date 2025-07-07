package fake

import (
	"fmt"

	r "github.com/cybozu-go/fin/internal/infrastructure/restore"
	"github.com/cybozu-go/fin/internal/model"
)

type restoreVolume struct {
	real         *r.RestoreVolume
	appliedDiffs []*ExportedDiff
}

var _ model.RestoreVolume = &restoreVolume{}

func NewRestoreVolume(
	path string,
) *restoreVolume {
	return &restoreVolume{
		real:         r.NewRestoreVolume(path),
		appliedDiffs: make([]*ExportedDiff, 0),
	}
}

func (r *restoreVolume) ApplyDiff(diffFilePath string) error {
	diff, err := ReadDiff(diffFilePath)
	if err != nil {
		return fmt.Errorf("failed to decode diff: %w", err)
	}

	// fake the diff apply
	r.appliedDiffs = append(r.appliedDiffs, diff)

	return nil
}

func (r *restoreVolume) GetPath() string {
	return r.real.GetPath()
}

func (r *restoreVolume) BlkDiscard() error {
	return nil
}

func (r *restoreVolume) AppliedDiffs() []*ExportedDiff {
	return r.appliedDiffs
}

func (r *restoreVolume) CopyChunk(rawPath string, index int, chunkSize int64) error {
	return r.real.CopyChunk(rawPath, index, chunkSize)
}

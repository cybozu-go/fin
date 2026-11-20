// Package diffexport stores the RBD diff of a snapshot as resumable parts.
package diffexport

import (
	"fmt"
	"io"
	"math"

	"github.com/cybozu-go/fin/internal/model"
)

type Exporter interface {
	ExportDiff(input *model.ExportDiffInput) (io.ReadCloser, error)
}

// PartStore stores one exported diff part. StorePart must close stream and
// make the part data durable before it returns nil, because Store records
// the checkpoint right after that.
type PartStore interface {
	StorePart(snapshotID, partIndex int, stream io.ReadCloser, checksumChunkSize uint64) error
}

type Target struct {
	PoolName           string
	ImageName          string
	SnapshotID         int
	SnapshotName       string
	SnapshotSize       uint64
	SourceSnapshotName *string
	MaxPartSize        uint64
	ChecksumChunkSize  uint64
}

// Store exports and stores the parts of target from part next, and calls
// checkpoint with the following part index after each stored part. It stops
// at the first error.
func Store(exporter Exporter, store PartStore, target *Target, next int, checkpoint func(next int) error) error {
	partCount := int(math.Ceil(float64(target.SnapshotSize) / float64(target.MaxPartSize)))
	for i := next; i < partCount; i++ {
		stream, err := exporter.ExportDiff(&model.ExportDiffInput{
			PoolName:       target.PoolName,
			ReadOffset:     target.MaxPartSize * uint64(i),
			ReadLength:     target.MaxPartSize,
			FromSnap:       target.SourceSnapshotName,
			MidSnapPrefix:  target.SnapshotName,
			ImageName:      target.ImageName,
			TargetSnapName: target.SnapshotName,
		})
		if err != nil {
			return fmt.Errorf("failed to export diff: %w", err)
		}

		if err := store.StorePart(target.SnapshotID, i, stream, target.ChecksumChunkSize); err != nil {
			return err
		}

		if err := checkpoint(i + 1); err != nil {
			return err
		}
	}
	return nil
}

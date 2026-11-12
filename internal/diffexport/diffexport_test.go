package diffexport_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/cybozu-go/fin/internal/diffexport"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errInjected = errors.New("injected")

type closeTrackingStream struct {
	io.Reader
	closed *bool
}

func (s *closeTrackingStream) Close() error {
	*s.closed = true
	return nil
}

// fakeExporter returns the export input of each call as the diff content, so
// the stored bytes show which range was exported for which part.
type fakeExporter struct {
	calls        []*model.ExportDiffInput
	failAtOffset *uint64
	closed       []*bool
}

func partContent(in *model.ExportDiffInput) []byte {
	from := "<nil>"
	if in.FromSnap != nil {
		from = *in.FromSnap
	}
	return fmt.Appendf(nil, "%s/%s@%s from %s offset %d length %d mid %s",
		in.PoolName, in.ImageName, in.TargetSnapName, from, in.ReadOffset, in.ReadLength, in.MidSnapPrefix)
}

func (e *fakeExporter) ExportDiff(in *model.ExportDiffInput) (io.ReadCloser, error) {
	e.calls = append(e.calls, in)
	if e.failAtOffset != nil && *e.failAtOffset == in.ReadOffset {
		e.failAtOffset = nil
		return nil, errInjected
	}
	closed := false
	e.closed = append(e.closed, &closed)
	return &closeTrackingStream{Reader: bytes.NewReader(partContent(in)), closed: &closed}, nil
}

type fakePartStore struct {
	parts       map[int][]byte
	storedOrder []int
	failAtPart  *int
}

func newFakePartStore() *fakePartStore {
	return &fakePartStore{parts: map[int][]byte{}}
}

func (s *fakePartStore) StorePart(snapshotID, partIndex int, stream io.ReadCloser, checksumChunkSize uint64) error {
	defer func() { _ = stream.Close() }()
	if snapshotID != testSnapshotID || checksumChunkSize != testChecksumChunkSize {
		return fmt.Errorf("unexpected snapshot ID %d or checksum chunk size %d", snapshotID, checksumChunkSize)
	}
	if s.failAtPart != nil && *s.failAtPart == partIndex {
		s.failAtPart = nil
		return errInjected
	}
	data, err := io.ReadAll(stream)
	if err != nil {
		return err
	}
	s.parts[partIndex] = data
	s.storedOrder = append(s.storedOrder, partIndex)
	return nil
}

type fakeCheckpoint struct {
	persisted int
	calls     []int
	failAtArg *int
}

func (c *fakeCheckpoint) save(next int) error {
	c.calls = append(c.calls, next)
	if c.failAtArg != nil && *c.failAtArg == next {
		c.failAtArg = nil
		return errInjected
	}
	c.persisted = next
	return nil
}

const (
	testSnapshotID        = 7
	testChecksumChunkSize = 4096
)

func newTarget(size, maxPartSize uint64, source *string) *diffexport.Target {
	return &diffexport.Target{
		PoolName:           "pool",
		ImageName:          "image",
		SnapshotID:         testSnapshotID,
		SnapshotName:       "target",
		SnapshotSize:       size,
		SourceSnapshotName: source,
		MaxPartSize:        maxPartSize,
		ChecksumChunkSize:  testChecksumChunkSize,
	}
}

func expectedParts(target *diffexport.Target, partCount int) map[int][]byte {
	parts := map[int][]byte{}
	for i := range partCount {
		parts[i] = partContent(&model.ExportDiffInput{
			PoolName:       target.PoolName,
			ImageName:      target.ImageName,
			TargetSnapName: target.SnapshotName,
			FromSnap:       target.SourceSnapshotName,
			ReadOffset:     uint64(i) * target.MaxPartSize,
			ReadLength:     target.MaxPartSize,
			MidSnapPrefix:  target.SnapshotName,
		})
	}
	return parts
}

func TestStore_Success(t *testing.T) {
	source := "source"
	testCases := []struct {
		name          string
		size          uint64
		maxPartSize   uint64
		source        *string
		wantPartCount int
	}{
		{name: "full with a short last part", size: 10, maxPartSize: 4, wantPartCount: 3},
		{name: "incremental with aligned parts", size: 8, maxPartSize: 4, source: &source, wantPartCount: 2},
		{name: "empty snapshot", size: 0, maxPartSize: 4, wantPartCount: 0},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			target := newTarget(tc.size, tc.maxPartSize, tc.source)
			exporter := &fakeExporter{}
			store := newFakePartStore()
			checkpoint := &fakeCheckpoint{}

			// Act
			err := diffexport.Store(exporter, store, target, 0, checkpoint.save)

			// Assert
			require.NoError(t, err)
			assert.Equal(t, expectedParts(target, tc.wantPartCount), store.parts)
			var wantCheckpoints []int
			for i := range tc.wantPartCount {
				wantCheckpoints = append(wantCheckpoints, i+1)
			}
			assert.Equal(t, wantCheckpoints, checkpoint.calls)
			for i, closed := range exporter.closed {
				assert.True(t, *closed, "stream of part %d is not closed", i)
			}
		})
	}
}

func TestStore_StartsFromNext(t *testing.T) {
	// Arrange
	target := newTarget(12, 4, nil)
	exporter := &fakeExporter{}
	store := newFakePartStore()
	checkpoint := &fakeCheckpoint{persisted: 2}

	// Act
	err := diffexport.Store(exporter, store, target, checkpoint.persisted, checkpoint.save)

	// Assert
	require.NoError(t, err)
	require.Len(t, exporter.calls, 1)
	assert.Equal(t, uint64(8), exporter.calls[0].ReadOffset)
	assert.Equal(t, []int{2}, store.storedOrder)
	assert.Equal(t, []int{3}, checkpoint.calls)
}

func TestStore_ResumeAfterFailure(t *testing.T) {
	failingPart := 1
	failingOffset := uint64(4)
	failingCheckpoint := 2
	testCases := []struct {
		name              string
		arrange           func(*fakeExporter, *fakePartStore, *fakeCheckpoint)
		wantFirstStored   []int
		wantFirstCalls    []int
		wantResumedStored []int
	}{
		{
			name:              "export fails",
			arrange:           func(e *fakeExporter, _ *fakePartStore, _ *fakeCheckpoint) { e.failAtOffset = &failingOffset },
			wantFirstStored:   []int{0},
			wantFirstCalls:    []int{1},
			wantResumedStored: []int{1, 2},
		},
		{
			name:              "store fails",
			arrange:           func(_ *fakeExporter, s *fakePartStore, _ *fakeCheckpoint) { s.failAtPart = &failingPart },
			wantFirstStored:   []int{0},
			wantFirstCalls:    []int{1},
			wantResumedStored: []int{1, 2},
		},
		{
			name:              "checkpoint fails after the part is stored",
			arrange:           func(_ *fakeExporter, _ *fakePartStore, c *fakeCheckpoint) { c.failAtArg = &failingCheckpoint },
			wantFirstStored:   []int{0, 1},
			wantFirstCalls:    []int{1, 2},
			wantResumedStored: []int{1, 2},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			target := newTarget(12, 4, nil)
			exporter := &fakeExporter{}
			store := newFakePartStore()
			checkpoint := &fakeCheckpoint{}
			tc.arrange(exporter, store, checkpoint)

			// Act: the first run fails while part 1 is processed.
			err := diffexport.Store(exporter, store, target, 0, checkpoint.save)

			// Assert
			require.ErrorIs(t, err, errInjected)
			assert.Equal(t, tc.wantFirstStored, store.storedOrder)
			assert.Equal(t, tc.wantFirstCalls, checkpoint.calls)
			assert.Equal(t, 1, checkpoint.persisted)

			// Act: a new run resumes from the persisted position.
			store.storedOrder = nil
			err = diffexport.Store(exporter, store, target, checkpoint.persisted, checkpoint.save)

			// Assert
			require.NoError(t, err)
			assert.Equal(t, tc.wantResumedStored, store.storedOrder)
			assert.Equal(t, 3, checkpoint.persisted)
			assert.Equal(t, expectedParts(target, 3), store.parts)
			for i, closed := range exporter.closed {
				assert.True(t, *closed, "stream %d is not closed", i)
			}
		})
	}
}

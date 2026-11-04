package backup

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type diffRecord struct {
	offset, length uint64
	data           []byte // nil for a zero record
}

type diffPart struct {
	fromSnap, toSnap string
	imageSize        uint64
	records          []diffRecord
}

// readDiffPart parses a stored part in the RBD diff v1 format
// (cf. https://docs.ceph.com/en/squid/dev/rbd-diff/).
func readDiffPart(t *testing.T, path string) *diffPart {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	r := bytes.NewReader(raw)

	header := make([]byte, len("rbd diff v1\n"))
	_, err = io.ReadFull(r, header)
	require.NoError(t, err)
	require.Equal(t, "rbd diff v1\n", string(header))

	readU64 := func() uint64 {
		var v uint64
		require.NoError(t, binary.Read(r, binary.LittleEndian, &v))
		return v
	}
	readName := func() string {
		var n int32
		require.NoError(t, binary.Read(r, binary.LittleEndian, &n))
		name := make([]byte, n)
		_, err := io.ReadFull(r, name)
		require.NoError(t, err)
		return string(name)
	}

	part := &diffPart{}
	for {
		tag, err := r.ReadByte()
		require.NoError(t, err, "diff part %s ends without the end record", path)
		switch tag {
		case 'f':
			part.fromSnap = readName()
		case 't':
			part.toSnap = readName()
		case 's':
			part.imageSize = readU64()
		case 'w':
			rec := diffRecord{offset: readU64(), length: readU64()}
			rec.data = make([]byte, rec.length)
			_, err := io.ReadFull(r, rec.data)
			require.NoError(t, err)
			part.records = append(part.records, rec)
		case 'z':
			part.records = append(part.records, diffRecord{offset: readU64(), length: readU64()})
		case 'e':
			require.Zero(t, r.Len(), "trailing bytes after the end record in %s", path)
			return part
		default:
			require.Failf(t, "unknown record", "tag %q in %s", tag, path)
		}
	}
}

// assertDiffPartContents checks the part idx against known volume contents.
// base is the volume the diff starts from (nil for a full backup, i.e. all zeros)
// and target is the volume the diff ends at. Records may carry more than the
// bytes that changed, but every changed byte in the part must be covered and
// every record must carry the target's bytes.
func assertDiffPartContents(
	t *testing.T, path string, idx, partCount int, partSize uint64,
	sourceSnapName, targetSnapName string, base, target []byte,
) {
	t.Helper()

	got := readDiffPart(t, path)

	wantFrom, wantTo := job.CalcSnapshotNamesWithOffset(sourceSnapName, targetSnapName, idx, partCount, partSize)
	assert.Equal(t, wantFrom, got.fromSnap, "part %d from snapshot", idx)
	assert.Equal(t, wantTo, got.toSnap, "part %d to snapshot", idx)
	assert.Equal(t, uint64(len(target)), got.imageSize, "part %d image size", idx)

	partStart := uint64(idx) * partSize
	partEnd := min(partStart+partSize, uint64(len(target)))

	covered := make([]bool, len(target))
	for _, rec := range got.records {
		require.GreaterOrEqual(t, rec.offset, partStart, "part %d record offset", idx)
		require.LessOrEqual(t, rec.offset+rec.length, partEnd, "part %d record end", idx)
		want := target[rec.offset : rec.offset+rec.length]
		if rec.data == nil {
			assert.Equal(t, make([]byte, rec.length), want, "part %d zero record at %d", idx, rec.offset)
		} else {
			assert.Equal(t, want, rec.data, "part %d data record at %d", idx, rec.offset)
		}
		for i := rec.offset; i < rec.offset+rec.length; i++ {
			covered[i] = true
		}
	}

	for i := partStart; i < partEnd; i++ {
		var baseByte byte
		if i < uint64(len(base)) {
			baseByte = base[i]
		}
		if baseByte != target[i] && !covered[i] {
			require.Failf(t, "changed byte not in diff", "part %d: byte %d differs (%#x -> %#x) but no record covers it",
				idx, i, baseByte, target[i])
		}
	}
}

// assertRawImageContents checks that the raw image holds want followed by zero padding.
func assertRawImageContents(t *testing.T, path string, want []byte) {
	t.Helper()

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(got), len(want))
	assert.True(t, bytes.Equal(want, got[:len(want)]), "raw image contents differ from the snapshot")
	assert.Equal(t, make([]byte, len(got)-len(want)), got[len(want):], "raw image padding must be zero")
}

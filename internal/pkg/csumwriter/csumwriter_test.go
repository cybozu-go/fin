package csumwriter_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/cybozu-go/fin/internal/pkg/csum"
	"github.com/cybozu-go/fin/internal/pkg/csumwriter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChecksumWriter(t *testing.T) {
	// Description:
	// Check that ChecksumWriter correctly writes data and generates checksums for various data lengths.
	// Tests multiple scenarios: chunk-aligned data and non-aligned data.
	//
	// Arrange:
	// - Test data of various lengths relative to chunk size
	// - ChecksumWriter with buffers for data and checksums
	//
	// Act:
	// Write all data using ChecksumWriter, then close
	//
	// Assert:
	// - Data should be written correctly to data buffer
	// - Checksums should be generated correctly for all chunks

	// Arrange: use a fixed chunk size and test multiple data lengths
	chunkSize := 4
	cases := []struct {
		name string
		data []byte
	}{
		{name: "chunk_aligned", data: []byte("abcdefgh")}, // 8 bytes = 2 chunks (4 bytes each)
		{name: "non_aligned", data: []byte("abcdefg")},    // 7 bytes = 1 full chunk + 1 partial chunk
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			dataBuffer := &bytes.Buffer{}
			checksumBuffer := &bytes.Buffer{}
			cw := csumwriter.NewChecksumWriter(dataBuffer, checksumBuffer, chunkSize)

			// Act
			n, err := cw.Write(tc.data)
			require.NoError(t, err)
			assert.Equal(t, len(tc.data), n)

			err = cw.Close()
			require.NoError(t, err)

			// Assert
			assert.Equal(t, tc.data, dataBuffer.Bytes())

			numChunks := (len(tc.data) + chunkSize - 1) / chunkSize
			expectedChecksums := make([]uint64, numChunks)
			for i := 0; i < numChunks; i++ {
				start := i * chunkSize
				end := start + chunkSize
				if end > len(tc.data) {
					end = len(tc.data)
				}
				expectedChecksums[i] = xxhash.Sum64(tc.data[start:end])
			}

			checksumBytes := checksumBuffer.Bytes()
			assert.Equal(t, len(expectedChecksums)*csum.ChecksumLen, len(checksumBytes))

			for i, expectedChecksum := range expectedChecksums {
				offset := i * csum.ChecksumLen
				actualChecksum := binary.LittleEndian.Uint64(checksumBytes[offset : offset+csum.ChecksumLen])
				assert.Equal(t, expectedChecksum, actualChecksum)
			}
		})
	}
}

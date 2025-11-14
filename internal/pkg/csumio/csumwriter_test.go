package csumio_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/cybozu-go/fin/internal/pkg/csumio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChecksumWriter(t *testing.T) {
	// Description:
	// Check that ChecksumWriter correctly writes data and generates checksums for various data lengths.
	// Tests multiple scenarios: chunk-aligned data and non-aligned data.
	//
	// Arrange:
	// - Prepare test data of various lengths relative to the chunk size
	// - Create a ChecksumWriter with buffers for data and checksums
	//
	// Act:
	// Write all data using ChecksumWriter, then close
	//
	// Assert:
	// - Data should be written correctly to data buffer
	// - Checksums should be generated correctly for all chunks

	chunkSize := csumio.MinimumChunkSize
	cases := []struct {
		name      string
		data      []byte
		writeSize int // if > 0, write in multiple small chunks of this size
	}{
		{name: "chunk_aligned", data: bytes.Repeat([]byte("a"), chunkSize)},
		{name: "non_aligned_one_less", data: bytes.Repeat([]byte("a"), chunkSize-1)},
		{name: "non_aligned_one_more", data: bytes.Repeat([]byte("a"), chunkSize+1)},
		{name: "two_chunks_aligned", data: bytes.Repeat([]byte("a"), chunkSize*2)},
		{name: "small_writes_one_chunk", data: bytes.Repeat([]byte("a"), chunkSize), writeSize: 512},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			dataBuffer := &bytes.Buffer{}
			checksumBuffer := &bytes.Buffer{}
			cw, err := csumio.NewChecksumWriter(dataBuffer, checksumBuffer, chunkSize)
			require.NoError(t, err)

			// Act
			totalWritten := 0
			if tc.writeSize > 0 {
				// Write in multiple small chunks
				for offset := 0; offset < len(tc.data); offset += tc.writeSize {
					end := offset + tc.writeSize
					if end > len(tc.data) {
						end = len(tc.data)
					}
					n, err := cw.Write(tc.data[offset:end])
					require.NoError(t, err)
					totalWritten += n
				}
			} else {
				// Write all at once
				n, err := cw.Write(tc.data)
				require.NoError(t, err)
				totalWritten = n
			}
			assert.Equal(t, len(tc.data), totalWritten)

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
			assert.Equal(t, len(expectedChecksums)*csumio.ChecksumLen, len(checksumBytes))

			for i, expectedChecksum := range expectedChecksums {
				offset := i * csumio.ChecksumLen
				actualChecksum := binary.LittleEndian.Uint64(checksumBytes[offset : offset+csumio.ChecksumLen])
				assert.Equal(t, expectedChecksum, actualChecksum)
			}
		})
	}
}

package csumio_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cybozu-go/fin/internal/pkg/csumio"
)

func TestReader_ChecksumMismatch_VerificationEnabled(t *testing.T) {
	// Description:
	// Check that checksum verification detects checksum mismatch when verification is enabled
	//
	// Arrange:
	// - Data file contains test data
	// - Checksum file contains checksum for different data (intentionally wrong)
	// - Checksum verification is enabled
	//
	// Act:
	// Read data using Reader
	//
	// Assert:
	// Should fail with ErrChecksumMismatch

	chunkSize := csumio.MinimumChunkSize
	cases := []struct {
		name string
		data []byte
	}{
		{name: "exact_chunk", data: bytes.Repeat([]byte("a"), chunkSize)},          // exactly one chunk
		{name: "partial_chunk", data: bytes.Repeat([]byte("a"), chunkSize-1)},      // less than one chunk
		{name: "more_than_chunk", data: bytes.Repeat([]byte("a"), chunkSize+1)},    // more than one chunk
		{name: "two_chunks_aligned", data: bytes.Repeat([]byte("a"), chunkSize*2)}, // exactly two chunks
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			wrongChecksum := uint64(0xdeadbeef) // Intentionally wrong checksum
			checksumBuf := make([]byte, csumio.ChecksumLen)
			binary.LittleEndian.PutUint64(checksumBuf, wrongChecksum)
			dataReader := bytes.NewReader(tc.data)
			checksumReader := bytes.NewReader(checksumBuf)

			// Act
			reader, err := csumio.NewReader(dataReader, checksumReader, chunkSize, true)
			require.NoError(t, err)

			buf := make([]byte, chunkSize)
			_, err = reader.Read(buf)
			// Assert
			assert.Error(t, err)
			assert.ErrorIs(t, err, csumio.ErrChecksumMismatch)
		})
	}
}

func TestReader_ChecksumMismatch_VerificationDisabled(t *testing.T) {
	// Description:
	// Check that checksum verification is ignored when verification is disabled
	//
	// Arrange:
	// - Data file contains test data
	// - Checksum file contains checksum for different data (intentionally wrong)
	// - Checksum verification is disabled
	//
	// Act:
	// Read data using Reader
	//
	// Assert:
	// Should succeed and return correct data despite wrong checksum

	chunkSize := csumio.MinimumChunkSize
	cases := []struct {
		name string
		data []byte
	}{
		{name: "exact_chunk", data: bytes.Repeat([]byte("a"), chunkSize)},          // exactly one chunk
		{name: "partial_chunk", data: bytes.Repeat([]byte("a"), chunkSize-1)},      // less than one chunk
		{name: "more_than_chunk", data: bytes.Repeat([]byte("a"), chunkSize+1)},    // more than one chunk
		{name: "two_chunks_aligned", data: bytes.Repeat([]byte("a"), chunkSize*2)}, // exactly two chunks
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			wrongChecksum := uint64(0xdeadbeef) // Intentionally wrong checksum
			checksumBuf := make([]byte, csumio.ChecksumLen)
			binary.LittleEndian.PutUint64(checksumBuf, wrongChecksum)
			dataReader := bytes.NewReader(tc.data)
			checksumReader := bytes.NewReader(checksumBuf)

			// Act
			reader, err := csumio.NewReader(dataReader, checksumReader, chunkSize, false)
			require.NoError(t, err)

			buf := make([]byte, len(tc.data))
			_, err = io.ReadFull(reader, buf)

			// Assert
			assert.NoError(t, err)
			assert.Equal(t, tc.data, buf)
		})
	}
}

func TestReader_CorrectChecksum_VerificationEnabled(t *testing.T) {
	// Description:
	// Check that checksum verification succeeds when checksums match and verification is enabled.
	// Tests multiple scenarios: exact chunk, partial chunk, and multiple chunks.
	//
	// Arrange:
	// - Data file contains test data of various lengths relative to chunk size
	// - Checksum file contains correct checksums for all chunks in the data
	// - Checksum verification is enabled
	//
	// Act:
	// Read all data using Reader sequentially, then attempt one more read
	//
	// Assert:
	// - All chunk reads should succeed and return correct data
	// - Final read should return EOF

	chunkSize := csumio.MinimumChunkSize
	cases := []struct {
		name string
		data []byte
	}{
		{name: "exact_chunk", data: bytes.Repeat([]byte("a"), chunkSize)},          // exactly one chunk
		{name: "partial_chunk", data: bytes.Repeat([]byte("a"), chunkSize-1)},      // less than one chunk
		{name: "more_than_chunk", data: bytes.Repeat([]byte("a"), chunkSize+1)},    // more than one chunk
		{name: "two_chunks_aligned", data: bytes.Repeat([]byte("a"), chunkSize*2)}, // exactly two chunks
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			numChunks := (len(tc.data) + chunkSize - 1) / chunkSize
			checksumBytes := make([]byte, 0, numChunks*csumio.ChecksumLen)
			for i := 0; i < numChunks; i++ {
				start := i * chunkSize
				end := start + chunkSize
				if end > len(tc.data) {
					end = len(tc.data)
				}
				cs := xxhash.Sum64(tc.data[start:end])
				b := make([]byte, csumio.ChecksumLen)
				binary.LittleEndian.PutUint64(b, cs)
				checksumBytes = append(checksumBytes, b...)
			}

			dataReader := bytes.NewReader(tc.data)
			checksumReader := bytes.NewReader(checksumBytes)

			reader, err := csumio.NewReader(dataReader, checksumReader, chunkSize, true)
			require.NoError(t, err)
			buf := make([]byte, chunkSize)

			for i := 0; i < numChunks; i++ {
				// Act
				n, err := reader.Read(buf)

				// Assert
				assert.NoError(t, err)
				expectedLen := chunkSize
				if (i+1)*chunkSize > len(tc.data) {
					expectedLen = len(tc.data) - i*chunkSize
				}

				start := i * chunkSize
				assert.Equal(t, expectedLen, n)
				assert.Equal(t, tc.data[start:start+expectedLen], buf[:n])
			}

			n, err := reader.Read(buf)
			assert.Equal(t, 0, n)
			assert.Equal(t, io.EOF, err)
		})
	}
}

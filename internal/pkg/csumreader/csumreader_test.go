package csumreader_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"

	"github.com/cybozu-go/fin/internal/pkg/csum"
	"github.com/cybozu-go/fin/internal/pkg/csumreader"
)

func TestChecksumReader_ChecksumMismatch_VerificationEnabled(t *testing.T) {
	// Description:
	// Check that checksum verification detects mismatched checksums when verification is enabled
	//
	// Arrange:
	// - Data file contains test data
	// - Checksum file contains checksum for different data (intentionally wrong)
	// - Checksum verification is enabled
	//
	// Act:
	// Read data using ChecksumReader
	//
	// Assert:
	// Should fail with ErrChecksumMismatch

	// Arrange
	data := []byte("test data")
	chunkSize := len(data)
	wrongData := []byte("wrong data")
	wrongChecksum := xxhash.Sum64(wrongData)
	checksumBuf := make([]byte, csum.ChecksumLen)
	binary.LittleEndian.PutUint64(checksumBuf, wrongChecksum)
	dataReader := bytes.NewReader(data)
	checksumReader := bytes.NewReader(checksumBuf)

	// Act
	reader := csumreader.NewChecksumReader(dataReader, checksumReader, chunkSize, false)

	buf := make([]byte, chunkSize)
	_, err := reader.Read(buf)

	// Assert
	assert.Error(t, err)
	assert.True(t, errors.Is(err, csumreader.ErrChecksumMismatch))
}

func TestChecksumReader_ChecksumMismatch_VerificationDisabled(t *testing.T) {
	// Description:
	// Check that checksum verification ignores mismatched checksums when verification is disabled
	//
	// Arrange:
	// - Data file contains test data
	// - Checksum file contains checksum for different data (intentionally wrong)
	// - Checksum verification is disabled
	//
	// Act:
	// Read data using ChecksumReader
	//
	// Assert:
	// Should succeed and return correct data despite wrong checksum

	// Arrange
	data := []byte("test data")
	chunkSize := len(data)
	wrongData := []byte("wrong data")
	wrongChecksum := xxhash.Sum64(wrongData)
	checksumBuf := make([]byte, csum.ChecksumLen)
	binary.LittleEndian.PutUint64(checksumBuf, wrongChecksum)
	dataReader := bytes.NewReader(data)
	checksumReader := bytes.NewReader(checksumBuf)

	// Act
	reader := csumreader.NewChecksumReader(dataReader, checksumReader, chunkSize, true)

	buf := make([]byte, chunkSize)
	n, err := reader.Read(buf)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, chunkSize, n)
	assert.Equal(t, data, buf[:n])
}

func TestChecksumReader_CorrectChecksum_VerificationEnabled(t *testing.T) {
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
	// Read all data using ChecksumReader sequentially, then attempt one more read
	//
	// Assert:
	// - All chunk reads should succeed and return correct data
	// - Final read should return EOF

	// Arrange: use a fixed chunk size and test multiple data lengths
	chunkSize := 8
	cases := []struct {
		name string
		data []byte
	}{
		{name: "exact_chunk", data: []byte("abcdefgh")},       // exactly one chunk
		{name: "partial_chunk", data: []byte("abcdefg")},      // less than one chunk
		{name: "more_than_chunk", data: []byte("abcdefghij")}, // more than one chunk
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			numChunks := (len(tc.data) + chunkSize - 1) / chunkSize
			checksumBytes := make([]byte, 0, numChunks*csum.ChecksumLen)
			for i := 0; i < numChunks; i++ {
				start := i * chunkSize
				end := start + chunkSize
				if end > len(tc.data) {
					end = len(tc.data)
				}
				cs := xxhash.Sum64(tc.data[start:end])
				b := make([]byte, csum.ChecksumLen)
				binary.LittleEndian.PutUint64(b, cs)
				checksumBytes = append(checksumBytes, b...)
			}

			dataReader := bytes.NewReader(tc.data)
			checksumReader := bytes.NewReader(checksumBytes)

			// Act
			reader := csumreader.NewChecksumReader(dataReader, checksumReader, chunkSize, false)
			buf := make([]byte, chunkSize)

			// Assert
			for i := 0; i < numChunks; i++ {
				n, err := reader.Read(buf)
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

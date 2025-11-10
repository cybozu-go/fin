package csumreader

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
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
	checksumBuf := make([]byte, ChecksumLen)
	binary.LittleEndian.PutUint64(checksumBuf, wrongChecksum)
	dataReader := bytes.NewReader(data)
	checksumReader := bytes.NewReader(checksumBuf)

	// Act
	reader := NewChecksumReader(dataReader, checksumReader, chunkSize, false)

	buf := make([]byte, chunkSize)
	_, err := reader.Read(buf)

	// Assert
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrChecksumMismatch))
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
	checksumBuf := make([]byte, ChecksumLen)
	binary.LittleEndian.PutUint64(checksumBuf, wrongChecksum)
	dataReader := bytes.NewReader(data)
	checksumReader := bytes.NewReader(checksumBuf)

	// Act
	reader := NewChecksumReader(dataReader, checksumReader, chunkSize, true)

	buf := make([]byte, chunkSize)
	n, err := reader.Read(buf)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, chunkSize, n)
	assert.Equal(t, data, buf[:n])
}

func TestChecksumReader_CorrectChecksum_VerificationEnabled(t *testing.T) {
	// Description:
	// Check that checksum verification succeeds when checksums match and verification is enabled
	//
	// Arrange:
	// - Data file contains test data
	// - Checksum file contains correct checksum for the test data
	// - Checksum verification is enabled
	//
	// Act:
	// Read data using ChecksumReader
	//
	// Assert:
	// Should succeed and return correct data

	// Arrange
	data := []byte("test data")
	chunkSize := len(data)
	correctChecksum := xxhash.Sum64(data)
	checksumBuf := make([]byte, ChecksumLen)
	binary.LittleEndian.PutUint64(checksumBuf, correctChecksum)
	dataReader := bytes.NewReader(data)
	checksumReader := bytes.NewReader(checksumBuf)

	// Act
	reader := NewChecksumReader(dataReader, checksumReader, chunkSize, false)
	buf := make([]byte, chunkSize)
	n, err := reader.Read(buf)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, chunkSize, n)
	assert.Equal(t, data, buf[:n])
}

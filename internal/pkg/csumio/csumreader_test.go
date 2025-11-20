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

// TestReader_ReadFactorCoverage exhaustively validates csumio.Reader.Read.
// The test matrix covers every combination across three factors:
//
//	Factor A: requested len(p) relative to chunkSize (> chunk / == chunk / < chunk).
//	Factor B: remaining unread bytes in `dataReader` relative to len(p) (> p / == p / < p).
//	Factor C: inner-buffer state (empty vs. partially filled by a prior read).
//
// These three factors yield 18 cases (case01–case18). Each entry sets the total payload size,
// pre-consumes bytes when needed to shape the inner buffer, and issues a single Read with
// len(p) = chunkSize±1 or chunkSize so the call experiences the intended combination.
func TestReader_ReadFactorCoverage(t *testing.T) {
	chunkSize := csumio.MinimumChunkSize

	type bufferLenRelation int
	const (
		bufferLenGreaterThanChunk bufferLenRelation = iota
		bufferLenEqualToChunk
		bufferLenSmallerThanChunk
	)

	type remainingRelation int
	const (
		remainingDataGreaterThanBuffer remainingRelation = iota
		remainingDataEqualToBuffer
		remainingDataLessThanBuffer
	)

	type matrixCase struct {
		name               string
		bufferLen          bufferLenRelation
		remainingData      remainingRelation
		isInnerBufferEmpty bool
	}

	// Helper function to calculate parameters for each tests from test factors.
	calculateParams := func(tc matrixCase, chunkSize int) (readLen, prefillLen, initialDataReaderLen int) {
		halfChunk := chunkSize / 2
		extraMargin := halfChunk

		// Buffer Length: readLen relative to chunkSize
		switch tc.bufferLen {
		case bufferLenGreaterThanChunk:
			readLen = chunkSize + 1
		case bufferLenEqualToChunk:
			readLen = chunkSize
		case bufferLenSmallerThanChunk:
			readLen = chunkSize - 1
		}

		// Inner Buffer Empty: preConsume based on reader's inner buffer state
		switch tc.isInnerBufferEmpty {
		case true:
			prefillLen = 0
		case false:
			prefillLen = halfChunk
		}

		// Remaining Data: totalLen based on remaining bytes relative to readLen
		initialDataReaderLen = prefillLen + readLen
		switch tc.remainingData {
		case remainingDataGreaterThanBuffer:
			initialDataReaderLen += extraMargin
		case remainingDataLessThanBuffer:
			initialDataReaderLen -= 1
		}

		return readLen, prefillLen, initialDataReaderLen
	}

	cases := []matrixCase{
		{
			name:               "case01_pGreater_remainingGreater_innerBufferEmpty",
			bufferLen:          bufferLenGreaterThanChunk,
			remainingData:      remainingDataGreaterThanBuffer,
			isInnerBufferEmpty: true,
		},
		{
			name:               "case02_pGreater_remainingGreater_innerBufferPartial",
			bufferLen:          bufferLenGreaterThanChunk,
			remainingData:      remainingDataGreaterThanBuffer,
			isInnerBufferEmpty: false,
		},
		{
			name:               "case03_pGreater_remainingEqual_innerBufferEmpty",
			bufferLen:          bufferLenGreaterThanChunk,
			remainingData:      remainingDataEqualToBuffer,
			isInnerBufferEmpty: true,
		},
		{
			name:               "case04_pGreater_remainingEqual_innerBufferPartial",
			bufferLen:          bufferLenGreaterThanChunk,
			remainingData:      remainingDataEqualToBuffer,
			isInnerBufferEmpty: false,
		},
		{
			name:               "case05_pGreater_remainingLess_innerBufferEmpty",
			bufferLen:          bufferLenGreaterThanChunk,
			remainingData:      remainingDataLessThanBuffer,
			isInnerBufferEmpty: true,
		},
		{
			name:               "case06_pGreater_remainingLess_innerBufferPartial",
			bufferLen:          bufferLenGreaterThanChunk,
			remainingData:      remainingDataLessThanBuffer,
			isInnerBufferEmpty: false,
		},
		{
			name:               "case07_pEqual_remainingGreater_innerBufferEmpty",
			bufferLen:          bufferLenEqualToChunk,
			remainingData:      remainingDataGreaterThanBuffer,
			isInnerBufferEmpty: true,
		},
		{
			name:               "case08_pEqual_remainingGreater_innerBufferPartial",
			bufferLen:          bufferLenEqualToChunk,
			remainingData:      remainingDataGreaterThanBuffer,
			isInnerBufferEmpty: false,
		},
		{
			name:               "case09_pEqual_remainingEqual_innerBufferEmpty",
			bufferLen:          bufferLenEqualToChunk,
			remainingData:      remainingDataEqualToBuffer,
			isInnerBufferEmpty: true,
		},
		{
			name:               "case10_pEqual_remainingEqual_innerBufferPartial",
			bufferLen:          bufferLenEqualToChunk,
			remainingData:      remainingDataEqualToBuffer,
			isInnerBufferEmpty: false,
		},
		{
			name:               "case11_pEqual_remainingLess_innerBufferEmpty",
			bufferLen:          bufferLenEqualToChunk,
			remainingData:      remainingDataLessThanBuffer,
			isInnerBufferEmpty: true,
		},
		{
			name:               "case12_pEqual_remainingLess_innerBufferPartial",
			bufferLen:          bufferLenEqualToChunk,
			remainingData:      remainingDataLessThanBuffer,
			isInnerBufferEmpty: false,
		},
		{
			name:               "case13_pSmaller_remainingGreater_innerBufferEmpty",
			bufferLen:          bufferLenSmallerThanChunk,
			remainingData:      remainingDataGreaterThanBuffer,
			isInnerBufferEmpty: true,
		},
		{
			name:               "case14_pSmaller_remainingGreater_innerBufferPartial",
			bufferLen:          bufferLenSmallerThanChunk,
			remainingData:      remainingDataGreaterThanBuffer,
			isInnerBufferEmpty: false,
		},
		{
			name:               "case15_pSmaller_remainingEqual_innerBufferEmpty",
			bufferLen:          bufferLenSmallerThanChunk,
			remainingData:      remainingDataEqualToBuffer,
			isInnerBufferEmpty: true,
		},
		{
			name:               "case16_pSmaller_remainingEqual_innerBufferPartial",
			bufferLen:          bufferLenSmallerThanChunk,
			remainingData:      remainingDataEqualToBuffer,
			isInnerBufferEmpty: false,
		},
		{
			name:               "case17_pSmaller_remainingLess_innerBufferEmpty",
			bufferLen:          bufferLenSmallerThanChunk,
			remainingData:      remainingDataLessThanBuffer,
			isInnerBufferEmpty: true,
		},
		{
			name:               "case18_pSmaller_remainingLess_innerBufferPartial",
			bufferLen:          bufferLenSmallerThanChunk,
			remainingData:      remainingDataLessThanBuffer,
			isInnerBufferEmpty: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			readLen, preConsume, totalLen := calculateParams(tc, chunkSize)

			// Arrange
			data := sequentialBytes(totalLen)
			checksumBytes := checksumBytesForData(data, chunkSize)
			reader, err := csumio.NewReader(bytes.NewReader(data), bytes.NewReader(checksumBytes), chunkSize, true)
			require.NoError(t, err)

			if preConsume > 0 {
				_, err := io.CopyN(io.Discard, reader, int64(preConsume))
				require.NoError(t, err)
			}

			remaining := totalLen - preConsume
			require.Greater(t, remaining, 0, "remaining bytes must not be zero")

			chunkStart := (preConsume / chunkSize) * chunkSize
			chunkEnd := min(chunkStart+chunkSize, totalLen)
			currentChunkRemaining := chunkEnd - preConsume
			require.Greater(t, currentChunkRemaining, 0, "current chunk must have bytes remaining")
			expected := min(readLen, currentChunkRemaining)

			// Act
			buf := make([]byte, readLen)
			n, err := reader.Read(buf)
			require.NoError(t, err)

			// Assert
			assert.Equal(t, expected, n)
			start := preConsume
			assert.Equal(t, data[start:start+expected], buf[:n])

			if start+expected == totalLen {
				n2, err := reader.Read(buf)
				assert.Equal(t, 0, n2)
				assert.Equal(t, io.EOF, err)
			}
		})
	}
}

// sequentialBytes fills each chunk with its chunk number (e.g. chunk 0 is
// "0000...", chunk 1 is "1111..."). This guarantees that every chunk hashes
// differently while keeping the payload easy to read.
func sequentialBytes(length int) []byte {
	if length <= 0 {
		return nil
	}

	data := make([]byte, length)
	chunkSize := csumio.MinimumChunkSize

	for offset := 0; offset < length; offset += chunkSize {
		chunkIndex := offset / chunkSize
		chunkByte := byte('0' + (chunkIndex % 10))

		chunkEnd := offset + chunkSize
		if chunkEnd > length {
			chunkEnd = length
		}
		for i := offset; i < chunkEnd; i++ {
			data[i] = chunkByte
		}
	}

	return data
}

func checksumBytesForData(data []byte, chunkSize int) []byte {
	numChunks := (len(data) + chunkSize - 1) / chunkSize
	checksumBytes := make([]byte, 0, numChunks*csumio.ChecksumLen)
	for i := 0; i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		sum := xxhash.Sum64(data[start:end])
		block := make([]byte, csumio.ChecksumLen)
		binary.LittleEndian.PutUint64(block, sum)
		checksumBytes = append(checksumBytes, block...)
	}
	return checksumBytes
}

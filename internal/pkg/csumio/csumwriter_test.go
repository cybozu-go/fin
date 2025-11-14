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

func TestWriter_Success(t *testing.T) {
	// Description:
	// Check that Writer correctly writes data and generates checksums for various data lengths.
	// Tests multiple scenarios: chunk-aligned data and non-aligned data.
	//
	// Arrange:
	// - Prepare test data of various lengths relative to the chunk size
	// - Create a Writer with buffers for data and checksums
	//
	// Act:
	// Write all data using Writer, then close
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
			cw, err := csumio.NewWriter(dataBuffer, checksumBuffer, chunkSize)
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

// TestWriter_WriteFactorCoverage exhaustively validates csumio.Writer.Write.
// The test matrix covers three factors for a single Write call:
//
//	Factor A: buffer state before Write (empty vs. prefilled).
//	Factor B: buffer state after Write (empty vs. non-empty).
//	Factor C: whether the Write spans two chunks (yes vs. no).
//
// These factors yield eight cases (case01–case08). Each entry chooses prefill and write
// lengths so the targeted combination is realized, then verifies written chunks and checksums.
func TestWriter_WriteFactorCoverage(t *testing.T) {
	chunkSize := csumio.MinimumChunkSize

	type matrixCase struct {
		name                   string
		bufferEmptyBeforeWrite bool
		bufferEmptyAfterWrite  bool
		spansTwoChunks         bool
	}

	getExpectedLengths := func(parts ...int) []int {
		total := 0
		for _, length := range parts {
			total += length
		}
		fullChunks := total / chunkSize
		remaining := total % chunkSize

		chunks := make([]int, 0, fullChunks+1)
		for i := 0; i < fullChunks; i++ {
			chunks = append(chunks, chunkSize)
		}
		if remaining > 0 {
			chunks = append(chunks, remaining)
		}
		return chunks
	}

	calculateParams := func(tc matrixCase, chunkSize int) (prefillLen, writeLen int) {
		twoChunk := chunkSize * 2
		halfChunk := chunkSize / 2
		quarterChunk := chunkSize / 4

		if !tc.bufferEmptyBeforeWrite {
			prefillLen = halfChunk
		}

		switch {
		case tc.bufferEmptyAfterWrite && tc.spansTwoChunks:
			writeLen = twoChunk - prefillLen
		case !tc.bufferEmptyAfterWrite && tc.spansTwoChunks:
			writeLen = chunkSize + halfChunk - prefillLen
		case tc.bufferEmptyAfterWrite && !tc.spansTwoChunks:
			writeLen = chunkSize - prefillLen
		case !tc.bufferEmptyAfterWrite && !tc.spansTwoChunks:
			writeLen = chunkSize - quarterChunk - prefillLen
		}

		return prefillLen, writeLen
	}

	cases := []matrixCase{
		{
			name:                   "case01_bufferEmptyBefore_bufferEmptyAfter_span",
			bufferEmptyBeforeWrite: true,
			bufferEmptyAfterWrite:  true,
			spansTwoChunks:         true,
		},
		{
			name:                   "case02_bufferEmptyBefore_bufferEmptyAfter_noSpan",
			bufferEmptyBeforeWrite: true,
			bufferEmptyAfterWrite:  true,
			spansTwoChunks:         false,
		},
		{
			name:                   "case03_bufferEmptyBefore_bufferNotEmptyAfter_span",
			bufferEmptyBeforeWrite: true,
			bufferEmptyAfterWrite:  false,
			spansTwoChunks:         true,
		},
		{
			name:                   "case04_bufferEmptyBefore_bufferNotEmptyAfter_noSpan",
			bufferEmptyBeforeWrite: true,
			bufferEmptyAfterWrite:  false,
			spansTwoChunks:         false,
		},
		{
			name:                   "case05_bufferNotEmptyBefore_bufferEmptyAfter_span",
			bufferEmptyBeforeWrite: false,
			bufferEmptyAfterWrite:  true,
			spansTwoChunks:         true,
		},
		{
			name:                   "case06_bufferNotEmptyBefore_bufferEmptyAfter_noSpan",
			bufferEmptyBeforeWrite: false,
			bufferEmptyAfterWrite:  true,
			spansTwoChunks:         false,
		},
		{
			name:                   "case07_bufferNotEmptyBefore_bufferNotEmptyAfter_span",
			bufferEmptyBeforeWrite: false,
			bufferEmptyAfterWrite:  false,
			spansTwoChunks:         true,
		},
		{
			name:                   "case08_bufferNotEmptyBefore_bufferNotEmptyAfter_noSpan",
			bufferEmptyBeforeWrite: false,
			bufferEmptyAfterWrite:  false,
			spansTwoChunks:         false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prefillLen, writeLen := calculateParams(tc, chunkSize)

			recorder := &writeRecorder{}
			checksumBuffer := &bytes.Buffer{}
			cw, err := csumio.NewWriter(recorder, checksumBuffer, chunkSize)
			require.NoError(t, err)

			var currentDataByte byte
			expectedData := make([]byte, 0, prefillLen+writeLen)

			write := func(length int) {
				data := bytes.Repeat([]byte{currentDataByte}, length)
				currentDataByte++
				n, err := cw.Write(data)
				require.NoError(t, err)
				require.Equal(t, length, n)
				expectedData = append(expectedData, data...)
			}

			if prefillLen > 0 {
				write(prefillLen)
			}

			//Act
			write(writeLen)
			err = cw.Close()
			require.NoError(t, err)

			// Assert
			require.Equal(t, expectedData, recorder.getBytes())

			expectedLengths := getExpectedLengths(prefillLen, writeLen)
			actualLengths := recorder.getLengths()
			require.Equal(t, expectedLengths, actualLengths)

			assertChecksumsMatch(t, recorder, checksumBuffer)
		})
	}
}

// writeRecorder records the write history. We need this struct because the write unit is important here.
type writeRecorder struct {
	chunks [][]byte
}

func (cr *writeRecorder) Write(p []byte) (int, error) {
	cr.chunks = append(cr.chunks, bytes.Clone(p))
	return len(p), nil
}

func (cr *writeRecorder) getBytes() []byte {
	return bytes.Join(cr.chunks, nil)
}

func (cr *writeRecorder) getLengths() []int {
	lens := make([]int, len(cr.chunks))
	for i, w := range cr.chunks {
		lens[i] = len(w)
	}
	return lens
}

func (cr *writeRecorder) getChunks() [][]byte {
	return cr.chunks
}

func assertChecksumsMatch(t *testing.T, recorder *writeRecorder, checksumBuffer *bytes.Buffer) {
	t.Helper()
	chunks := recorder.getChunks()
	checksumBytes := checksumBuffer.Bytes()
	require.Equal(t, len(chunks)*csumio.ChecksumLen, len(checksumBytes))
	for i, chunk := range chunks {
		expected := xxhash.Sum64(chunk)
		offset := i * csumio.ChecksumLen
		actual := binary.LittleEndian.Uint64(checksumBytes[offset : offset+csumio.ChecksumLen])
		assert.Equal(t, expected, actual)
	}
}

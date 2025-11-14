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
// The test matrix uses pairwise coverage across three factors:
//
//	Factor A: requested write length p relative to chunkSize (> chunk / == chunk / < chunk).
//	Factor B: remaining buffer capacity before Write relative to p (> p / == p / < p).
//	Factor C: offset state (no chunk flushed yet vs. at least one chunk flushed).
//
// These three factors yield nine cases. Each entry below lists one combination, drives chunkSize±1
// boundary writes, and approximates “capacity > p” with an empty buffer when p > chunkSize.
func TestWriter_WriteFactorCoverage(t *testing.T) {
	chunkSize := csumio.MinimumChunkSize
	require.Greater(t, chunkSize, 2, "chunkSize must exceed 2 for ±1 boundary writes")

	pGreater := chunkSize + 1 // Represent p > chunkSize by overshooting by 1 byte.
	pEqual := chunkSize       // Exact chunk boundary.
	pSmaller := chunkSize - 1 // Represent p < chunkSize by undershooting by 1 byte.

	almostFull := chunkSize - 1    // Leave only 1 byte of remaining capacity (capacity < p).
	twoBytesShort := chunkSize - 2 // Expect chunk-2 tail during Close.
	singleByte := 1                // Use single-byte writes to control remaining capacity precisely.

	type matrixCase struct {
		name              string
		description       string
		preLens           []int
		targetLen         int
		expectedChunkLens []int
	}

	cases := []matrixCase{
		{
			name:        "pGreater_capacityChunk_offsetZero",
			description: "p > chunk, capacity == chunk, offset zero",
			preLens:     nil,
			targetLen:   pGreater,
			expectedChunkLens: []int{
				chunkSize,
				singleByte,
			},
		},
		{
			name:        "pGreater_capacityChunk_offsetNonZero",
			description: "p > chunk, capacity == chunk, offset non-zero",
			preLens: []int{
				chunkSize,
			},
			targetLen: pGreater,
			expectedChunkLens: []int{
				chunkSize,
				chunkSize,
				singleByte,
			},
		},
		{
			name:        "pGreater_capacityLess_offsetZero",
			description: "p > chunk, capacity < p, offset zero",
			preLens: []int{
				almostFull,
			},
			targetLen: pGreater,
			expectedChunkLens: []int{
				chunkSize,
				chunkSize,
			},
		},
		{
			name:        "pEqual_capacityChunk_offsetNonZero",
			description: "p == chunk, capacity == chunk, offset non-zero",
			preLens: []int{
				chunkSize,
			},
			targetLen: pEqual,
			expectedChunkLens: []int{
				chunkSize,
				chunkSize,
			},
		},
		{
			name:        "pEqual_capacityEqual_offsetZero",
			description: "p == chunk, capacity == p, offset zero",
			preLens:     nil,
			targetLen:   pEqual,
			expectedChunkLens: []int{
				chunkSize,
			},
		},
		{
			name:        "pEqual_capacityLess_offsetNonZero",
			description: "p == chunk, capacity < p, offset non-zero",
			preLens: []int{
				chunkSize,
				almostFull,
			},
			targetLen: pEqual,
			expectedChunkLens: []int{
				chunkSize,
				chunkSize,
				almostFull,
			},
		},
		{
			name:        "pSmaller_capacityGreater_offsetZero",
			description: "p < chunk, capacity > p, offset zero",
			preLens:     nil,
			targetLen:   pSmaller,
			expectedChunkLens: []int{
				pSmaller,
			},
		},
		{
			name:        "pSmaller_capacityEqual_offsetNonZero",
			description: "p < chunk, capacity == p, offset non-zero",
			preLens: []int{
				chunkSize,
				singleByte,
			},
			targetLen: pSmaller,
			expectedChunkLens: []int{
				chunkSize,
				chunkSize,
			},
		},
		{
			name:        "pSmaller_capacityLess_offsetZero",
			description: "p < chunk, capacity < p, offset zero",
			preLens: []int{
				almostFull,
			},
			targetLen: pSmaller,
			expectedChunkLens: []int{
				chunkSize,
				twoBytesShort,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			dataRecorder := &chunkRecorder{}
			checksumBuffer := &bytes.Buffer{}
			cw, err := csumio.NewWriter(dataRecorder, checksumBuffer, chunkSize)
			require.NoError(t, err)

			currentDataByte := 0
			expectedData := make([]byte, 0)

			write := func(length int) {
				data := bytes.Repeat([]byte{byte(currentDataByte)}, length)
				currentDataByte++
				n, err := cw.Write(data)
				require.NoError(t, err)
				require.Equal(t, length, n)
				expectedData = append(expectedData, data...)
			}

			for _, preLen := range tc.preLens {
				write(preLen)
			}

			// Act
			write(tc.targetLen)
			err = cw.Close()
			require.NoError(t, err)

			// Assert
			t.Log(tc.description)
			require.Equal(t, expectedData, dataRecorder.Bytes())
			assert.Equal(t, tc.expectedChunkLens, dataRecorder.Lengths())
			assertChecksumsMatch(t, dataRecorder.Chunks(), checksumBuffer.Bytes())
		})
	}
}

type chunkRecorder struct {
	writes [][]byte
}

func (cr *chunkRecorder) Write(p []byte) (int, error) {
	chunk := make([]byte, len(p))
	copy(chunk, p)
	cr.writes = append(cr.writes, chunk)
	return len(p), nil
}

func (cr *chunkRecorder) Bytes() []byte {
	total := 0
	for _, w := range cr.writes {
		total += len(w)
	}
	merged := make([]byte, 0, total)
	for _, w := range cr.writes {
		merged = append(merged, w...)
	}
	return merged
}

func (cr *chunkRecorder) Lengths() []int {
	lens := make([]int, len(cr.writes))
	for i, w := range cr.writes {
		lens[i] = len(w)
	}
	return lens
}

func (cr *chunkRecorder) Chunks() [][]byte {
	out := make([][]byte, len(cr.writes))
	copy(out, cr.writes)
	return out
}

func assertChecksumsMatch(t *testing.T, chunks [][]byte, checksumBytes []byte) {
	t.Helper()
	require.Equal(t, len(chunks)*csumio.ChecksumLen, len(checksumBytes))
	for i, chunk := range chunks {
		expected := xxhash.Sum64(chunk)
		offset := i * csumio.ChecksumLen
		actual := binary.LittleEndian.Uint64(checksumBytes[offset : offset+csumio.ChecksumLen])
		assert.Equal(t, expected, actual)
	}
}

package restore_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/infrastructure/restore"
	"github.com/cybozu-go/fin/internal/pkg/csumio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	rawImageFileName     = "raw.img"
	restoreImageFileName = "restore.img"
	chunkSize            = csumio.MinimumChunkSize
)

func writeRawWithChecksum(t *testing.T, rawPath string, data []byte) {
	t.Helper()

	rawFile, err := os.Create(rawPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, rawFile.Close()) }()
	checksumFile, err := os.Create(nlv.ChecksumFilePath(rawPath))
	require.NoError(t, err)
	defer func() { require.NoError(t, checksumFile.Close()) }()

	writer, err := csumio.NewWriter(rawFile, checksumFile, chunkSize)
	require.NoError(t, err)
	_, err = writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
}

func prepareRestoreVolume(t *testing.T, path string, size int64) *restore.RestoreVolume {
	t.Helper()

	file, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(size))
	require.NoError(t, file.Close())

	return restore.NewRestoreVolume(path)
}

func TestCopyChunk(t *testing.T) {
	// Description:
	// This test verifies CopyChunk behavior under combinations
	// of checksum verification enabled/disabled and checksum file integrity.
	//
	// Arrange:
	// For each scenario, prepare a temporary raw image filled with constant data
	// and optionally corrupt the associated checksum entries.
	//
	// Act:
	// Invoke CopyChunk with the configured checksum verification flag.
	//
	// Assert:
	// - When checksum verification is enabled and checksums match, data is copied.
	// - When verification is enabled and checksums are corrupted, CopyChunk returns ErrChecksumMismatch.
	// - When verification is disabled, data is copied regardless of checksum corruption.
	const (
		writtenDataByte       byte = 0xAA
		corruptedChecksumByte byte = 0x01
	)

	testCases := []struct {
		name          string
		enableVerify  bool
		matchChecksum bool
		expectErr     error
	}{
		{
			name:          "ChecksumMatch_EnableVerify",
			enableVerify:  true,
			matchChecksum: true,
		},
		{
			name:          "ChecksumMismatch_EnableVerify",
			enableVerify:  true,
			matchChecksum: false,
			expectErr:     csumio.ErrChecksumMismatch,
		},
		{
			name:          "ChecksumMismatch_DisableVerify",
			enableVerify:  false,
			matchChecksum: false,
		},
		{
			name:          "ChecksumMatch_DisableVerify",
			enableVerify:  false,
			matchChecksum: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			data := bytes.Repeat([]byte{writtenDataByte}, chunkSize)
			tmpDir := t.TempDir()
			rawPath := filepath.Join(tmpDir, rawImageFileName)
			writeRawWithChecksum(t, rawPath, data)

			if !tc.matchChecksum {
				err := os.WriteFile(
					nlv.ChecksumFilePath(rawPath),
					bytes.Repeat([]byte{corruptedChecksumByte}, csumio.ChecksumLen),
					0644,
				)
				require.NoError(t, err)
			}

			restorePath := filepath.Join(tmpDir, restoreImageFileName)
			rv := prepareRestoreVolume(t, restorePath, int64(chunkSize))

			// Act
			err := rv.CopyChunk(rawPath, 0, uint64(chunkSize), tc.enableVerify)

			// Assert
			if tc.expectErr != nil {
				assert.ErrorIs(t, err, tc.expectErr)
				return
			}

			restoreFile, err := os.Open(restorePath)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, restoreFile.Close())
			}()

			readBuf := make([]byte, chunkSize)
			_, err = io.ReadFull(restoreFile, readBuf)
			assert.NoError(t, err)
			assert.Equal(t, data, readBuf)
		})
	}
}

package nlv_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/pkg/csumio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testChecksumChunkSize = csumio.MinimumChunkSize

type trackingStream struct {
	io.Reader
	closed   bool
	closeErr error
}

func (s *trackingStream) Close() error {
	s.closed = true
	return s.closeErr
}

func newRepoWithDiffDir(t *testing.T, snapshotID int) *nlv.NodeLocalVolumeRepository {
	t.Helper()
	repo, err := nlv.NewNodeLocalVolumeRepository(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })
	require.NoError(t, repo.MakeDiffDir(snapshotID))
	return repo
}

func readVerified(t *testing.T, dataPath, checksumPath string) []byte {
	t.Helper()
	dataFile, err := os.Open(dataPath)
	require.NoError(t, err)
	defer func() { _ = dataFile.Close() }()
	checksumFile, err := os.Open(checksumPath)
	require.NoError(t, err)
	defer func() { _ = checksumFile.Close() }()

	reader, err := csumio.NewReader(dataFile, checksumFile, testChecksumChunkSize, true)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	return data
}

func TestStorePart_Success(t *testing.T) {
	// Arrange
	repo := newRepoWithDiffDir(t, 1)
	data := bytes.Repeat([]byte("0123456789"), 1000)
	stream := &trackingStream{Reader: bytes.NewReader(data)}

	// Act
	err := repo.StorePart(1, 2, stream, testChecksumChunkSize)

	// Assert
	require.NoError(t, err)
	assert.True(t, stream.closed)
	assert.Equal(t, data, readVerified(t, repo.GetDiffPartPath(1, 2), repo.GetDiffChecksumPath(1, 2)))
}

func TestStorePart_Error_StreamClose(t *testing.T) {
	// Arrange
	repo := newRepoWithDiffDir(t, 1)
	closeErr := errors.New("export failed")
	stream := &trackingStream{Reader: bytes.NewReader([]byte("data")), closeErr: closeErr}

	// Act
	err := repo.StorePart(1, 0, stream, testChecksumChunkSize)

	// Assert
	assert.ErrorIs(t, err, closeErr)
}

func TestStorePart_Error_NoDiffDir(t *testing.T) {
	// Arrange
	repo, err := nlv.NewNodeLocalVolumeRepository(t.TempDir())
	require.NoError(t, err)
	defer func() { _ = repo.Close() }()
	stream := &trackingStream{Reader: bytes.NewReader([]byte("data"))}

	// Act
	err = repo.StorePart(1, 0, stream, testChecksumChunkSize)

	// Assert
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.True(t, stream.closed)
}

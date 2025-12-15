package nlv_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveDiffPartFile_Success_NoFile(t *testing.T) {
	// Arrange
	nlv, err := nlv.NewNodeLocalVolumeRepository(t.TempDir())
	require.NoError(t, err)
	defer func() { _ = nlv.Close() }()

	// Act
	err = nlv.RemoveDiffPartFile(0, 0)

	// Assert
	assert.NoError(t, err)
}

func TestRemoveDiffPartFile_Success_WithFile(t *testing.T) {
	// Arrange
	nlv, err := nlv.NewNodeLocalVolumeRepository(t.TempDir())
	require.NoError(t, err)
	defer func() { _ = nlv.Close() }()

	createDiffPartFile := func(snapshotID, partIndex int) {
		path := nlv.GetDiffPartPath(snapshotID, partIndex)
		err = os.MkdirAll(filepath.Dir(path), 0o755)
		require.NoError(t, err)
		file, err := os.Create(path)
		require.NoError(t, err)
		err = file.Close()
		require.NoError(t, err)
		_, err = os.Stat(path)
		require.NoError(t, err)
	}
	createDiffPartFile(0, 0)
	createDiffPartFile(0, 1)

	// Act
	err1 := nlv.RemoveDiffPartFile(0, 0)
	err2 := nlv.RemoveDiffPartFile(0, 0)

	// Assert
	require.NoError(t, err1)
	require.NoError(t, err2)
	_, err = os.Stat(nlv.GetDiffPartPath(0, 0))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(nlv.GetDiffPartPath(0, 1))
	require.NoError(t, err)
}

func TestRemoveInstantVerifyImage_Success_NoFile(t *testing.T) {
	// Arrange
	nlv, err := nlv.NewNodeLocalVolumeRepository(t.TempDir())
	require.NoError(t, err)
	defer func() { _ = nlv.Close() }()

	// Act
	err = nlv.RemoveInstantVerifyImage()

	// Assert
	require.NoError(t, err)
}

func TestRemoveInstantVerifyImage_Success_WithFile(t *testing.T) {
	// Arrange
	nlv, err := nlv.NewNodeLocalVolumeRepository(t.TempDir())
	require.NoError(t, err)
	defer func() { _ = nlv.Close() }()

	path := nlv.GetInstantVerifyImagePath()
	file, err := os.Create(path)
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)
	_, err = os.Stat(path)
	require.NoError(t, err)

	// Act
	err1 := nlv.RemoveInstantVerifyImage()
	err2 := nlv.RemoveInstantVerifyImage()

	// Assert
	require.NoError(t, err1)
	require.NoError(t, err2)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

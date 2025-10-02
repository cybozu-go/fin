package nlv_test

import (
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/stretchr/testify/require"
)

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

package ceph

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateEmptyRawImage(t *testing.T) {
	repo := &RBDRepository{}
	filePath := "test.img"
	size := 1024 * 1024
	err := repo.CreateEmptyRawImage(filePath, size)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(filePath) })

	info, err := os.Stat(filePath)
	require.NoError(t, err)
	assert.Equal(t, filePath, info.Name())
	assert.Equal(t, int64(size), info.Size())
}

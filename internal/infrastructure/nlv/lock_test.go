package nlv

import (
	"testing"

	"github.com/cybozu-go/fin/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryLock(t *testing.T) {
	repo, err := NewNodeLocalVolumeRepository(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	unlock, err := repo.TryLock()
	require.NoError(t, err)

	// A second repository on the same directory stands for another process.
	other, err := NewNodeLocalVolumeRepository(repo.root.Name())
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })

	_, err = other.TryLock()
	assert.ErrorIs(t, err, model.ErrBusy)

	require.NoError(t, unlock())

	unlock2, err := other.TryLock()
	require.NoError(t, err)
	require.NoError(t, unlock2())
}

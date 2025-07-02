package sqlite

import (
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDatasource = "test.db"
	testUID        = "test-uid"
	testAction     = model.Backup
)

func TestStartOrRestartAction_success(t *testing.T) {
	repo, err := New(testDatasource)
	defer os.Remove(testDatasource)
	require.NoError(t, err)
	assert.NotNil(t, repo)

	err = repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	// Check idempotency.
	err = repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	err = repo.Close()
	require.NoError(t, err)
}

func TestStartOrRestartAction_successToReopenDB(t *testing.T) {
	repo, err := New(testDatasource)
	defer os.Remove(testDatasource)
	require.NoError(t, err)

	err = repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	err = repo.UpdateActionPrivateData(testUID, []byte("test-private-data"))
	require.NoError(t, err)

	err = repo.Close()
	require.NoError(t, err)

	// Reopen the repository to check if the private data is correctly stored
	repo, err = New(testDatasource)
	require.NoError(t, err)

	err = repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	privateData, err := repo.GetActionPrivateData(testUID)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-private-data"), privateData)

	err = repo.Close()
	require.NoError(t, err)
}

func TestUpdateAndCompleteAction_success(t *testing.T) {
	repo, err := New(testDatasource)
	defer os.Remove(testDatasource)
	require.NoError(t, err)

	err = repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)
	privateData, err := repo.GetActionPrivateData(testUID)
	require.NoError(t, err)
	assert.Len(t, privateData, 0)

	testPrivateData := []byte("test-private-data")
	err = repo.UpdateActionPrivateData(testUID, testPrivateData)
	require.NoError(t, err)
	privateData, err = repo.GetActionPrivateData(testUID)
	require.NoError(t, err)
	assert.Equal(t, testPrivateData, privateData)

	err = repo.CompleteAction(testUID)
	require.NoError(t, err)
	_, err = repo.GetActionPrivateData(testUID)
	require.Error(t, err)

	// Check idempotency.
	err = repo.CompleteAction(testUID)
	require.NoError(t, err)

	err = repo.Close()
	require.NoError(t, err)
}

func TestStartOrRestartAction_anotherActionCanStartAfterComplete(t *testing.T) {
	repo, err := New(testDatasource)
	defer os.Remove(testDatasource)
	require.NoError(t, err)

	err = repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	err = repo.CompleteAction(testUID)
	require.NoError(t, err)

	repo2, err := New(testDatasource)
	require.NoError(t, err)

	const testUID2 = "test-uid2"
	err = repo2.StartOrRestartAction(testUID2, testAction)
	require.NoError(t, err)

	err = repo.CompleteAction(testUID2)
	require.NoError(t, err)

	err = repo.Close()
	require.NoError(t, err)

	err = repo2.Close()
	require.NoError(t, err)
}

func TestStartOrRestartAction_busyError(t *testing.T) {
	repo, err := New(testDatasource)
	defer os.Remove(testDatasource)
	require.NoError(t, err)

	err = repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	err = repo.Close()
	require.NoError(t, err)

	// Try to start action with a different UID
	repo2, err := New(testDatasource)
	require.NoError(t, err)
	err = repo2.StartOrRestartAction("test-uid2", testAction)
	require.ErrorIs(t, err, model.ErrBusy)

	err = repo.Close()
	require.NoError(t, err)
}

func TestUpdateActionPrivateData_failToUpdateBeforeStart(t *testing.T) {
	repo, err := New(testDatasource)
	defer os.Remove(testDatasource)
	require.NoError(t, err)

	err = repo.UpdateActionPrivateData(testUID, []byte("test-private-data"))
	require.Error(t, err)

	err = repo.Close()
	require.NoError(t, err)
}

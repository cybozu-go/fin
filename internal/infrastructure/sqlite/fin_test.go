package sqlite

import (
	"database/sql"
	"fmt"
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

func CreateRepoForTest(t *testing.T) model.FinRepository {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_txlock=exclusive", testDatasource))
	t.Cleanup(func() { _ = db.Close(); _ = os.Remove(testDatasource) })
	require.NoError(t, err)
	repo, err := New(db)
	require.NoError(t, err)

	return repo
}

func TestStartOrRestartAction_success(t *testing.T) {
	repo := CreateRepoForTest(t)
	assert.NotNil(t, repo)

	err := repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	// Check idempotency.
	err = repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)
}

func TestStartOrRestartAction_successToReopenDB(t *testing.T) {
	repo := CreateRepoForTest(t)
	assert.NotNil(t, repo)

	err := repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	err = repo.UpdateActionPrivateData(testUID, []byte("test-private-data"))
	require.NoError(t, err)

	// Reopen the repository to check if the private data is correctly stored
	repo = CreateRepoForTest(t)
	assert.NotNil(t, repo)

	err = repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	privateData, err := repo.GetActionPrivateData(testUID)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-private-data"), privateData)
}

func TestUpdateAndCompleteAction_success(t *testing.T) {
	repo := CreateRepoForTest(t)
	assert.NotNil(t, repo)

	err := repo.StartOrRestartAction(testUID, testAction)
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
}

func TestStartOrRestartAction_anotherActionCanStartAfterComplete(t *testing.T) {
	repo := CreateRepoForTest(t)
	assert.NotNil(t, repo)

	err := repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	err = repo.CompleteAction(testUID)
	require.NoError(t, err)

	repo2 := CreateRepoForTest(t)
	assert.NotNil(t, repo)

	const testUID2 = "test-uid2"
	err = repo2.StartOrRestartAction(testUID2, testAction)
	require.NoError(t, err)

	err = repo.CompleteAction(testUID2)
	require.NoError(t, err)
}

func TestStartOrRestartAction_busyError(t *testing.T) {
	repo := CreateRepoForTest(t)
	assert.NotNil(t, repo)

	err := repo.StartOrRestartAction(testUID, testAction)
	require.NoError(t, err)

	// Try to start action with a different UID
	repo2 := CreateRepoForTest(t)
	assert.NotNil(t, repo)
	err = repo2.StartOrRestartAction("test-uid2", testAction)
	require.ErrorIs(t, err, model.ErrBusy)
}

func TestUpdateActionPrivateData_failToUpdateBeforeStart(t *testing.T) {
	repo := CreateRepoForTest(t)
	assert.NotNil(t, repo)

	err := repo.UpdateActionPrivateData(testUID, []byte("test-private-data"))
	require.Error(t, err)
}

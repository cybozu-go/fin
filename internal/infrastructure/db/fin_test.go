package db

import (
	"os"
	"testing"

	"github.com/cybozu-go/fin/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDBFile = "test.db"
	testUID    = "test-uid"
	testAction = model.Backup
)

func CreateRepoForTest(t *testing.T) *FinRepository {
	repo, err := New(testDBFile)
	t.Cleanup(func() { _ = repo.Close(); _ = os.Remove(testDBFile) })
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

func TestGetBackupMetadataAndSetBackupMetadata_success(t *testing.T) {
	// Arrange
	repo := CreateRepoForTest(t)
	require.NotNil(t, repo)

	// Act
	err1 := repo.SetBackupMetadata([]byte("{}"))
	metadata, err2 := repo.GetBackupMetadata()

	// Assert
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, "{}", string(metadata))
}

func TestGetBackupMetadata_error_moreThanOneRow(t *testing.T) {
	// CSATEST-1502
	// Description:
	//   Backups should fail if the backup_metadata table contains more than one row.
	//
	// Arrange:
	//   Make a backup_metadata table with more than one row.
	//
	// Act:
	//   Try to get the backup metadata.
	//
	// Assert:
	//   Check that an error is returned.

	// Arrange
	repo := CreateRepoForTest(t)
	require.NotNil(t, repo)

	err := repo.SetBackupMetadata([]byte("{}"))
	require.NoError(t, err)

	// Insert another row to cause an error
	_, err = repo.db.Exec("INSERT INTO backup_metadata (data) VALUES (?)", "{}")
	require.NoError(t, err)

	// Act
	_, err = repo.GetBackupMetadata()

	// Assert
	assert.Error(t, err)
}

func TestDeleteBackupMetadata_success(t *testing.T) {
	// Arrange
	repo := CreateRepoForTest(t)
	require.NotNil(t, repo)

	err := repo.SetBackupMetadata([]byte("{}"))
	require.NoError(t, err)
	metadata, err := repo.GetBackupMetadata()
	require.NoError(t, err)
	require.Equal(t, "{}", string(metadata))

	// Act
	err = repo.DeleteBackupMetadata()

	// Assert
	assert.NoError(t, err)
	_, err = repo.GetBackupMetadata()
	assert.ErrorIs(t, err, model.ErrNotFound)
}

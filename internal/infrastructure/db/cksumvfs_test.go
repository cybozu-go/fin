package db

import (
	"bytes"
	"errors"
	"os"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

var errIoErrData = sqlite3.ErrIoErr.Extend(32)

func TestCksumVFS_DetectsCorruption(t *testing.T) {
	dbFile := "test_cksum.db"
	t.Cleanup(func() { os.Remove(dbFile) })

	targetString := "this-is-the-target-string-for-checksum-test"

	repo, err := New(dbFile)
	require.NoError(t, err)

	err = repo.SetBackupMetadata([]byte(targetString))
	require.NoError(t, err)

	err = repo.Close()
	require.NoError(t, err)

	data, err := os.ReadFile(dbFile)
	require.NoError(t, err)

	idx := bytes.Index(data, []byte(targetString))
	require.NotEqual(t, -1, idx, "target string not found in database file")

	// Flip one bit in the content data only — this preserves the SQLite page
	// structure (headers, cell pointers, etc.) so PRAGMA integrity_check still
	// passes, but the actual cell payload is wrong. Only a page-level checksum
	// can detect this kind of corruption.
	data[idx] ^= 0x01
	err = os.WriteFile(dbFile, data, 0644)
	require.NoError(t, err)

	repo, err = New(dbFile)
	require.NoError(t, err)
	t.Cleanup(func() { repo.Close() })

	_, err = repo.GetBackupMetadata()

	var sqliteErr sqlite3.Error
	require.True(t, errors.As(err, &sqliteErr), "expected sqlite3.Error, got: %v", err)
	require.Equal(t, errIoErrData, sqliteErr.ExtendedCode,
		"expected SQLITE_IOERR_DATA (extended code %d), got extended code %d", errIoErrData, sqliteErr.ExtendedCode)
}

package db

import (
	"bytes"
	"database/sql"
	"errors"
	"io"
	"os"
	"sync"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

var errIoErrData = sqlite3.ErrIoErr.Extend(32)

const legacyDriver = "sqlite3-legacy-test"

var registerLegacyDriverOnce sync.Once

// createLegacyDB creates a SQLite database without cksumvfs (simulates a DB
// created before cksumvfs was enabled in Fin), writes some data into the
// backup_metadata table, and closes the connection.
func createLegacyDB(t *testing.T, dbFile string, payload string) {
	t.Helper()
	registerLegacyDriverOnce.Do(func() {
		sql.Register(legacyDriver, &sqlite3.SQLiteDriver{})
	})

	db, err := sql.Open(legacyDriver, dbFile)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec(`CREATE TABLE backup_metadata (
		id INTEGER PRIMARY KEY,
		data BLOB,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME
	)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO backup_metadata (id, data) VALUES (1, ?)`, []byte(payload))
	require.NoError(t, err)
}

// reserveBytes reads byte 20 of the SQLite file header, which indicates the
// number of bytes reserved per page (8 means cksumvfs is active).
func reserveBytes(t *testing.T, dbFile string) byte {
	t.Helper()
	f, err := os.Open(dbFile)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	var hdr [21]byte
	_, err = io.ReadFull(f, hdr[:])
	require.NoError(t, err)
	return hdr[20]
}

func TestCksumVFS_DetectsCorruption(t *testing.T) {
	dbFile := "test_cksum.db"
	t.Cleanup(func() { _ = os.Remove(dbFile) })

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
	t.Cleanup(func() { _ = repo.Close() })

	_, err = repo.GetBackupMetadata()

	var sqliteErr sqlite3.Error
	require.True(t, errors.As(err, &sqliteErr), "expected sqlite3.Error, got: %v", err)
	require.Equal(t, errIoErrData, sqliteErr.ExtendedCode,
		"expected SQLITE_IOERR_DATA (extended code %d), got extended code %d", errIoErrData, sqliteErr.ExtendedCode)
}

// TestCksumVFS_Migration tests that a SQLite database created without cksumvfs
// (simulating a pre-upgrade Fin DB) is transparently migrated when opened via
// db.New(), and that cksumvfs corruption detection works after migration.
func TestCksumVFS_Migration(t *testing.T) {
	const payload = "migration-test-payload"
	dbFile := "test_migration.db"
	t.Cleanup(func() { _ = os.Remove(dbFile) })

	// Step 1: create a legacy DB (no cksumvfs, reserve bytes = 0).
	createLegacyDB(t, dbFile, payload)
	require.Equal(t, byte(0), reserveBytes(t, dbFile), "legacy DB should have reserve bytes = 0")

	// Step 2: open via db.New() — this should trigger VACUUM migration.
	repo, err := New(dbFile)
	require.NoError(t, err)
	defer func() { _ = repo.Close() }()

	// Step 3: verify the file header now shows reserve bytes = 8.
	require.Equal(t, byte(8), reserveBytes(t, dbFile),
		"after migration, reserve bytes should be 8 (cksumvfs active)")

	// Step 4: verify the original data survived migration intact.
	got, err := repo.GetBackupMetadata()
	require.NoError(t, err)
	require.Equal(t, []byte(payload), got, "data must survive migration")

	err = repo.Close()
	require.NoError(t, err)

	// Step 5: verify that corruption is detected after migration.
	data, err := os.ReadFile(dbFile)
	require.NoError(t, err)
	idx := bytes.Index(data, []byte(payload))
	require.NotEqual(t, -1, idx, "payload not found in migrated DB file")
	data[idx] ^= 0x01
	require.NoError(t, os.WriteFile(dbFile, data, 0644))

	repo, err = New(dbFile)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	_, err = repo.GetBackupMetadata()
	var sqliteErr sqlite3.Error
	require.True(t, errors.As(err, &sqliteErr), "expected sqlite3.Error after corruption, got: %v", err)
	require.Equal(t, errIoErrData, sqliteErr.ExtendedCode,
		"expected SQLITE_IOERR_DATA after migration+corruption, got extended code %d", sqliteErr.ExtendedCode)
}

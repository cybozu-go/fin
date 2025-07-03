package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cybozu-go/fin/internal/model"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// FinRepository implements model.FinRepository interface.
type FinRepository struct {
	db *sql.DB
}

var _ model.FinRepository = &FinRepository{}

func isSQLiteBusy(err error) bool {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		if sqliteErr.Code == sqlite3.ErrBusy {
			return true
		}
	}
	return false
}

func New(db *sql.DB) (*FinRepository, error) {
	tx, err := db.Begin()
	if err != nil {
		if isSQLiteBusy(err) {
			return nil, model.ErrBusy
		}
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	createTableStmt := `
	CREATE TABLE IF NOT EXISTS action_status (
		uid TEXT NOT NULL PRIMARY KEY,
		action TEXT NOT NULL,
		private_data BLOB,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME
	);
	CREATE TABLE IF NOT EXISTS backup_metadata (
		id INTEGER PRIMARY KEY,
		data BLOB,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME
	);
	`
	_, err = tx.Exec(createTableStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &FinRepository{
		db: db,
	}, nil
}

func (fr *FinRepository) StartOrRestartAction(uid string, action model.ActionKind) error {
	tx, err := fr.db.Begin()
	if err != nil {
		if isSQLiteBusy(err) {
			return model.ErrBusy
		}
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Query("SELECT uid, action, private_data FROM action_status")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	foundMyEntry := false
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount >= 2 {
			return fmt.Errorf("multiple actions found")
		}

		var foundUID, foundAction string
		var foundPrivateData []byte
		err = rows.Scan(&foundUID, &foundAction, &foundPrivateData)
		if err != nil {
			return err
		}

		if foundUID != uid {
			return fmt.Errorf("%w: another job (uid: %s, action: %s) is already running", model.ErrBusy, foundUID, foundAction)
		}

		if foundAction != string(action) {
			return fmt.Errorf("bug: unexpected action (uid: %s, action: %s)", foundUID, foundAction)
		}

		foundMyEntry = true
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if !foundMyEntry {
		stmt, err := tx.Prepare("INSERT INTO action_status (uid, action) VALUES(?, ?)")
		if err != nil {
			return err
		}
		defer func() { _ = stmt.Close() }()

		_, err = stmt.Exec(uid, action)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (fr *FinRepository) GetActionPrivateData(uid string) ([]byte, error) {
	stmt, err := fr.db.Prepare("SELECT private_data FROM action_status WHERE uid = ?")
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()

	var privateData []byte
	err = stmt.QueryRow(uid).Scan(&privateData)
	if err != nil {
		if isSQLiteBusy(err) {
			return nil, model.ErrBusy
		}
		if errors.Is(err, sql.ErrNoRows) {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	return privateData, nil
}

func (fr *FinRepository) UpdateActionPrivateData(uid string, privateData []byte) error {
	tx, err := fr.db.Begin()
	if err != nil {
		if isSQLiteBusy(err) {
			return model.ErrBusy
		}
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare("UPDATE action_status SET private_data = ?, updated_at = ? WHERE uid = ?")
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	result, err := stmt.Exec(privateData, time.Now(), uid)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("no rows updated for uid: %s", uid)
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (fr *FinRepository) CompleteAction(uid string) error {
	tx, err := fr.db.Begin()
	if err != nil {
		if isSQLiteBusy(err) {
			return model.ErrBusy
		}
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare("DELETE FROM action_status WHERE uid = ?")
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	_, err = stmt.Exec(uid)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (fr *FinRepository) GetBackupMetadata() ([]byte, error) {
	stmt, err := fr.db.Prepare("SELECT data FROM backup_metadata")
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()

	var data []byte
	err = stmt.QueryRow().Scan(&data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, model.ErrNotFound
		}
		return nil, fmt.Errorf("failed to scan: %w", err)
	}
	return data, nil
}

func (fr *FinRepository) SetBackupMetadata(data []byte) error {
	tx, err := fr.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare("INSERT INTO backup_metadata (id, data, created_at) VALUES (1, ?, ?)" +
		" ON CONFLICT(id) DO UPDATE SET data = excluded.data, updated_at = excluded.created_at")
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	result, err := stmt.Exec(data, time.Now())
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("backup_metadata was not updated")
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

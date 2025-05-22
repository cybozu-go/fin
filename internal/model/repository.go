package model

import "errors"

type ActionKind string

const (
	// TODO: Add required actions here.
	Backup ActionKind = "Backup"
)

var (
	ErrBusy     = errors.New("repository is busy")
	ErrNotFound = errors.New("not found")
)

type FinRepository interface {
	// StartOrRestartAction starts or restarts an action with the given UID and action kind.
	// If the repository is busy and temporarily unavailable, it returns `ErrBusy`.
	StartOrRestartAction(uid string, action ActionKind) error

	// GetActionPrivateData retrieves the private data associated with the given UID.
	// If the repository is busy and temporarily unavailable, it returns `ErrBusy`.
	// If the UID is not found, it returns `model.ErrNotFound`.
	GetActionPrivateData(uid string) ([]byte, error)

	// UpdateActionPrivateData updates the private data associated with the given UID.
	// If the repository is busy and temporarily unavailable, it returns `ErrBusy`.
	UpdateActionPrivateData(uid string, privateData []byte) error

	// CompleteAction marks the action with the given UID as complete.
	// If the repository is busy and temporarily unavailable, it returns `ErrBusy`.
	CompleteAction(uid string) error

	// GetBackupMetadata retrieves the backup metadata.
	// If the metadata is not found, it returns model.ErrNotFound.
	GetBackupMetadata() ([]byte, error)

	// SetBackupMetadata sets the backup metadata.
	SetBackupMetadata(metadata []byte) error
}

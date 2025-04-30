package model

import "errors"

type ActionKind string

const (
	// TODO: Add required actions here.
	Backup ActionKind = "Backup"
)

var (
	ErrBusy = errors.New("repository is busy")
)

type FinRepository interface {
	// StartOrRestartAction starts or restarts an action with the given UID and action kind.
	// If the repository is busy and temporarily unavailable, it returns `ErrBusy`.
	StartOrRestartAction(uid string, action ActionKind) error
	// GetActionPrivateData retrieves the private data associated with the given UID.
	// If the repository is busy and temporarily unavailable, it returns `ErrBusy`.
	GetActionPrivateData(uid string) ([]byte, error)
	// UpdateActionPrivateData updates the private data associated with the given UID.
	// If the repository is busy and temporarily unavailable, it returns `ErrBusy`.
	UpdateActionPrivateData(uid string, privateData []byte) error
	// CompleteAction marks the action with the given UID as complete.
	// If the repository is busy and temporarily unavailable, it returns `ErrBusy`.
	CompleteAction(uid string) error
}

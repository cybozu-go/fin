package model

import (
	"errors"

	corev1 "k8s.io/api/core/v1"
)

type ActionKind string

const (
	Backup  ActionKind = "Backup"
	Restore ActionKind = "Restore"
)

var (
	ErrBusy          = errors.New("repository is busy")
	ErrAlreadyExists = errors.New("already exists")
	ErrNotFound      = errors.New("not found")
)

type SqliteRepository interface {
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

type KubernetesRepository interface {
	// GetPVC retrieves the PVC with the given name and namespace.
	// It returns a non-nil pointer to the PVC if it is found.
	// If the PVC is not found, it returns an error.
	GetPVC(name, namespace string) (*corev1.PersistentVolumeClaim, error)

	// GetPV retrieves the PV with the given name.
	// It returns a non-nil pointer to the PV if it is found.
	// If the PV is not found, it returns an error.
	GetPV(name string) (*corev1.PersistentVolume, error)
}

type RBDSnapshot struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Size      int    `json:"size"`
	Timestamp string `json:"timestamp"`
}

type ExportDiffInput struct {
	PoolName       string
	ReadOffset     int
	ReadLength     int
	FromSnap       *string
	MidSnapPrefix  string
	ImageName      string
	TargetSnapName string
	OutputFile     string
}

// RBDRepository is an interface for managing RBD images and snapshots.
// It provides any operations that need knowledge of RBD's internal structure.
type RBDRepository interface {
	// ListSnapshots retrieves a list of snapshots for the specified pool and image.
	ListSnapshots(poolName, imageName string) ([]*RBDSnapshot, error)

	// ExportDiff exports the difference between the source snapshot and the target snapshot.
	// If the source snapshot is not specified, it exports the difference from the empty image.
	ExportDiff(input *ExportDiffInput) error

	// ApplyDiff applies the difference from the diff file to the raw image file.
	ApplyDiff(rawImageFilePath, diffFilePath string) error

	// CreateEmptyRawImage creates an empty file with the specified size.
	// It returns an error if the file creation fails.
	// If the file already exists, it returns `ErrAlreadyExists`.
	CreateEmptyRawImage(filePath string, size int) error
}

// NodeLocalVolumeRepository is an interface for directly managing a filesystem
// on the node local volume. The root path is supposed to be `/volume/<PVC's namespace>/<PVC's name>`.
type NodeLocalVolumeRepository interface {
	// GetDiffPartPath returns the diff part path.
	GetDiffPartPath(snapshotID, partIndex int) string

	// GetRawImagePath returns the path of raw.img
	GetRawImagePath() string

	// PutPVC puts PVC's manifest into this repository.
	PutPVC(pvc *corev1.PersistentVolumeClaim) error

	// PutPV puts PV's manifest into this repository.
	PutPV(pv *corev1.PersistentVolume) error

	// GetPVC gets PVC's manifest.
	GetPVC() (*corev1.PersistentVolumeClaim, error)

	// GetPV gets PV's manifest.
	GetPV() (*corev1.PersistentVolume, error)

	// GetDBPath return the path of database.
	GetDBPath() string

	// MakeDiffPartDir creates a diff directory. It uses 0755 as the permission.
	// If the directory already exists, it returns `ErrAlreadyExists`.
	MakeDiffDir(snapshotID int) error

	// RemoveDiffDirRecursively removes a diff directory and its contents.
	RemoveDiffDirRecursively(snapshotID int) error

	// Close closes the NLV, rendering it unusable for further operations.
	Close() error
}

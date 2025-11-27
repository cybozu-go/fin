package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cybozu-go/fin/internal/model"
)

var ErrCantLock = errors.New("can't lock")

const (
	RetryInterval = time.Duration(10) * time.Second
)

type BackupMetadata struct {
	PVCUID                string                 `json:"pvcUID,omitempty"`
	RBDImageName          string                 `json:"rbdImageName,omitempty"`
	RawChecksumChunkSize  int64                  `json:"rawChecksumChunkSize,omitempty"`
	DiffChecksumChunkSize int64                  `json:"diffChecksumChunkSize,omitempty"`
	Raw                   *BackupMetadataEntry   `json:"raw,omitempty"`
	Diff                  []*BackupMetadataEntry `json:"diff,omitempty"`
}

type BackupMetadataEntry struct {
	SnapID    int       `json:"snapID"`
	SnapName  string    `json:"snapName"`
	SnapSize  uint64    `json:"snapSize"`
	PartSize  uint64    `json:"partSize"`
	CreatedAt time.Time `json:"createdAt"`
}

func GetBackupMetadata(repo model.FinRepository) (*BackupMetadata, error) {
	metadata, err := repo.GetBackupMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get backup metadata: %w", err)
	}

	var data BackupMetadata
	if err := json.Unmarshal(metadata, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal backup metadata: %w", err)
	}
	return &data, nil
}

func SetBackupMetadata(repo model.FinRepository, metadata *BackupMetadata) error {
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal backup metadata: %w", err)
	}
	if err := repo.SetBackupMetadata(metadataBytes); err != nil {
		return fmt.Errorf("failed to set backup metadata: %w", err)
	}
	return nil
}

func CalcSnapshotNamesWithOffset(
	sourceSnapshotName, targetSnapshotName string,
	idx, partCount int, partSize uint64) (string, string) {
	if idx != 0 {
		sourceSnapshotName = fmt.Sprintf("%s-offset-%d", targetSnapshotName, uint64(idx)*partSize)
	}
	if idx != partCount-1 {
		targetSnapshotName = fmt.Sprintf("%s-offset-%d", targetSnapshotName, uint64(idx+1)*partSize)
	}
	return sourceSnapshotName, targetSnapshotName
}

func SyncData(filename string) error {
	f, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", filename, err)
	}
	defer func() { _ = f.Close() }()

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync %q: %w", filename, err)
	}

	return nil
}

package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cybozu-go/fin/internal/model"
)

var ErrCantLock = errors.New("can't lock")

type BackupMetadata struct {
	PVCUID       string                 `json:"pvcUID,omitempty"`
	RBDImageName string                 `json:"rbdImageName,omitempty"`
	Raw          *BackupMetadataEntry   `json:"raw,omitempty"`
	Diff         []*BackupMetadataEntry `json:"diff,omitempty"`
}

type BackupMetadataEntry struct {
	SnapID    int       `json:"snapID"`
	SnapName  string    `json:"snapName"`
	SnapSize  int       `json:"snapSize"`
	PartSize  int       `json:"partSize"`
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

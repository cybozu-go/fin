package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cybozu-go/fin/internal/model"
)

func getRawImagePath() string {
	return "raw.img"
}

func getDiffDirPath(snapshotID int) string {
	return filepath.Join("diff", fmt.Sprintf("%d", snapshotID))
}

func getDiffPartPath(snapshotID, partIndex int) string {
	return filepath.Join(getDiffDirPath(snapshotID), fmt.Sprintf("part-%d", partIndex))
}

func createDiffDir(nlv model.NodeLocalVolumeRepository, snapshotID int) error {
	if err := nlv.Mkdir("diff"); err != nil && !errors.Is(err, model.ErrAlreadyExists) {
		return fmt.Errorf("failed to create base diff directory 'diff': %w", err)
	}
	if err := nlv.Mkdir(getDiffDirPath(snapshotID)); err != nil && !errors.Is(err, model.ErrAlreadyExists) {
		return fmt.Errorf("failed to create snapshot-specific diff directory '%s': %w", getDiffDirPath(snapshotID), err)
	}
	return nil
}

type backupMetadata struct {
	PVCUID       string                 `json:"pvcUID,omitempty"`
	RBDImageName string                 `json:"rbdImageName,omitempty"`
	Raw          *backupMetadataEntry   `json:"raw,omitempty"`
	Diff         []*backupMetadataEntry `json:"diff,omitempty"`
}

type backupMetadataEntry struct {
	SnapID    int       `json:"snapID"`
	SnapName  string    `json:"snapName"`
	SnapSize  int       `json:"snapSize"`
	CreatedAt time.Time `json:"createdAt"`
}

func getBackupMetadata(repo model.FinRepository) (*backupMetadata, error) {
	metadata, err := repo.GetBackupMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get backup metadata: %w", err)
	}

	var data backupMetadata
	if err := json.Unmarshal(metadata, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal backup metadata: %w", err)
	}
	return &data, nil
}

func setBackupMetadata(repo model.FinRepository, metadata *backupMetadata) error {
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal backup metadata: %w", err)
	}
	if err := repo.SetBackupMetadata(metadataBytes); err != nil {
		return fmt.Errorf("failed to set backup metadata: %w", err)
	}
	return nil
}

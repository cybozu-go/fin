package fake

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cybozu-go/fin/internal/model"
)

type RawImage struct {
	Size         int             `json:"size"`
	AppliedDiffs []*ExportedDiff `json:"appliedDiffs"`
}

type ExportedDiff struct {
	PoolName      string    `json:"poolName"`
	ReadOffset    int       `json:"readOffset"`
	ReadLength    int       `json:"readLength"`
	FromSnap      *string   `json:"fromSnap"`
	MidSnapPrefix string    `json:"midSnapPrefix"`
	ImageName     string    `json:"imageName"`
	SnapID        int       `json:"snapId"`
	SnapName      string    `json:"snapName"`
	SnapSize      int       `json:"snapSize"`
	SnapTimestamp time.Time `json:"snapTimestamp"`
}

type RBDRepository struct {
	poolName, imageName string
	snapshots           map[int]*model.RBDSnapshot
}

var _ model.RBDRepository = &RBDRepository{}

func NewRBDRepository(poolName, imageName string, snapshots map[int]*model.RBDSnapshot) *RBDRepository {
	return &RBDRepository{poolName: poolName, imageName: imageName, snapshots: snapshots}
}

func (r *RBDRepository) ImageName() string {
	return r.imageName
}

func (r *RBDRepository) GetSnapshot(snapshotID int) (*model.RBDSnapshot, error) {
	snapshot, ok := r.snapshots[snapshotID]
	if !ok {
		return nil, model.ErrNotFound
	}
	return snapshot, nil
}

func (r *RBDRepository) GetSnapshotByName(snapshotName string) (*model.RBDSnapshot, error) {
	for _, snapshot := range r.snapshots {
		if snapshot.Name == snapshotName {
			return snapshot, nil
		}
	}
	return nil, model.ErrNotFound
}

func (r *RBDRepository) ExportDiff(targetSnapName, midSnapPrefix string, readOffset, readLength int, fromSnap *string, outputFile string) error {
	snapshot, err := r.GetSnapshotByName(targetSnapName)
	if err != nil {
		return fmt.Errorf("failed to get snapshot by name: %w", err)

	}
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer func() { _ = file.Close() }()

	encoder := json.NewEncoder(file)
	diff := ExportedDiff{
		PoolName:      r.poolName,
		ReadOffset:    readOffset,
		ReadLength:    readLength,
		FromSnap:      fromSnap,
		MidSnapPrefix: midSnapPrefix,
		ImageName:     r.imageName,
		SnapID:        snapshot.ID,
		SnapName:      snapshot.Name,
		SnapSize:      snapshot.Size,
		SnapTimestamp: snapshot.Timestamp.Time,
	}
	if err := encoder.Encode(diff); err != nil {
		return fmt.Errorf("failed to encode exported diff: %w", err)
	}
	return nil
}

func (r *RBDRepository) CreateEmptyRawImage(filePath string, size int) error {
	if _, err := os.Stat(filePath); err == nil {
		return model.ErrAlreadyExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to check file existence: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create raw image file: %w", err)
	}
	defer func() { _ = file.Close() }()

	encoder := json.NewEncoder(file)
	rawImage := RawImage{
		Size: size,
	}
	if err := encoder.Encode(rawImage); err != nil {
		return fmt.Errorf("failed to encode raw image: %w", err)
	}
	return nil
}

func (r *RBDRepository) ApplyDiff(rawImageFilePath, diffFilePath string) error {
	rawImage, err := ReadRawImage(rawImageFilePath)
	if err != nil {
		return fmt.Errorf("failed to decode raw image: %w", err)
	}
	diff, err := ReadDiff(diffFilePath)
	if err != nil {
		return fmt.Errorf("failed to decode diff: %w", err)
	}

	// update the raw image
	rawImage.AppliedDiffs = append(rawImage.AppliedDiffs, diff)
	rawImage.Size = max(rawImage.Size, min(diff.SnapSize, diff.ReadOffset+diff.ReadLength))

	// write the updated raw image back to the file
	rawImageFile, err := os.Create(rawImageFilePath)
	if err != nil {
		return fmt.Errorf("failed to open raw image file for writing: %w", err)
	}
	defer func() { _ = rawImageFile.Close() }()
	encoder := json.NewEncoder(rawImageFile)
	if err := encoder.Encode(rawImage); err != nil {
		return fmt.Errorf("failed to encode updated raw image: %w", err)
	}

	return nil
}

func ReadRawImage(filePath string) (*RawImage, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open raw image file: %w", err)
	}
	defer func() { _ = file.Close() }()

	decoder := json.NewDecoder(file)
	var rawImage RawImage
	if err := decoder.Decode(&rawImage); err != nil {
		return nil, fmt.Errorf("failed to decode raw image: %w", err)
	}
	return &rawImage, nil
}

func ReadDiff(filePath string) (*ExportedDiff, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open diff file: %w", err)
	}
	defer func() { _ = file.Close() }()

	decoder := json.NewDecoder(file)
	var diff ExportedDiff
	if err := decoder.Decode(&diff); err != nil {
		return nil, fmt.Errorf("failed to decode diff: %w", err)
	}
	return &diff, nil
}

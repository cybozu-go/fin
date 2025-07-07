package fake

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/cybozu-go/fin/internal/model"
)

type PoolImageName struct {
	PoolName, ImageName string
}

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
	snapshots map[PoolImageName][]*model.RBDSnapshot
}

var _ model.RBDRepository = &RBDRepository{}

func NewRBDRepository(
	snapshots map[PoolImageName][]*model.RBDSnapshot,
) *RBDRepository {
	return &RBDRepository{snapshots: snapshots}
}

func (r *RBDRepository) ListSnapshots(poolName, imageName string) ([]*model.RBDSnapshot, error) {
	if snapshots, ok := r.snapshots[PoolImageName{poolName, imageName}]; ok {
		return snapshots, nil
	}
	return nil, model.ErrNotFound
}

func (r *RBDRepository) ExportDiff(input *model.ExportDiffInput) error {
	snapshots, ok := r.snapshots[PoolImageName{PoolName: input.PoolName, ImageName: input.ImageName}]
	if !ok {
		return errors.New("source image not found")
	}
	snapshotIndex := slices.IndexFunc(snapshots, func(snapshot *model.RBDSnapshot) bool {
		return snapshot.Name == input.TargetSnapName
	})
	if snapshotIndex == -1 {
		return errors.New("source snapshot not found")
	}
	snapshot := snapshots[snapshotIndex]

	file, err := os.Create(input.OutputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer func() { _ = file.Close() }()

	encoder := json.NewEncoder(file)
	diff := ExportedDiff{
		PoolName:      input.PoolName,
		ReadOffset:    input.ReadOffset,
		ReadLength:    input.ReadLength,
		FromSnap:      input.FromSnap,
		MidSnapPrefix: input.MidSnapPrefix,
		ImageName:     input.ImageName,
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

func (r *RBDRepository) ApplyDiffToBlockDevice(blockDevicePath, diffFilePath, fromSnapName, toSnapName string) error {
	return r.applyDiff(blockDevicePath, diffFilePath, fromSnapName, toSnapName)
}

func (r *RBDRepository) ApplyDiffToRawImage(rawImageFilePath, diffFilePath, fromSnapName, toSnapName string) error {
	return r.applyDiff(rawImageFilePath, diffFilePath, fromSnapName, toSnapName)
}

func (r *RBDRepository) applyDiff(rawImageFilePath, diffFilePath, fromSnapName, toSnapName string) error {
	rawImage, err := ReadRawImage(rawImageFilePath)
	if err != nil {
		return fmt.Errorf("failed to decode raw image: %w", err)
	}
	diff, err := ReadDiff(diffFilePath)
	if err != nil {
		return fmt.Errorf("failed to decode diff: %w", err)
	}

	// validate the diff
	if diff.ReadOffset == 0 {
		if diff.FromSnap == nil && fromSnapName != "" {
			return fmt.Errorf("fromSnapName mismatch: expected %s, got nil", fromSnapName)
		}
		if diff.FromSnap != nil && *diff.FromSnap != fromSnapName {
			return fmt.Errorf("fromSnapName mismatch: expected %s, got %s", fromSnapName, *diff.FromSnap)
		}
	} else {
		gotFromSnapName := fmt.Sprintf("%s-offset-%d", diff.SnapName, diff.ReadOffset)
		if gotFromSnapName != fromSnapName {
			return fmt.Errorf("fromSnapName mismatch: expected %s, got %s", fromSnapName, gotFromSnapName)
		}
	}

	if gotToSnapName := fmt.Sprintf(
		"%s-offset-%d", diff.SnapName, diff.ReadOffset+diff.ReadLength,
	); diff.SnapSize > diff.ReadOffset+diff.ReadLength && gotToSnapName != toSnapName {
		return fmt.Errorf("toSnapName mismatch: expected %s, got %s", toSnapName, gotToSnapName)
	}
	if diff.SnapSize <= diff.ReadOffset+diff.ReadLength && diff.SnapName != toSnapName {
		return fmt.Errorf("toSnapName mismatch: expected %s, got %s", toSnapName, diff.SnapName)
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

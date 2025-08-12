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
	AppliedDiffs []*ExportedDiff `json:"appliedDiffs"`
}

type ExportedDiff struct {
	PoolName      string    `json:"poolName"`
	ReadOffset    uint64    `json:"readOffset"`
	ReadLength    uint64    `json:"readLength"`
	FromSnap      *string   `json:"fromSnap"`
	MidSnapPrefix string    `json:"midSnapPrefix"`
	ImageName     string    `json:"imageName"`
	SnapID        int       `json:"snapId"`
	SnapName      string    `json:"snapName"`
	SnapSize      uint64    `json:"snapSize"`
	SnapTimestamp time.Time `json:"snapTimestamp"`
}

type RBDRepository struct {
	poolName, imageName string
	lastSnapID          int
	snapshots           []*model.RBDSnapshot
}

var _ model.RBDRepository = &RBDRepository{}

// deprecated: this function will be removed later as a result of refactoring.
func NewRBDRepository(
	s map[PoolImageName][]*model.RBDSnapshot,
) *RBDRepository {
	if len(s) > 1 {
		panic("multiple pool/image names are not supported")
	}
	repo := &RBDRepository{
		snapshots: make([]*model.RBDSnapshot, 0),
	}
	if len(s) == 0 {
		return repo
	}

	for poolImageName, snapshots := range s {
		repo.poolName = poolImageName.PoolName
		repo.imageName = poolImageName.ImageName
		repo.snapshots = append(repo.snapshots, snapshots...)
		repo.lastSnapID = snapshots[len(snapshots)-1].ID
	}

	return repo
}

// CreateFakeSnapshot creates a fake snapshot for the specified pool and image.
// It returns the snapshot ID of the created one.
func (r *RBDRepository) CreateFakeSnapshot(name string, size uint64, timestamp time.Time) *model.RBDSnapshot {
	r.lastSnapID++
	snapshot := &model.RBDSnapshot{
		ID:        r.lastSnapID,
		Name:      name,
		Size:      size,
		Timestamp: model.NewRBDTimeStamp(timestamp),
	}
	r.snapshots = append(r.snapshots, snapshot)
	return snapshot
}

func (r *RBDRepository) ListSnapshots(poolName, imageName string) ([]*model.RBDSnapshot, error) {
	if poolName == r.poolName && imageName == r.imageName {
		return r.snapshots, nil
	}
	return nil, model.ErrNotFound
}

func (r *RBDRepository) ExportDiff(input *model.ExportDiffInput) error {
	if len(r.snapshots) == 0 {
		return errors.New("source image not found")
	}
	snapshotIndex := slices.IndexFunc(r.snapshots, func(snapshot *model.RBDSnapshot) bool {
		return snapshot.Name == input.TargetSnapName
	})
	if snapshotIndex == -1 {
		return errors.New("source snapshot not found")
	}
	snapshot := r.snapshots[snapshotIndex]

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

func (r *RBDRepository) ApplyDiffToBlockDevice(blockDevicePath, diffFilePath, fromSnapName, toSnapName string) error {
	return r.applyDiff(blockDevicePath, diffFilePath, fromSnapName, toSnapName)
}

func (r *RBDRepository) ApplyDiffToRawImage(rawImageFilePath, diffFilePath, fromSnapName, toSnapName string) error {
	return r.applyDiff(rawImageFilePath, diffFilePath, fromSnapName, toSnapName)
}

func (r *RBDRepository) applyDiff(rawImageFilePath, diffFilePath, fromSnapName, toSnapName string) error {
	err := createRawImageIfNotExists(rawImageFilePath)
	if err != nil {
		return fmt.Errorf("failed to create raw image if not exists: %w", err)
	}
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

func createRawImageIfNotExists(filePath string) error {
	if _, err := os.Stat(filePath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to check file existence: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create raw image file: %w", err)
	}
	defer func() { _ = file.Close() }()

	encoder := json.NewEncoder(file)
	rawImage := RawImage{}
	if err := encoder.Encode(rawImage); err != nil {
		return fmt.Errorf("failed to encode raw image: %w", err)
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

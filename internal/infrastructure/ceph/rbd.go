package ceph

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"

	"github.com/cybozu-go/fin/internal/model"
)

type RBDRepository struct {
	poolName  string
	imageName string
}

var _ model.RBDRepository = &RBDRepository{}

func NewRBDRepository(poolName, imageName string) *RBDRepository {
	return &RBDRepository{
		poolName:  poolName,
		imageName: imageName,
	}
}

func (r *RBDRepository) ImageName() string {
	return r.imageName
}

func (r *RBDRepository) CreateSnapshot(snapName string) error {
	args := []string{"snap", "create", fmt.Sprintf("%s/%s@%s", r.poolName, r.imageName, snapName)}
	_, stderr, err := runRBDCommand(args...)
	if err != nil {
		return fmt.Errorf("failed to create RBD snapshot: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

func (r *RBDRepository) snapshots() ([]*model.RBDSnapshot, error) {
	args := []string{"snap", "ls", "--format", "json", fmt.Sprintf("%s/%s", r.poolName, r.imageName)}
	stdout, stderr, err := runRBDCommand(args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list RBD snapshots: %w, stderr: %s", err, string(stderr))
	}

	var snapshots []*model.RBDSnapshot
	err = json.Unmarshal(stdout, &snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD snapshots: %w", err)
	}

	return snapshots, nil
}

func (r *RBDRepository) GetSnapshot(snapshotID int) (*model.RBDSnapshot, error) {
	snapshots, err := r.snapshots()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	targetSnapshotIndex := slices.IndexFunc(snapshots, func(s *model.RBDSnapshot) bool {
		return s.ID == snapshotID
	})
	if targetSnapshotIndex == -1 {
		return nil, fmt.Errorf("target snapshot ID %d not found", snapshotID)
	}
	return snapshots[targetSnapshotIndex], nil
}

func (r *RBDRepository) GetSnapshotByName(snapshotName string) (*model.RBDSnapshot, error) {
	snapshots, err := r.snapshots()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	targetSnapshotIndex := slices.IndexFunc(snapshots, func(s *model.RBDSnapshot) bool {
		return s.Name == snapshotName
	})
	if targetSnapshotIndex == -1 {
		return nil, fmt.Errorf("target snapshot name %q not found", snapshotName)
	}
	return snapshots[targetSnapshotIndex], nil
}

func (r *RBDRepository) ExportDiff(targetSnapName, midSnapPrefix string, readOffset, readLength int, fromSnap *string, outputFile string) error {
	args := []string{
		"export-diff",
		"-p", r.poolName,
		"--read-offset", strconv.Itoa(readOffset),
		"--read-length", strconv.Itoa(readLength),
		"--mid-snap-prefix", midSnapPrefix,
	}
	if fromSnap != nil {
		args = append(args, "--from-snap", *fromSnap)
	}
	args = append(args, fmt.Sprintf("%s@%s", r.imageName, targetSnapName), outputFile)
	_, stderr, err := runRBDCommand(args...)
	if err != nil {
		return fmt.Errorf("failed to export diff: %w, stderr: %s", err, string(stderr))
	}
	return nil
}

func (r *RBDRepository) ApplyDiff(rawImageFilePath, diffFilePath string) error {
	slog.Error("ApplyDiff is not implemented")
	return nil
}

func (r *RBDRepository) CreateEmptyRawImage(filePath string, size int) error {
	if size <= 0 {
		return errors.New("invalid raw image size")
	}
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(int64(size)-1, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to end of file %s: %w", filePath, err)
	}

	if _, err := f.Write([]byte{0}); err != nil {
		return fmt.Errorf("failed to write to file %s: %w", filePath, err)
	}
	return nil
}

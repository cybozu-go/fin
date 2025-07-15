package ceph

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/cybozu-go/fin/internal/model"
)

type RBDRepository struct {
}

var _ model.RBDRepository = &RBDRepository{}

func NewRBDRepository() *RBDRepository {
	return &RBDRepository{}
}

func (r *RBDRepository) ListSnapshots(poolName, imageName string) ([]*model.RBDSnapshot, error) {
	args := []string{"snap", "ls", "--format", "json", fmt.Sprintf("%s/%s", poolName, imageName)}
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

func (r *RBDRepository) ExportDiff(input *model.ExportDiffInput) error {
	args := []string{"export-diff", "-p", input.PoolName, "--read-offset", strconv.Itoa(input.ReadOffset),
		"--read-length", strconv.Itoa(input.ReadLength)}
	if input.FromSnap != nil {
		args = append(args, "--from-snap", *input.FromSnap)
	}
	args = append(args, "--mid-snap-prefix", input.MidSnapPrefix,
		fmt.Sprintf("%s@%s", input.ImageName, input.TargetSnapName), input.OutputFile)

	_, stderr, err := runRBDCommand(args...)
	if err != nil {
		return fmt.Errorf("failed to export diff: %w, stderr: %s", err, string(stderr))
	}
	return nil
}

func (r *RBDRepository) ApplyDiff(rawImageFilePath, diffFilePath string) error {
	return errors.New("not implemented")
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

package nlv

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/cybozu-go/fin/internal/model"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

const (
	pvcFilePath = "pvc.yaml"
	pvFilePath  = "pv.yaml"
)

type NodeLocalVolumeRepository struct {
	rootPath string
}

var _ model.NodeLocalVolumeRepository = &NodeLocalVolumeRepository{}

func NewNodeLocalVolumeRepository(rootPath string) *NodeLocalVolumeRepository {
	return &NodeLocalVolumeRepository{
		rootPath: rootPath,
	}
}

func (r *NodeLocalVolumeRepository) Cleanup() {
	if r.rootPath == "" {
		return
	}
	_ = os.RemoveAll(r.rootPath)
}

func getDiffRelPath(snapshotID int) string {
	return filepath.Join("diff", fmt.Sprintf("%d", snapshotID))
}

func (r *NodeLocalVolumeRepository) GetDiffPartPath(snapshotID, partIndex int) string {
	return filepath.Join(r.rootPath, getDiffRelPath(snapshotID), fmt.Sprintf("part-%d", partIndex))
}

func (r *NodeLocalVolumeRepository) GetRawImagePath() string {
	return filepath.Join(r.rootPath, "raw.img")
}

func (r *NodeLocalVolumeRepository) GetDBPath() string {
	return filepath.Join(r.rootPath, "fin.sqlite3")
}

func (r *NodeLocalVolumeRepository) writeFile(filePath string, data []byte) error {
	root, err := os.OpenRoot(r.rootPath)
	if err != nil {
		return fmt.Errorf("failed to open node local volume root directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create or open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

func (r *NodeLocalVolumeRepository) PutPVC(pvc *corev1.PersistentVolumeClaim) error {
	data, err := yaml.Marshal(pvc)
	if err != nil {
		return fmt.Errorf("failed to marshal target PVC: %w", err)
	}
	if err := r.writeFile(pvcFilePath, data); err != nil {
		return fmt.Errorf("failed to write target PVC file: %w", err)
	}
	return nil
}

func (r *NodeLocalVolumeRepository) PutPV(pv *corev1.PersistentVolume) error {
	data, err := yaml.Marshal(pv)
	if err != nil {
		return fmt.Errorf("failed to marshal target PV: %w", err)
	}
	if err := r.writeFile(pvFilePath, data); err != nil {
		return fmt.Errorf("failed to write target PV file: %w", err)
	}
	return nil
}

// This function exist for testing.
//
//nolint:dupl
func (r *NodeLocalVolumeRepository) GetPVC() (*corev1.PersistentVolumeClaim, error) {
	root, err := os.OpenRoot(r.rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open node local volume root directory: %w", err)
	}
	defer func() { _ = root.Close() }()

	f, err := root.Open(pvcFilePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read from %s: %w", f.Name(), err)
	}
	var pvc corev1.PersistentVolumeClaim
	err = yaml.Unmarshal(data, &pvc)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal pvc's manifest: %w", err)
	}
	return &pvc, nil
}

// This function exist for testing.
//
//nolint:dupl
func (r *NodeLocalVolumeRepository) GetPV() (*corev1.PersistentVolume, error) {
	root, err := os.OpenRoot(r.rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open node local volume root directory: %w", err)
	}
	defer func() { _ = root.Close() }()

	f, err := root.Open(pvFilePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read from %s: %w", f.Name(), err)
	}
	var pv corev1.PersistentVolume
	err = yaml.Unmarshal(data, &pv)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal pv's manifest: %w", err)
	}
	return &pv, nil
}

func (r *NodeLocalVolumeRepository) MakeDiffDir(snapshotID int) error {
	root, err := os.OpenRoot(r.rootPath)
	if err != nil {
		return fmt.Errorf("failed to open node local volume root directory: %w", err)
	}
	defer func() { _ = root.Close() }()

	if err := root.Mkdir("diff", 0755); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("failed to create base diff directory 'diff': %w", err)
	}
	if err := root.Mkdir(getDiffRelPath(snapshotID), 0755); err != nil {
		if os.IsExist(err) {
			return model.ErrAlreadyExists
		}
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

func (r *NodeLocalVolumeRepository) RemoveDiffDirRecursively(snapshotID int) error {
	root, err := os.OpenRoot(r.rootPath)
	if err != nil {
		return fmt.Errorf("failed to open root directory: %w", err)
	}
	defer func() { _ = root.Close() }()

	entries := []string{}
	if err := fs.WalkDir(root.FS(), getDiffRelPath(snapshotID), func(path string, d fs.DirEntry, err error) error {
		entries = append(entries, path)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	// reverse the order of entries to remove them from leaf to root
	slices.Reverse(entries)

	for _, entry := range entries {
		if err := root.Remove(entry); err != nil {
			return fmt.Errorf("failed to remove entry %s: %w", entry, err)
		}
	}

	return nil
}

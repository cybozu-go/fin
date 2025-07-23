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
	pvcFilePath   = "pvc.yaml"
	pvFilePath    = "pv.yaml"
	imageFilePath = "raw.img"
)

type NodeLocalVolumeRepository struct {
	root *os.Root
}

var _ model.NodeLocalVolumeRepository = &NodeLocalVolumeRepository{}

func NewNodeLocalVolumeRepository(rootPath string) (*NodeLocalVolumeRepository, error) {
	st, err := os.Stat(rootPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("failed to stat %s: %w", rootPath, err)
		}
		err = os.MkdirAll(rootPath, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create root directory %s: %w", rootPath, err)
		}
	} else if !st.IsDir() {
		return nil, fmt.Errorf("%s already exists but is not a directory", rootPath)
	}

	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open root directory: %w", err)
	}

	return &NodeLocalVolumeRepository{root: root}, nil
}

func (r *NodeLocalVolumeRepository) Close() error {
	return r.root.Close()
}

func getDiffRelPath(snapshotID int) string {
	return filepath.Join("diff", fmt.Sprintf("%d", snapshotID))
}

func (r *NodeLocalVolumeRepository) GetDiffPartPath(snapshotID, partIndex int) string {
	return filepath.Join(r.root.Name(), getDiffRelPath(snapshotID), fmt.Sprintf("part-%d", partIndex))
}

func (r *NodeLocalVolumeRepository) GetRawImagePath() string {
	return filepath.Join(r.root.Name(), "raw.img")
}

func (r *NodeLocalVolumeRepository) GetDBPath() string {
	return filepath.Join(r.root.Name(), "fin.sqlite3")
}

func (r *NodeLocalVolumeRepository) writeFile(filePath string, data []byte) error {
	file, err := r.root.Create(filePath)
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
	f, err := r.root.Open(pvcFilePath)
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
	f, err := r.root.Open(pvFilePath)
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
	if err := r.root.Mkdir("diff", 0755); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("failed to create base diff directory 'diff': %w", err)
	}

	if err := r.root.Mkdir(getDiffRelPath(snapshotID), 0755); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return model.ErrAlreadyExists
		}
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

func (r *NodeLocalVolumeRepository) RemoveDiffDirRecursively(snapshotID int) error {
	entries := []string{}
	if err := fs.WalkDir(r.root.FS(), getDiffRelPath(snapshotID), func(path string, d fs.DirEntry, err error) error {
		entries = append(entries, path)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	// reverse the order of entries to remove them from leaf to root
	slices.Reverse(entries)

	for _, entry := range entries {
		if err := r.root.Remove(entry); err != nil {
			return fmt.Errorf("failed to remove entry %s: %w", entry, err)
		}
	}

	return nil
}

func (r *NodeLocalVolumeRepository) RemoveRawImage() error {
	if err := r.root.Remove(imageFilePath); err != nil {
		return fmt.Errorf("failed to remove file %s: %w", imageFilePath, err)
	}

	return nil
}

// RemoveOngoingFullBackupFiles removes all files corresponding to ongoing full backup.
// This method returns nil if all files does not exist.
func (r *NodeLocalVolumeRepository) RemoveOngoingFullBackupFiles(snapID int) error {
	if err := r.RemoveDiffDirRecursively(snapID); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove diff dir: %w", err)
	}

	if err := r.RemoveRawImage(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to remove the raw image: %w", err)
	}
	if err := r.root.Remove(pvcFilePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to remove the pvc file: %w", err)
	}
	if err := r.root.Remove(pvFilePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to remove the pv file: %w", err)
	}

	return nil
}

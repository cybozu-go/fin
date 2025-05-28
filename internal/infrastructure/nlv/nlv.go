package nlv

import (
	"fmt"
	"io/fs"
	"os"
	"slices"

	"github.com/cybozu-go/fin/internal/model"
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

func (r *NodeLocalVolumeRepository) GetRootPath() string {
	return r.rootPath
}

func (r *NodeLocalVolumeRepository) WriteFile(filePath string, data []byte) error {
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

func (r *NodeLocalVolumeRepository) Mkdir(dirPath string) error {
	root, err := os.OpenRoot(r.rootPath)
	if err != nil {
		return fmt.Errorf("failed to open node local volume root directory: %w", err)
	}
	defer func() { _ = root.Close() }()

	if err := root.Mkdir(dirPath, 0755); err != nil {
		if os.IsExist(err) {
			return model.ErrAlreadyExists
		}
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

func (r *NodeLocalVolumeRepository) RemoveDirRecursively(dirPath string) error {
	root, err := os.OpenRoot(r.rootPath)
	if err != nil {
		return fmt.Errorf("failed to open root directory: %w", err)
	}
	defer func() { _ = root.Close() }()

	entries := []string{}
	if err := fs.WalkDir(root.FS(), dirPath, func(path string, d fs.DirEntry, err error) error {
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

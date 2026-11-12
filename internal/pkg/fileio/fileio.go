package fileio

import (
	"fmt"
	"os"
)

func SyncData(filename string) error {
	f, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", filename, err)
	}
	defer func() { _ = f.Close() }()

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync %q: %w", filename, err)
	}

	return nil
}

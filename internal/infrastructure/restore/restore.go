package restore

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/cybozu-go/fin/internal/model"
)

type RestoreVolume struct {
	path string
}

var _ model.RestoreVolume = &RestoreVolume{}

func NewRestoreVolume(devPath string) *RestoreVolume {
	return &RestoreVolume{
		path: devPath,
	}
}

func (r *RestoreVolume) GetPath() string {
	return r.path
}

func (r *RestoreVolume) BlkDiscard() error {
	return errors.New("not implemented")
}

func (r *RestoreVolume) ApplyDiff(diffPath string) error {
	return errors.New("not implemented")
}

func (r *RestoreVolume) CopyChunk(rawPath string, index int, chunkSize int64) error {
	rawFile, err := os.Open(rawPath)
	if err != nil {
		return fmt.Errorf("failed to open `%s`: %w", rawPath, err)
	}
	defer func() { _ = rawFile.Close() }()

	resVol, err := os.OpenFile(r.GetPath(), os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("failed to open `%s`: %w", r.GetPath(), err)
	}
	defer func() { _ = resVol.Close() }()

	if _, err := rawFile.Seek(int64(index)*chunkSize, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek `%s` to %d: %w",
			rawPath, int64(index)*chunkSize, err)
	}
	if _, err = resVol.Seek(int64(index)*chunkSize, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek `%s` to %d: %w",
			r.GetPath(), int64(index)*chunkSize, err)
	}

	buf := make([]byte, chunkSize)
	for {
		rn, err := rawFile.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read: %w", err)
		}

		allZero := true
		for _, b := range buf {
			if b != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			return nil
		}

		if rn > 0 {
			wn, err := resVol.Write(buf[:rn])
			if err != nil {
				return fmt.Errorf("failed to write: %w", err)
			}
			if wn != rn {
				return fmt.Errorf("short write: wrote %d bytes, expected %d", wn, rn)
			}
			// FIXME: calculate and store the checksum of `buf` here.
		}
	}

	return nil
}

package restore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/cybozu-go/fin/internal/pkg/csumio"
)

const (
	VolumePath = "/dev/restore"
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

func (r *RestoreVolume) ZeroOut() error {
	cmd := exec.Command("blkdiscard", "-z", r.GetPath())

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to zero out %s: %w: %s", r.GetPath(), err, stderr.String())
	}

	return nil
}

func (r *RestoreVolume) ApplyDiff(diffPath string) error {
	return errors.New("not implemented")
}

func (r *RestoreVolume) CopyChunk(rawPath string, index int, rawImageChunkSize, rawChecksumChunkSize uint64, enableChecksumVerify bool) error {
	rawFile, err := os.Open(rawPath)
	if err != nil {
		return fmt.Errorf("failed to open `%s`: %w", rawPath, err)
	}
	defer func() { _ = rawFile.Close() }()

	var checksumFile *os.File
	var checksumFilePath string
	if enableChecksumVerify {
		checksumFilePath = nlv.ChecksumFilePath(rawPath)
		checksumFile, err = os.Open(checksumFilePath)
		if err != nil {
			return fmt.Errorf("failed to open `%s`: %w", checksumFilePath, err)
		}
		defer func() { _ = checksumFile.Close() }()
	}

	resVol, err := os.OpenFile(r.GetPath(), os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("failed to open `%s`: %w", r.GetPath(), err)
	}
	defer func() { _ = resVol.Close() }()

	dataOffset := int64(index) * int64(rawImageChunkSize)
	if _, err := rawFile.Seek(dataOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek `%s` to %d: %w", rawPath, dataOffset, err)
	}
	if _, err = resVol.Seek(dataOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek `%s` to %d: %w", r.GetPath(), dataOffset, err)
	}

	if enableChecksumVerify && checksumFile != nil {
		csumOffset := dataOffset / int64(rawChecksumChunkSize)
		checksumOffset := csumOffset * int64(csumio.ChecksumLen)
		if _, err := checksumFile.Seek(checksumOffset, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek `%s` to %d: %w", checksumFilePath, checksumOffset, err)
		}
	}

	reader, err := csumio.NewReader(rawFile, checksumFile, int(rawChecksumChunkSize), enableChecksumVerify)
	if err != nil {
		return fmt.Errorf("failed to create checksum reader: %w", err)
	}

	buf := make([]byte, rawImageChunkSize)
	rn, err := io.ReadFull(reader, buf)
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("failed to read: %w", err)
		}
	}

	allZero := true
	for _, b := range buf[:rn] {
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
	}

	return nil
}

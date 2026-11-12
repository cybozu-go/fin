package nlv

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/cybozu-go/fin/internal/pkg/csumio"
	"github.com/cybozu-go/fin/internal/pkg/fileio"
)

func (r *NodeLocalVolumeRepository) StorePart(snapshotID, partIndex int, stream io.ReadCloser, checksumChunkSize uint64) error {
	partPath := r.GetDiffPartPath(snapshotID, partIndex)
	checksumPath := r.GetDiffChecksumPath(snapshotID, partIndex)
	if err := writeDiffPartAndCloseStream(stream, partPath, checksumPath, checksumChunkSize); err != nil {
		return fmt.Errorf("failed to write diff part: %w", err)
	}

	if err := fileio.SyncData(partPath); err != nil {
		return fmt.Errorf("failed to sync %q: %w", partPath, err)
	}

	return nil
}

func writeDiffPartAndCloseStream(stream io.ReadCloser, outputPath, checksumPath string, chunkSize uint64) (err error) {
	defer func() {
		if cerr := stream.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close diff stream: %w", cerr)
		}
	}()

	outputFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file %s: %w", outputPath, err)
	}
	defer func() {
		if cerr := outputFile.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close output file %s: %w", outputPath, cerr)
		}
	}()

	checksumFile, err := os.OpenFile(checksumPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create checksum file %s: %w", checksumPath, err)
	}
	defer func() {
		if cerr := checksumFile.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close checksum file %s: %w", checksumPath, cerr)
		}
	}()

	if chunkSize == 0 {
		return errors.New("diffChecksumChunkSize must be greater than zero")
	}

	writer, err := csumio.NewWriter(outputFile, checksumFile, int(chunkSize))
	if err != nil {
		return fmt.Errorf("failed to create checksum writer: %w", err)
	}
	defer func() {
		if cerr := writer.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close checksum writer: %w", cerr)
		}
	}()

	if _, err := io.Copy(writer, stream); err != nil {
		return fmt.Errorf("failed to write diff to %s: %w", outputPath, err)
	}

	return nil
}

package csumio

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
)

const (
	ChecksumLen      = 8
	minimumChunkSize = 4 * 1024 // 4 KiB
)

type Writer struct {
	dataWriter     io.Writer
	checksumWriter io.Writer
	chunkSize      int
	buf            []byte
}

func NewChecksumWriter(dataFile, checksumFile io.Writer, chunkSize int) (*Writer, error) {
	if chunkSize <= minimumChunkSize && chunkSize%(minimumChunkSize) != 0 {
		return nil, fmt.Errorf("chunksize must be at least %d KiB and a multiple of %d KiB when checksum verification is enabled", minimumChunkSize/1024, minimumChunkSize/1024)
	}
	return &Writer{
		dataWriter:     dataFile,
		checksumWriter: checksumFile,
		chunkSize:      chunkSize,
		buf:            make([]byte, 0, chunkSize),
	}, nil
}

func (cw *Writer) Write(p []byte) (int, error) {
	totalWritten := 0

	for len(p) > 0 {
		n := cw.chunkSize - len(cw.buf)
		toWrite := len(p)
		if toWrite > n {
			toWrite = n
		}

		cw.buf = append(cw.buf, p[:toWrite]...)
		p = p[toWrite:]
		totalWritten += toWrite

		if len(cw.buf) == cw.chunkSize {
			if err := cw.flushChunk(); err != nil {
				return totalWritten, err
			}
		}
	}

	return totalWritten, nil
}

func (cw *Writer) flushChunk() error {
	if len(cw.buf) == 0 {
		return nil
	}

	_, err := cw.dataWriter.Write(cw.buf)
	if err != nil {
		return fmt.Errorf("failed to write data chunk: %w", err)
	}

	checksum := xxhash.Sum64(cw.buf)
	checksumBytes := make([]byte, ChecksumLen)
	binary.LittleEndian.PutUint64(checksumBytes, checksum)

	_, err = cw.checksumWriter.Write(checksumBytes)
	if err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}
	cw.buf = cw.buf[:0]

	return nil
}

func (cw *Writer) Close() error {
	if len(cw.buf) > 0 {
		if err := cw.flushChunk(); err != nil {
			return err
		}
	}
	return nil
}

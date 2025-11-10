package csumwriter

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
)

const (
	ChecksumLen = 8
)

type ChecksumWriter struct {
	dataFile     io.Writer
	checksumFile io.Writer
	chunkSize    int
	buf          []byte
}

func NewChecksumWriter(dataFile, checksumFile io.Writer, chunkSize int) *ChecksumWriter {
	return &ChecksumWriter{
		dataFile:     dataFile,
		checksumFile: checksumFile,
		chunkSize:    chunkSize,
		buf:          make([]byte, 0, chunkSize),
	}
}

func (cw *ChecksumWriter) Write(p []byte) (int, error) {
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

func (cw *ChecksumWriter) flushChunk() error {
	if len(cw.buf) == 0 {
		return nil
	}

	n, err := cw.dataFile.Write(cw.buf)
	if err != nil {
		return fmt.Errorf("failed to write data chunk: %w", err)
	}
	if n != len(cw.buf) {
		return fmt.Errorf("short write to data file: wrote %d, expected %d", n, len(cw.buf))
	}

	checksum := xxhash.Sum64(cw.buf)
	checksumBytes := make([]byte, ChecksumLen)
	binary.LittleEndian.PutUint64(checksumBytes, checksum)

	n, err = cw.checksumFile.Write(checksumBytes)
	if err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}
	if n != ChecksumLen {
		return fmt.Errorf("short write to checksum file: wrote %d, expected %d", n, ChecksumLen)
	}

	cw.buf = cw.buf[:0]
	return nil
}

func (cw *ChecksumWriter) Close() error {
	if len(cw.buf) > 0 {
		if err := cw.flushChunk(); err != nil {
			return err
		}
	}
	return nil
}

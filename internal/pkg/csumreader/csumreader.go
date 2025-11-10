package csumreader

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
)

const (
	ChecksumLen = 8
)

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
)

type ChecksumReader struct {
	dataFile              io.Reader
	checksumFile          io.Reader
	buf                   []byte
	offset                int
	chunkSize             int
	eof                   bool
	disableChecksumVerify bool
}

func NewChecksumReader(dataFile, checksumFile io.Reader, chunkSize int, disableChecksumVerify bool) *ChecksumReader {
	return &ChecksumReader{
		dataFile:              dataFile,
		checksumFile:          checksumFile,
		chunkSize:             chunkSize,
		disableChecksumVerify: disableChecksumVerify,
		buf:                   nil,
	}
}

func (cr *ChecksumReader) Read(p []byte) (int, error) {
	if cr.eof && len(cr.buf) == 0 {
		return 0, io.EOF
	}

	if len(cr.buf) == 0 {
		block := make([]byte, cr.chunkSize)
		n, err := io.ReadFull(cr.dataFile, block)
		if err == io.ErrUnexpectedEOF {
			cr.eof = true
			block = block[:n]
		} else if err == io.EOF {
			return 0, io.EOF
		} else if err != nil {
			return 0, fmt.Errorf("failed to read data block: %w", err)
		}

		if !cr.disableChecksumVerify {
			checksumBytes := make([]byte, ChecksumLen)
			n2, err := io.ReadFull(cr.checksumFile, checksumBytes)
			if err != nil {
				return 0, fmt.Errorf("failed to read checksum: %w", err)
			}
			if n2 != ChecksumLen {
				return 0, fmt.Errorf("invalid checksum length: expected %d, got %d", ChecksumLen, n2)
			}

			expected := binary.LittleEndian.Uint64(checksumBytes)
			actual := xxhash.Sum64(block)
			if expected != actual {
				return 0, fmt.Errorf("%w: expected %016x, got %016x", ErrChecksumMismatch, expected, actual)
			}
		} else {
			checksumBytes := make([]byte, ChecksumLen)
			_, err := io.ReadFull(cr.checksumFile, checksumBytes)
			if err != nil && err != io.EOF {
				return 0, fmt.Errorf("failed to skip checksum: %w", err)
			}
		}

		cr.buf = block
		cr.offset = 0
	}

	n := copy(p, cr.buf[cr.offset:])
	cr.offset += n
	if cr.offset >= len(cr.buf) {
		cr.buf = nil
		cr.offset = 0
	}
	return n, nil
}

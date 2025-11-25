package csumio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
)

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
)

type Reader struct {
	dataReader           io.Reader
	checksumReader       io.Reader
	buf                  []byte
	offset               int
	chunkSize            int
	enableChecksumVerify bool
}

func NewReader(dataFile, checksumFile io.Reader, chunkSize int, enableChecksumVerify bool) (*Reader, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunkSize must be greater than 0")
	}
	if enableChecksumVerify {
		if chunkSize < 4*1024 && chunkSize%(4*1024) != 0 {
			return nil, fmt.Errorf("chunksize must be at least 4 KiB and a multiple of 4 KiB when checksum verification is enabled")
		}
	}
	return &Reader{
		dataReader:           dataFile,
		checksumReader:       checksumFile,
		chunkSize:            chunkSize,
		enableChecksumVerify: enableChecksumVerify,
		buf:                  make([]byte, chunkSize),
	}, nil
}

func (cr *Reader) Read(p []byte) (int, error) {
	if cr.offset == 0 {
		n, err := io.ReadFull(cr.dataReader, cr.buf)
		if err == io.ErrUnexpectedEOF {
			cr.buf = cr.buf[:n]
		} else if err == io.EOF {
			return 0, io.EOF
		} else if err != nil {
			return 0, fmt.Errorf("failed to read data block: %w", err)
		}

		if cr.enableChecksumVerify {
			checksumBytes := make([]byte, ChecksumLen)
			_, err := io.ReadFull(cr.checksumReader, checksumBytes)
			if err != nil {
				return 0, fmt.Errorf("failed to read checksum: %w", err)
			}

			expected := binary.LittleEndian.Uint64(checksumBytes)
			actual := xxhash.Sum64(cr.buf)
			if expected != actual {
				return 0, fmt.Errorf("%w: expected %016x, got %016x", ErrChecksumMismatch, expected, actual)
			}
		}
		// No checksum reading when verification is disabled
	}

	n := copy(p, cr.buf[cr.offset:])
	cr.offset += n
	if cr.offset >= len(cr.buf) {
		cr.offset = 0
	}
	return n, nil
}

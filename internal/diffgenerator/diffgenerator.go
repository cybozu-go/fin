// FIXME: Give me a better package name
package diffgenerator

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
)

type recordType uint8

const (
	recordTypeUpdated recordType = iota
	recordTypeZero
)

// DataRecord represents a Data Record in RBD Incremental Backup format
// (cf. https://docs.ceph.com/en/squid/dev/rbd-diff/).
type DataRecord struct {
	type_  recordType
	offset uint64
	length uint64
	data   []byte
}

// NewZeroDataRecord creates a new DataRecord for Zero data.
func NewZeroDataRecord(offset, length uint64) *DataRecord {
	return &DataRecord{
		type_:  recordTypeZero,
		offset: offset,
		length: length,
	}
}

// NewUpdatedDataRecord creates a new DataRecord for Updated data with the given offset, length, and data.
func NewUpdatedDataRecord(offset, length uint64, data []byte) *DataRecord {
	return &DataRecord{
		type_:  recordTypeUpdated,
		offset: offset,
		length: length,
		data:   data,
	}
}

// NewRandomUpdatedDataRecord creates a new DataRecord for Updated data with the given offset and length,
// filling the data with random bytes of the specified length.
func NewRandomUpdatedDataRecord(offset, length uint64) *DataRecord {
	return NewUpdatedDataRecord(offset, length, nil)
}

type config struct {
	header       string
	fromSnapName string
	toSnapName   string
	imageSize    uint64
	records      []*DataRecord
}

type option func(cfg *config)

func WithHeader(header string) option {
	return func(cfg *config) {
		cfg.header = header
	}
}

func WithFromSnapName(fromSnapName string) option {
	return func(cfg *config) {
		cfg.fromSnapName = fromSnapName
	}
}

func WithToSnapName(toSnapName string) option {
	return func(cfg *config) {
		cfg.toSnapName = toSnapName
	}
}

func WithImageSize(imageSize uint64) option {
	return func(cfg *config) {
		cfg.imageSize = imageSize
	}
}

func WithRecords(records []*DataRecord) option {
	return func(cfg *config) {
		cfg.records = records
	}
}

// Run creates a Reader that generates a diff in the RBD Incremental Backup format.
func Run(opts ...option) (io.Reader, error) {
	// Setup Config
	cfg := &config{
		header:       "rbd diff v1\n",
		fromSnapName: "",
		toSnapName:   "",
		imageSize:    0,
		records:      nil,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// Build chunkReaders
	chunkReaders := []io.Reader{
		bytes.NewBufferString(cfg.header),
	}

	// Append Metadata Records
	metadataChunkReader, err := makeMetadataChunkReader(cfg)
	if err != nil {
		return nil, err
	}
	chunkReaders = append(chunkReaders, metadataChunkReader)

	// Append Data Records
	for _, record := range cfg.records {
		switch record.type_ {
		case recordTypeUpdated:
			r, err := makeUpdatedDataChunkReader(record.offset, record.length, record.data)
			if err != nil {
				return nil, err
			}
			chunkReaders = append(chunkReaders, r)
		case recordTypeZero:
			r, err := makeZeroDataChunkReader(record.offset, record.length)
			if err != nil {
				return nil, err
			}
			chunkReaders = append(chunkReaders, r)
		}
	}

	// Append Final Record
	chunkReaders = append(chunkReaders, bytes.NewBufferString("e"))

	return io.MultiReader(chunkReaders...), nil
}

func makeUpdatedDataChunkReader(offset, length uint64, data []byte) (io.Reader, error) {
	buf := bytes.Buffer{}
	buf.WriteByte('w')

	err := binary.Write(&buf, binary.LittleEndian, offset)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.LittleEndian, length)
	if err != nil {
		return nil, err
	}

	if data == nil {
		chacha8 := io.LimitReader(
			rand.NewChaCha8([32]byte{}), // Use a seeded random generator for reproducibility
			int64(length),
		)
		return io.MultiReader(&buf, chacha8), nil
	}
	if len(data) != int(length) {
		return nil, errors.New("data length does not match specified length")
	}
	buf.Write(data)
	return &buf, nil
}

func makeZeroDataChunkReader(offset, length uint64) (io.Reader, error) {
	buf := bytes.Buffer{}
	buf.WriteByte('z')

	err := binary.Write(&buf, binary.LittleEndian, offset)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.LittleEndian, length)
	if err != nil {
		return nil, err
	}

	return &buf, nil
}

func makeMetadataChunkReader(cfg *config) (io.Reader, error) {
	buf := bytes.Buffer{}
	if cfg.fromSnapName != "" {
		buf.WriteByte('f')
		err := binary.Write(&buf, binary.LittleEndian, int32(len(cfg.fromSnapName)))
		if err != nil {
			return nil, err
		}
		buf.WriteString(cfg.fromSnapName)
	}

	if cfg.toSnapName != "" {
		buf.WriteByte('t')
		err := binary.Write(&buf, binary.LittleEndian, int32(len(cfg.toSnapName)))
		if err != nil {
			return nil, err
		}
		buf.WriteString(cfg.toSnapName)
	}

	if cfg.imageSize != 0 {
		buf.WriteByte('s')
		err := binary.Write(&buf, binary.LittleEndian, int64(cfg.imageSize))
		if err != nil {
			return nil, err
		}
	}

	return &buf, nil
}

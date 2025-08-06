package diffgenerator_test

import (
	"io"
	"testing"

	"github.com/cybozu-go/fin/internal/diffgenerator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerate_Success_EmptyRecords(t *testing.T) {
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(100*1024*1024),
	)
	require.NoError(t, err)

	buf, err := io.ReadAll(reader)
	require.NoError(t, err)

	assert.Equal(t, []byte("rbd diff v1\nt\x06\x00\x00\x00toSnaps\x00\x00\x40\x06\x00\x00\x00\x00e"), buf)
}

func TestGenerate_Success(t *testing.T) {
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(100*1024*1024),
		diffgenerator.WithFromSnapName("fromSnap"),
		diffgenerator.WithHeader("header"),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewRandomUpdatedDataRecord(0, 10),
			diffgenerator.NewZeroDataRecord(10, 20),
		}),
	)
	require.NoError(t, err)
	buf, err := io.ReadAll(reader)
	require.NoError(t, err)

	assert.Equal(t, []byte{
		// HEADER
		'h', 'e', 'a', 'd', 'e', 'r',

		// METADATA RECORDS
		// FROM SNAP
		'f',
		0x8, 0x0, 0x0, 0x0, // 8 == len("fromSnap")
		'f', 'r', 'o', 'm', 'S', 'n', 'a', 'p',
		// TO SNAP
		't',
		0x6, 0x0, 0x0, 0x0, // 6 == len("toSnap")
		't', 'o', 'S', 'n', 'a', 'p',
		// SIZE
		's',
		0x0, 0x0, 0x40, 0x6, 0x0, 0x0, 0x0, 0x0, // 100*1024*1024

		// DATA RECORDS
		// UPDATED DATA
		'w',
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // 0: offset
		0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // 10: length
		0xd9, 0x87, 0x7e, 0xce, 0x6d, 0x36, 0x8a, 0xac, 0x1a, 0x6f, // 10 bytes of random data
		// ZERO DATA
		'z',
		0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // 10: offset
		0x14, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // 20: length

		// FINAL RECORD
		// END
		'e',
	}, buf)
}

package ceph

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"bufio"
	"compress/gzip"
	"io"
	"path/filepath"
	"unsafe"

	"github.com/cybozu-go/fin/internal/diffgenerator"
	"github.com/cybozu-go/fin/internal/pkg/zeroreader"
	"github.com/cybozu-go/fin/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestReadDiffHeaderAndMetadata_full_success(t *testing.T) {
	header, err := readDiffHeaderAndMetadata(bufio.NewReader(openGZFile(t, "testdata/full.gz")))
	assert.NoError(t, err)
	assert.Equal(t, "", header.FromSnapName)
	assert.Equal(t, "snap20", header.ToSnapName)
	assert.Equal(t, uint64(100*1024*1024), header.Size)
}

func TestReadDiffHeaderAndMetadata_diff_success(t *testing.T) {
	header, err := readDiffHeaderAndMetadata(bufio.NewReader(openGZFile(t, "testdata/diff.gz")))
	assert.NoError(t, err)
	assert.Equal(t, "snap20", header.FromSnapName)
	assert.Equal(t, "snap21", header.ToSnapName)
	assert.Equal(t, uint64(100*1024*1024), header.Size)
}

func TestApplyDiffToRawImage_success(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fin")
	require.NoError(t, err)

	gotFilePath := filepath.Join(tempDir, "test-raw.img")
	err = applyDiffToRawImage(gotFilePath, openGZFile(t, "testdata/full.gz"), "", "snap20", 8*1024*1024)
	assert.NoError(t, err)
	fileInfo, err := os.Stat(gotFilePath)
	assert.NoError(t, err)
	assert.Equal(t, int64(16*1024*1024), fileInfo.Size())
	utils.CompareReaders(t, openFileResized(t, gotFilePath, 100*1024*1024), openGZFile(t, "testdata/full-raw-img.gz"))

	err = applyDiffToRawImage(gotFilePath, openGZFile(t, "testdata/diff.gz"), "snap20", "snap21", 8*1024*1024)
	assert.NoError(t, err)
	utils.CompareReaders(t, openFileResized(t, gotFilePath, 100*1024*1024), openGZFile(t, "testdata/diff-raw-img.gz"))
}

func TestApplyDiffToBlockDevice_success(t *testing.T) {
	blockDevicePath := getBlockDevicePathForTest(t)
	zerooutWholeBlockDevice(t, blockDevicePath)

	err := applyDiffToBlockDevice(blockDevicePath, openGZFile(t, "testdata/full.gz"), "", "snap20")
	assert.NoError(t, err)
	utils.CompareReaders(t, openFile(t, blockDevicePath), openGZFile(t, "testdata/full-raw-img.gz"))

	err = applyDiffToBlockDevice(blockDevicePath, openGZFile(t, "testdata/diff.gz"), "snap20", "snap21")
	assert.NoError(t, err)
	utils.CompareReaders(t, openFile(t, blockDevicePath), openGZFile(t, "testdata/diff-raw-img.gz"))
}

func TestZeroFill_success(t *testing.T) {
	f, err := os.CreateTemp("", "test-fin")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()

	// Test ZeroFill() with not clean numbers
	// We arrange a test file with the following configuration.
	// - 0 ~ firstBlockLength: 0xff
	// - firstBlockLength ~ secondBlockLength: 0x00 (By Zerofill())
	// - secondBlockLength ~ fileSize: 0xff
	fileSize := uint64(10 * 1024)
	firstBlockLength := uint64(1*1024 + 1)
	secondBlockLength := uint64(2*1024 + 1)

	buf := make([]byte, fileSize)
	for i := range buf {
		buf[i] = 0xff
	}
	_, err = f.Write(buf)
	require.NoError(t, err)

	err = zeroFill(f, firstBlockLength, secondBlockLength)
	assert.NoError(t, err)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	bufioReader := bufio.NewReader(f)

	for range firstBlockLength {
		readByte, err := bufioReader.ReadByte()
		require.NoError(t, err)
		assert.Equal(t, byte(0xff), readByte)
	}

	for range secondBlockLength {
		readByte, err := bufioReader.ReadByte()
		require.NoError(t, err)
		assert.Equal(t, byte(0), readByte)
	}

	for range fileSize - (firstBlockLength + secondBlockLength) {
		readByte, err := bufioReader.ReadByte()
		require.NoError(t, err)
		assert.Equal(t, byte(0xff), readByte)
	}
}

func TestApplyDiffToRawImage_success_MissingRawImage(t *testing.T) {
	// Description:
	// Success case of diff application to raw image when the raw image file does not exist
	//
	// Arrange:
	// - raw.img does not exist
	// - Incremental data file exists containing UPDATED DATA and ZERO DATA, but no FROM SNAP
	//
	// Act:
	// Call the incremental data file application process with target snapshot name set to empty
	//
	// Assert:
	// All of the following conditions are met:
	// - Process completes successfully
	// - raw.img is newly created with file expansion unit size
	// - raw.img is overwritten for length bytes from offset according to UPDATED DATA
	// - raw.img is overwritten for length bytes from offset with 0 according to ZERO DATA
	// - Areas not included in either UPDATED DATA or ZERO DATA remain unchanged
	//

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(30),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewUpdatedDataRecord(0, 10, []byte("0123456789")),
			diffgenerator.NewZeroDataRecord(10, 20),
		}),
	)
	require.NoError(t, err)

	rawImageFilePath := getRawImagePathForTest(t)

	// Act
	err = applyDiffToRawImage(rawImageFilePath, reader, "", "toSnap", 7)

	// Assert
	assert.NoError(t, err)

	fileInfo, err := os.Stat(rawImageFilePath)
	require.NoError(t, err)
	assert.Equal(t, int64(35), fileInfo.Size())

	got, err := os.ReadFile(rawImageFilePath)
	require.NoError(t, err)
	assert.Equal(
		t,
		// UPDATED DATA ("0123456789") + ZERO DATA (20 bytes) + Filler of expansion unit (5 bytes)
		[]byte("0123456789\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"+
			"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
		got,
	)
}

func TestApplyDiffToRawImage_success_ExistentRawImage(t *testing.T) {
	// Description:
	// Success case of diff application to raw image when the raw image file exists
	//
	// Arrange:
	// - raw.img exists
	// - Incremental data file exists containing UPDATED DATA, ZERO DATA, and FROM SNAP
	//
	// Act:
	// Call the incremental data file application process
	// with target snapshot name matching the FROM SNAP in the incremental data file
	//
	// Assert:
	// All of the following conditions are met:
	// - Process completes successfully
	// - raw.img is overwritten for length bytes from offset according to UPDATED DATA
	// - raw.img is overwritten for length bytes from offset with 0 according to ZERO DATA
	// - Areas not included in either UPDATED DATA or ZERO DATA remain unchanged

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithFromSnapName("fromSnap"),
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(30),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewUpdatedDataRecord(0, 10, []byte("0123456789")),
			diffgenerator.NewZeroDataRecord(10, 20),
		}),
	)
	require.NoError(t, err)

	rawImageFilePath := getRawImagePathForTest(t)
	err = os.WriteFile(rawImageFilePath, bytes.Repeat([]byte{0xff}, 35), 0644)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(rawImageFilePath, reader, "fromSnap", "toSnap", 7)

	// Assert
	assert.NoError(t, err)

	got, err := os.ReadFile(rawImageFilePath)
	require.NoError(t, err)
	assert.Equal(
		t,
		// UPDATED DATA ("0123456789") + ZERO DATA (20 bytes) + old 0xff data (5 bytes)
		[]byte("0123456789\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"+
			"\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff"),
		got,
	)
}

func TestApplyDiffToRawImage_success_EmptyDataRecords(t *testing.T) {
	// Description:
	// Ensure that the application process to a raw image completes successfully even when there are no Data Records
	//
	// Arrange:
	// Incremental data file exists with no Data Records
	//
	// Act:
	// Execute the incremental data application process to a raw image
	//
	// Assert:
	// - the process completes successfully
	// - raw.img is created with the file expansion unit size

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithFromSnapName("fromSnap"),
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(30),
	)
	require.NoError(t, err)

	expansionUnitSize := 7
	rawImageFilePath := getRawImagePathForTest(t)

	// Act
	err = applyDiffToRawImage(rawImageFilePath, reader, "fromSnap", "toSnap", uint64(expansionUnitSize))

	// Assert
	assert.NoError(t, err)

	fileInfo, err := os.Stat(rawImageFilePath)
	require.NoError(t, err)
	assert.Equal(t, int64(expansionUnitSize), fileInfo.Size())
}

func TestApplyDiffToRawImage_success_RawImageExpansion(t *testing.T) {
	// Description:
	// Check if raw.img is expanded in expansion unit size according to the size of incremental data.
	//
	// Arrange:
	// - A raw.img file of size 1KiB exists.
	// - An incremental data file exists where offset + length is 1.5 KiB.
	//
	// Act:
	// Call the apply process using the incremental data file.
	//
	// Assert:
	// - Completes successfully.
	// - The size of raw.img is expanded to 2 KiB.

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithFromSnapName("fromSnap"),
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(2*1024),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewRandomUpdatedDataRecord(512, 1*1024),
		}),
	)
	require.NoError(t, err)
	rawImageFilePath := getRawImagePathForTest(t)
	err = os.WriteFile(rawImageFilePath, bytes.Repeat([]byte{0xff}, 1*1024), 0644)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(rawImageFilePath, reader, "fromSnap", "toSnap", 1*1024)

	// Assert
	require.NoError(t, err)

	fileInfo, err := os.Stat(rawImageFilePath)
	require.NoError(t, err)
	assert.Equal(t, int64(2*1024), fileInfo.Size())
}

func TestApplyDiffToRawImage_success_LengthZero(t *testing.T) {
	// Description:
	// Check that application of incremental data to a raw image completes successfully even if the DATA RECORD length is 0
	//
	// Arrange:
	// Either of the following conditions satisfies:
	// - An incremental data file exists with a ZERO DATA record of length zero
	// - An incremental data file exists with an UPDATED DATA record of length zero
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// - Process completes successfully
	// - The raw.img remains unchanged after application

	testCases := []struct {
		name       string
		dataRecord *diffgenerator.DataRecord
	}{
		{
			name:       "Zero Data with length zero",
			dataRecord: diffgenerator.NewZeroDataRecord(0, 0),
		},
		{
			name:       "Updated Data with length zero",
			dataRecord: diffgenerator.NewRandomUpdatedDataRecord(0, 0),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			rawImageFilePath := getRawImagePathForTest(t)
			err := os.WriteFile(rawImageFilePath, bytes.Repeat([]byte{0xff}, 10), 0644)
			require.NoError(t, err)

			reader, err := diffgenerator.Run(
				diffgenerator.WithFromSnapName("fromSnap"),
				diffgenerator.WithToSnapName("toSnap"),
				diffgenerator.WithImageSize(10),
				diffgenerator.WithRecords([]*diffgenerator.DataRecord{
					tc.dataRecord,
				}),
			)
			require.NoError(t, err)

			// Act
			err = applyDiffToRawImage(rawImageFilePath, reader, "fromSnap", "toSnap", 10)

			// Assert
			assert.NoError(t, err)

			got, err := os.ReadFile(rawImageFilePath)
			require.NoError(t, err)
			assert.Equal(t, bytes.Repeat([]byte{0xff}, 10), got)
		})
	}
}

func TestApplyDiffToRawImage_error_InvalidHeader(t *testing.T) {
	// Description:
	// Check the header of the incremental data file
	//
	// Arrange:
	// An incremental data file A with a header other than "rbd diff v1\n" exists
	//
	// Act:
	// Call the apply process on A
	//
	// Assert:
	// Should terminate abnormally
	//

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(10),
		diffgenerator.WithHeader("invalid header"),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(getRawImagePathForTest(t), reader, "", "toSnap", 10)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToRawImage_error_MissingToSnap(t *testing.T) {
	// Description:
	// Check the metadata of the incremental data file
	//
	// Arrange:
	// TO SNAP does not exist in the METADATA RECORDS of the incremental data file
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithImageSize(10),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(getRawImagePathForTest(t), reader, "", "toSnap", 10)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToRawImage_error_MissingDiffFileName(t *testing.T) {
	// Description:
	// Check when incremental data file is missing during raw image application
	//
	// Arrange:
	// Incremental data file name is missing
	//
	// Act:
	// Call the incremental data application process
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	rbdRepository := NewRBDRepository()

	// Act
	err := rbdRepository.ApplyDiffToRawImage(
		getRawImagePathForTest(t),
		"non existing file",
		"",
		"toSnap",
		4*1024,
	)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToRawImage_error_MissingToSnapNameArg(t *testing.T) {
	// Description:
	// Check when incremental data snapshot name is missing during raw image application
	//
	// Arrange:
	// Incremental data snapshot name is empty
	//
	// Act:
	// Call the incremental data application process
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(10),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(getRawImagePathForTest(t), reader, "", "" /* missing to-snap */, 10)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToRawImage_error_ExpansionUnitSizeNonPositive(t *testing.T) {
	// Description:
	// Check when expansion unit size is 0 or less during raw image application
	//
	// Arrange:
	// File expansion unit size is 0 or a negative value
	//
	// Act:
	// Call the incremental data application process
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(10),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(getRawImagePathForTest(t), reader, "", "toSnap", 0 /* non positive expansion unit size */)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToRawImage_error_FromSnapNameMismatch(t *testing.T) {
	// Description:
	// Check when the FROM SNAP name in the incremental data file is different
	// from the target snapshot name as an argument.
	//
	// Arrange:
	// An incremental data file containing a FROM SNAP name exists.
	//
	// Act:
	// Call the apply process with a target snapshot name different from the FROM SNAP value in the incremental data file.
	//
	// Assert:
	// Should terminate abnormally.

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithFromSnapName("fromSnap1"),
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(10),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(getRawImagePathForTest(t), reader, "fromSnap2", "toSnap", 10)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToRawImage_error_ToSnapNameMismatch(t *testing.T) {
	// Description:
	// Check when the TO SNAP name in the incremental data file is different
	// from the incremental data snapshot name as an argument.
	//
	// Arrange:
	// An incremental data file containing a TO SNAP name exists
	//
	// Act:
	// Call the apply process with an incremental data snapshot name different
	// from the TO SNAP value in the incremental data file
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithFromSnapName("fromSnap"),
		diffgenerator.WithToSnapName("toSnap1"),
		diffgenerator.WithImageSize(10),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(getRawImagePathForTest(t), reader, "fromSnap", "toSnap2", 10)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToRawImage_error_UnsortedDataRecords(t *testing.T) {
	// Description:
	// Check that an error occurs when DATA RECORDS
	// in the incremental data file are not sorted in ascending order by offset address
	//
	// Arrange:
	// An incremental data file exists with DATA RECORDS as follows:
	// 	- UPDATED DATA: offset = 1KiB, length = 1KiB, data = random
	// 	- UPDATED DATA: offset = 0, length = 1KiB, data = random
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(2*1024),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewRandomUpdatedDataRecord(1*1024, 1*1024),
			diffgenerator.NewRandomUpdatedDataRecord(0, 1*1024),
		}),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(getRawImagePathForTest(t), reader, "", "toSnap", 10)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToRawImage_error_OverlappedDataRecords(t *testing.T) {
	// Description:
	// Check that an error occurs when DATA RECORDS
	// in the incremental data file contain overlaps in offset ~ offset + length
	//
	// Arrange:
	// An incremental data file exists with DATA RECORDS as follows:
	// 	- UPDATED DATA: offset = 1KiB, length = 2KiB, data = random
	// 	- UPDATED DATA: offset = 2KiB, length = 1KiB, data = random
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(3*1024),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewRandomUpdatedDataRecord(1*1024, 2*1024),
			diffgenerator.NewRandomUpdatedDataRecord(2*1024, 1*1024),
		}),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToRawImage(getRawImagePathForTest(t), reader, "", "toSnap", 10)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToBlockDevice_success_ExistentFromSnap(t *testing.T) {
	// Description:
	// Success case of diff application to block device when FROM SNAP exists
	//
	// Arrange:
	// - A block device exists
	// - Incremental data file exists containing UPDATED DATA, ZERO DATA and FROM SNAP
	//
	// Act:
	// Call the incremental data file application process with target snapshot name
	//
	// Assert:
	// All of the following conditions are met:
	// - Process completes successfully
	// - Block device is overwritten for length bytes from offset according to UPDATED DATA
	// - Block device is overwritten for length bytes from offset with 0 according to ZERO DATA
	// - Areas not included in either UPDATED DATA or ZERO DATA remain unchanged

	// Arrange
	blockDevicePath := getBlockDevicePathForTest(t)

	reader, err := diffgenerator.Run(
		diffgenerator.WithFromSnapName("fromSnap"),
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(uint64(getBlockDeviceSize(t, blockDevicePath))),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewUpdatedDataRecord(0, 10, []byte("0123456789")),
			diffgenerator.NewZeroDataRecord(10, 20),
		}),
	)
	require.NoError(t, err)

	zerooutWholeBlockDevice(t, blockDevicePath)

	file := openFileWriteOnly(t, blockDevicePath)
	_, err = io.Copy(file, bytes.NewReader(bytes.Repeat([]byte{0xff}, 35)))
	require.NoError(t, err)

	// Act
	err = applyDiffToBlockDevice(blockDevicePath, reader, "fromSnap", "toSnap")

	// Assert
	assert.NoError(t, err)

	file = openFile(t, blockDevicePath)
	head := make([]byte, 35)
	_, err = io.ReadFull(file, head)
	require.NoError(t, err)
	assert.Equal(
		t,
		// UPDATED DATA ("0123456789") + ZERO DATA (20 bytes) + old 0xff data (5 bytes)
		[]byte("0123456789\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"+
			"\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff"),
		head,
	)

	// Ensure the rest of the block device is zeroed out
	utils.CompareReaders(t, file, io.LimitReader(zeroreader.New(), int64(getBlockDeviceSize(t, blockDevicePath)-35)))
}

func TestApplyDiffToBlockDevice_success_MissingFromSnap(t *testing.T) {
	// Description:
	// Success case of diff application to block device when FROM SNAP does not exist
	//
	// Arrange:
	// - A block device exists
	// - Incremental data file exists containing UPDATED DATA and ZERO DATA, but no FROM SNAP
	//
	// Act:
	// Call the incremental data file application process with target snapshot name set to empty
	//
	// Assert:
	// All of the following conditions are met:
	// - Process completes successfully
	// - Block device is overwritten for length bytes from offset according to UPDATED DATA
	// - Block device is overwritten for length bytes from offset with 0 according to ZERO DATA
	// - Areas not included in either UPDATED DATA or ZERO DATA remain unchanged

	// Arrange
	blockDevicePath := getBlockDevicePathForTest(t)

	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(uint64(getBlockDeviceSize(t, blockDevicePath))),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewUpdatedDataRecord(0, 10, []byte("0123456789")),
			diffgenerator.NewZeroDataRecord(10, 20),
		}),
	)
	require.NoError(t, err)

	zerooutWholeBlockDevice(t, blockDevicePath)

	// Act
	err = applyDiffToBlockDevice(blockDevicePath, reader, "", "toSnap")

	// Assert
	assert.NoError(t, err)

	file := openFile(t, blockDevicePath)
	head := make([]byte, 30)
	_, err = io.ReadFull(file, head)
	require.NoError(t, err)
	assert.Equal(
		t,
		// UPDATED DATA ("0123456789") + ZERO DATA (20 bytes)
		[]byte("0123456789\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"+
			"\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
		head,
	)

	// Ensure the rest of the block device is zeroed out
	utils.CompareReaders(t, file, io.LimitReader(zeroreader.New(), int64(getBlockDeviceSize(t, blockDevicePath)-30)))
}

func TestApplyDiffToBlockDevice_success_VariousZeroDataRecords(t *testing.T) {
	// Description:
	// For ZERO DATA, verify that the region offset ~ offset + length is overwritten with 0 on the block device
	//
	// Arrange:
	// This test covers all combinations of three conditions:
	// 1. Whether offset is aligned to sector boundary or not
	// 2. Whether (offset + length) is aligned to sector boundary or not
	// 3. Whether the range offset ~ offset + length includes a complete sector or not
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// - Completes successfully
	// - The region offset ~ offset + length of the block device is overwritten with 0

	blockDevicePath := getBlockDevicePathForTest(t)
	blockDeviceSectorSize := getBlockDeviceSectorSize(t, blockDevicePath)

	testCases := []struct {
		name                 string
		zeroDataRecordOffset uint64
		zeroDataRecordLength uint64
		expected             []byte
	}{
		{
			name:                 "aligned offset and (offset + length)",
			zeroDataRecordOffset: uint64(blockDeviceSectorSize),
			zeroDataRecordLength: uint64(blockDeviceSectorSize),
			expected: bytes.Join(
				[][]byte{
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize),
					bytes.Repeat([]byte{0x00}, blockDeviceSectorSize),
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize),
				},
				nil,
			),
		},
		{
			name:                 "aligned offset, not aligned (offset + length) and including whole sector",
			zeroDataRecordOffset: uint64(blockDeviceSectorSize),
			zeroDataRecordLength: uint64(blockDeviceSectorSize + 1),
			expected: bytes.Join(
				[][]byte{
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize),
					bytes.Repeat([]byte{0x00}, blockDeviceSectorSize+1),
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize-1),
				},
				nil,
			),
		},
		{
			name:                 "aligned offset, not aligned (offset + length) and not including whole sector",
			zeroDataRecordOffset: uint64(blockDeviceSectorSize),
			zeroDataRecordLength: uint64(blockDeviceSectorSize - 1),
			expected: bytes.Join(
				[][]byte{
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize),
					bytes.Repeat([]byte{0x00}, blockDeviceSectorSize-1),
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize+1),
				},
				nil,
			),
		},
		{
			name:                 "not aligned offset, aligned (offset + length) and including whole sector",
			zeroDataRecordOffset: uint64(blockDeviceSectorSize - 1),
			zeroDataRecordLength: uint64(blockDeviceSectorSize + 1),
			expected: bytes.Join(
				[][]byte{
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize-1),
					bytes.Repeat([]byte{0x00}, blockDeviceSectorSize+1),
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize),
				},
				nil,
			),
		},
		{
			name:                 "not aligned offset, aligned (offset + length) and not including whole sector",
			zeroDataRecordOffset: uint64(blockDeviceSectorSize + 1),
			zeroDataRecordLength: uint64(blockDeviceSectorSize - 1),
			expected: bytes.Join(
				[][]byte{
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize+1),
					bytes.Repeat([]byte{0x00}, blockDeviceSectorSize-1),
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize),
				},
				nil,
			),
		},
		{
			name:                 "not aligned offset, not aligned (offset + length) and including whole sector",
			zeroDataRecordOffset: uint64(blockDeviceSectorSize - 1),
			zeroDataRecordLength: uint64(blockDeviceSectorSize + 2),
			expected: bytes.Join(
				[][]byte{
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize-1),
					bytes.Repeat([]byte{0x00}, blockDeviceSectorSize+2),
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize-1),
				},
				nil,
			),
		},
		{
			name:                 "not aligned offset, not aligned (offset + length) and not including whole sector",
			zeroDataRecordOffset: uint64(blockDeviceSectorSize + 1),
			zeroDataRecordLength: uint64(blockDeviceSectorSize - 2),
			expected: bytes.Join(
				[][]byte{
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize+1),
					bytes.Repeat([]byte{0x00}, blockDeviceSectorSize-2),
					bytes.Repeat([]byte{0xff}, blockDeviceSectorSize+1),
				},
				nil,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			zerooutWholeBlockDevice(t, blockDevicePath)
			file := openFileWriteOnly(t, blockDevicePath)
			_, err := io.Copy(file, bytes.NewReader(bytes.Repeat([]byte{0xff}, blockDeviceSectorSize*3)))
			require.NoError(t, err)

			reader, err := diffgenerator.Run(
				diffgenerator.WithToSnapName("toSnap"),
				diffgenerator.WithImageSize(uint64(getBlockDeviceSize(t, blockDevicePath))),
				diffgenerator.WithRecords([]*diffgenerator.DataRecord{
					diffgenerator.NewZeroDataRecord(tc.zeroDataRecordOffset, tc.zeroDataRecordLength),
				}),
			)
			require.NoError(t, err)

			// Act
			err = applyDiffToBlockDevice(blockDevicePath, reader, "", "toSnap")

			// Assert
			assert.NoError(t, err)

			// Verify that the beginning of the block device matches tc.expected
			file = openFile(t, blockDevicePath)
			head := make([]byte, len(tc.expected))
			_, err = io.ReadFull(file, head)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, head)

			// Verify that the remaining part of the block device is filled with zeros
			utils.CompareReaders(
				t,
				file,
				io.LimitReader(zeroreader.New(), int64(getBlockDeviceSize(t, blockDevicePath)-len(tc.expected))),
			)
		})
	}
}

func TestApplyDiffToBlockDevice_success_LengthZero(t *testing.T) {
	// Description:
	// Check that application of incremental data to a block device completes successfully
	// even if the DATA RECORD length is 0
	//
	// Arrange:
	// Either of the following conditions satisfies:
	// - An incremental data file exists with a ZERO DATA record of length zero
	// - An incremental data file exists with an UPDATED DATA record of length zero
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// - Process completes successfully
	// - The block device remains unchanged after application

	testCases := []struct {
		name       string
		dataRecord *diffgenerator.DataRecord
	}{
		{
			name:       "Zero Data with length zero",
			dataRecord: diffgenerator.NewZeroDataRecord(0, 0),
		},
		{
			name:       "Updated Data with length zero",
			dataRecord: diffgenerator.NewRandomUpdatedDataRecord(0, 0),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			blockDevicePath := getBlockDevicePathForTest(t)
			file := openFileWriteOnly(t, blockDevicePath)
			_, err := io.Copy(file, bytes.NewBuffer(bytes.Repeat([]byte{0xff}, 10)))
			require.NoError(t, err)

			reader, err := diffgenerator.Run(
				diffgenerator.WithFromSnapName("fromSnap"),
				diffgenerator.WithToSnapName("toSnap"),
				diffgenerator.WithImageSize(10),
				diffgenerator.WithRecords([]*diffgenerator.DataRecord{
					tc.dataRecord,
				}),
			)
			require.NoError(t, err)

			// Act
			err = applyDiffToBlockDevice(blockDevicePath, reader, "fromSnap", "toSnap")

			// Assert
			assert.NoError(t, err)

			file = openFile(t, blockDevicePath)
			head := make([]byte, 10)
			_, err = io.ReadFull(file, head)
			require.NoError(t, err)
			assert.Equal(t, bytes.Repeat([]byte{0xff}, 10), head)
		})
	}
}

func TestApplyDiffToBlockDevice_error_FromSnapNameMismatch(t *testing.T) {
	// Description:
	// Check when the FROM SNAP name in the incremental data file is different
	// from the target snapshot name as an argument.
	//
	// Arrange:
	// An incremental data file containing a FROM SNAP name exists.
	//
	// Act:
	// Call the apply process with a target snapshot name different from the FROM SNAP value in the incremental data file.
	//
	// Assert:
	// Should terminate abnormally.

	// Arrange
	blockDevicePath := getBlockDevicePathForTest(t)
	reader, err := diffgenerator.Run(
		diffgenerator.WithFromSnapName("fromSnap1"),
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(uint64(getBlockDeviceSize(t, blockDevicePath))),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToBlockDevice(blockDevicePath, reader, "fromSnap2", "toSnap")

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToBlockDevice_error_ToSnapNameMismatch(t *testing.T) {
	// Description:
	// Check when the TO SNAP name in the incremental data file is different
	// from the incremental data snapshot name as an argument.
	//
	// Arrange:
	// An incremental data file containing a TO SNAP name exists
	//
	// Act:
	// Call the apply process with an incremental data snapshot name different
	// from the TO SNAP value in the incremental data file
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	blockDevicePath := getBlockDevicePathForTest(t)
	reader, err := diffgenerator.Run(
		diffgenerator.WithFromSnapName("fromSnap"),
		diffgenerator.WithToSnapName("toSnap1"),
		diffgenerator.WithImageSize(uint64(getBlockDeviceSize(t, blockDevicePath))),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToBlockDevice(blockDevicePath, reader, "fromSnap", "toSnap2")

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToBlockDevice_error_MetadataSizeTooLarge(t *testing.T) {
	// Description:
	// Check that an error occurs when the metadata size in the incremental data file exceeds the block device size
	//
	// Arrange:
	// A block device and incremental data file exist, and the METADATA SIZE
	// in the incremental data file is larger than the block device size.
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	blockDevicePath := getBlockDevicePathForTest(t)
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(uint64(getBlockDeviceSize(t, blockDevicePath)+1)),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToBlockDevice(blockDevicePath, reader, "", "toSnap")

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToBlockDevice_error_MissingBlockDevice(t *testing.T) {
	// Description:
	// Check that the incremental data application process terminates abnormally when the block device does not exist
	//
	// Arrange:
	// A block device does not exist
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(10),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToBlockDevice("missing block device", reader, "", "toSnap")

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToBlockDevice_error_MissingDiffFileName(t *testing.T) {
	// Description:
	// Check when incremental data file is missing during block device application
	//
	// Arrange:
	// Incremental data file name is missing
	//
	// Act:
	// Call the incremental data application process
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	rbdRepository := NewRBDRepository()

	// Act
	err := rbdRepository.ApplyDiffToBlockDevice(
		getBlockDevicePathForTest(t),
		"non existing file",
		"fromSnap",
		"toSnap",
	)

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToBlockDevice_error_MissingToSnapNameArg(t *testing.T) {
	// Description:
	// Check when incremental data snapshot name is missing during block device application.
	//
	// Arrange:
	// An incremental data file with a TO SNAP name exists.
	//
	// Act:
	// Call the incremental data application process with an empty incremental data snapshot name.
	//
	// Assert:
	// Should terminate abnormally.

	// Arrange
	blockDevicePath := getBlockDevicePathForTest(t)
	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(uint64(getBlockDeviceSize(t, blockDevicePath))),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToBlockDevice(blockDevicePath, reader, "", "")

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToBlockDevice_error_UnsortedDataRecords(t *testing.T) {
	// Description:
	// Check that an error occurs when DATA RECORDS
	// in the incremental data file are not sorted in ascending order by offset address
	//
	// Arrange:
	// An incremental data file exists with DATA RECORDS as follows:
	// 	- UPDATED DATA: offset = 1KiB, length = 1KiB, data = random
	// 	- UPDATED DATA: offset = 0, length = 1KiB, data = random
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	blockDevicePath := getBlockDevicePathForTest(t)
	require.GreaterOrEqual(t, getBlockDeviceSize(t, blockDevicePath), 2*1024)

	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(2*1024),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewRandomUpdatedDataRecord(1*1024, 1*1024),
			diffgenerator.NewRandomUpdatedDataRecord(0, 1*1024),
		}),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToBlockDevice(blockDevicePath, reader, "", "toSnap")

	// Assert
	assert.Error(t, err)
}

func TestApplyDiffToBlockDevice_error_OverlappedDataRecords(t *testing.T) {
	// Description:
	// Check that an error occurs when DATA RECORDS
	// in the incremental data file contain overlaps in offset ~ offset + length
	//
	// Arrange:
	// An incremental data file exists with DATA RECORDS as follows:
	// 	- UPDATED DATA: offset = 1KiB, length = 2KiB, data = random
	// 	- UPDATED DATA: offset = 2KiB, length = 1KiB, data = random
	//
	// Act:
	// Call the apply process using the incremental data file
	//
	// Assert:
	// Should terminate abnormally

	// Arrange
	blockDevicePath := getBlockDevicePathForTest(t)
	require.GreaterOrEqual(t, getBlockDeviceSize(t, blockDevicePath), 3*1024)

	reader, err := diffgenerator.Run(
		diffgenerator.WithToSnapName("toSnap"),
		diffgenerator.WithImageSize(3*1024),
		diffgenerator.WithRecords([]*diffgenerator.DataRecord{
			diffgenerator.NewRandomUpdatedDataRecord(1*1024, 2*1024),
			diffgenerator.NewRandomUpdatedDataRecord(2*1024, 1*1024),
		}),
	)
	require.NoError(t, err)

	// Act
	err = applyDiffToBlockDevice(blockDevicePath, reader, "", "toSnap")

	// Assert
	assert.Error(t, err)
}

func getRawImagePathForTest(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "raw.img")
}

func getBlockDevicePathForTest(t *testing.T) string {
	t.Helper()

	blockDevicePath := os.Getenv("TEST_BLOCK_DEV")
	require.NotEmpty(t, blockDevicePath)

	return blockDevicePath
}

func getBlockDeviceSize(t *testing.T, path string) int {
	t.Helper()
	file := openFile(t, path)
	n, err := unix.IoctlGetInt(int(file.Fd()), unix.BLKGETSIZE64)
	require.NoError(t, err)

	return n
}

func getBlockDeviceSectorSize(t *testing.T, path string) int {
	t.Helper()
	file := openFile(t, path)
	n, err := unix.IoctlGetInt(int(file.Fd()), unix.BLKSSZGET)
	require.NoError(t, err)

	return n
}

func openFile(t *testing.T, path string) *os.File {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = file.Close()
	})

	return file
}

type resizedReader struct {
	r io.Reader
	n uint64
}

func newResizedReader(r io.Reader, size uint64) *resizedReader {
	return &resizedReader{r: r, n: size}
}

func (rr *resizedReader) Read(p []byte) (int, error) {
	if rr.n <= 0 {
		return 0, io.EOF
	}
	if uint64(len(p)) > rr.n {
		p = p[0:rr.n]
	}

	n, err := rr.r.Read(p)
	if errors.Is(err, io.EOF) {
		for i := n; i < len(p); i++ {
			p[i] = 0
		}
		err = nil
		n = len(p)
	}
	rr.n -= uint64(n)
	return n, err
}

func TestResizedReader(t *testing.T) {
	rr := newResizedReader(bytes.NewReader([]byte("aaaaa")), 10)
	got, err := io.ReadAll(rr)
	require.NoError(t, err)
	assert.Equal(t, []byte("aaaaa\x00\x00\x00\x00\x00"), got)
}

func openFileResized(t *testing.T, path string, size uint64) io.Reader {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = file.Close()
	})

	resized := newResizedReader(file, size)

	return resized
}

func openFileWriteOnly(t *testing.T, path string) *os.File {
	t.Helper()

	file, err := os.OpenFile(path, os.O_WRONLY, 0644)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = file.Close()
	})

	return file
}

func openGZFile(t *testing.T, path string) io.Reader {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)

	uncompressed, err := gzip.NewReader(file)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = uncompressed.Close()
		_ = file.Close()
	})

	return uncompressed
}

func zerooutWholeBlockDevice(t *testing.T, path string) {
	file := openFileWriteOnly(t, path)
	blockDeviceSize, err := unix.IoctlGetInt(int(file.Fd()), unix.BLKGETSIZE64)
	require.NoError(t, err)
	discardRange := [2]uint64{0, uint64(blockDeviceSize)}
	err = unix.IoctlSetInt(int(file.Fd()), unix.BLKZEROOUT, int(uintptr(unsafe.Pointer(&discardRange[0]))))
	require.NoError(t, err)
}

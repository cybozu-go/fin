package ceph

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"io"
	"path/filepath"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestCreateEmptyRawImage(t *testing.T) {
	repo := &RBDRepository{}
	filePath := "test.img"
	size := 1024 * 1024
	err := repo.CreateEmptyRawImage(filePath, size)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(filePath) })

	info, err := os.Stat(filePath)
	require.NoError(t, err)
	assert.Equal(t, filePath, info.Name())
	assert.Equal(t, int64(size), info.Size())
}

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
	compareReaders(t, openFileResized(t, gotFilePath, 100*1024*1024), openGZFile(t, "testdata/full-raw-img.gz"))

	err = applyDiffToRawImage(gotFilePath, openGZFile(t, "testdata/diff.gz"), "snap20", "snap21", 8*1024*1024)
	assert.NoError(t, err)
	compareReaders(t, openFileResized(t, gotFilePath, 100*1024*1024), openGZFile(t, "testdata/diff-raw-img.gz"))
}

func TestApplyDiffToBlockDevice_success(t *testing.T) {
	blockDevicePath := os.Getenv("TEST_BLOCK_DEV")
	zerooutWholeBlockDevice(t, blockDevicePath)

	err := applyDiffToBlockDevice(blockDevicePath, openGZFile(t, "testdata/full.gz"), "", "snap20")
	assert.NoError(t, err)
	compareReaders(t, openFile(t, blockDevicePath), openGZFile(t, "testdata/full-raw-img.gz"))

	err = applyDiffToBlockDevice(blockDevicePath, openGZFile(t, "testdata/diff.gz"), "snap20", "snap21")
	assert.NoError(t, err)
	compareReaders(t, openFile(t, blockDevicePath), openGZFile(t, "testdata/diff-raw-img.gz"))
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
	fileSize := 10 * 1024
	firstBlockLength := 1*1024 + 1
	secondBlockLength := 2*1024 + 1

	buf := make([]byte, fileSize)
	for i := range buf {
		buf[i] = 0xff
	}
	_, err = f.Write(buf)
	require.NoError(t, err)

	err = zeroFill(f, int64(firstBlockLength), int64(secondBlockLength))
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
	n int64
}

func newResizedReader(r io.Reader, size int64) *resizedReader {
	return &resizedReader{r: r, n: size}
}

func (rr *resizedReader) Read(p []byte) (int, error) {
	if rr.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > rr.n {
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
	rr.n -= int64(n)
	return n, err
}

func TestResizedReader(t *testing.T) {
	rr := newResizedReader(bytes.NewReader([]byte("aaaaa")), 10)
	got, err := io.ReadAll(rr)
	require.NoError(t, err)
	assert.Equal(t, []byte("aaaaa\x00\x00\x00\x00\x00"), got)
}

func openFileResized(t *testing.T, path string, size int64) io.Reader {
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

func compareReaders(t *testing.T, gotReader, expectedReader io.Reader) {
	t.Helper()

	gotHash, err := calcFileHash(gotReader)
	require.NoError(t, err)

	expectedHash, err := calcFileHash(expectedReader)
	require.NoError(t, err)

	assert.Equal(t, expectedHash, gotHash)
}

func calcFileHash(file io.Reader) ([]byte, error) {
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return nil, err
	}
	hash := h.Sum(nil)

	return hash, nil
}

func zerooutWholeBlockDevice(t *testing.T, path string) {
	file := openFileWriteOnly(t, path)
	blockDeviceSize, err := unix.IoctlGetInt(int(file.Fd()), unix.BLKGETSIZE64)
	require.NoError(t, err)
	discardRange := [2]uint64{0, uint64(blockDeviceSize)}
	err = unix.IoctlSetInt(int(file.Fd()), unix.BLKZEROOUT, int(uintptr(unsafe.Pointer(&discardRange[0]))))
	require.NoError(t, err)
}

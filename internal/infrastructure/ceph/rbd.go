package ceph

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"unsafe"

	"github.com/cybozu-go/fin/internal/model"
	"golang.org/x/sys/unix"
)

type RBDRepository struct {
}

var _ model.RBDRepository = &RBDRepository{}

func NewRBDRepository() *RBDRepository {
	return &RBDRepository{}
}

func (r *RBDRepository) CreateSnapshot(poolName, imageName, snapName string) error {
	args := []string{"snap", "create", fmt.Sprintf("%s/%s@%s", poolName, imageName, snapName)}
	_, stderr, err := runRBDCommand(args...)
	if err != nil {
		return fmt.Errorf("failed to create RBD snapshot: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

func (r *RBDRepository) RemoveSnapshot(poolName, imageName, snapName string) error {
	args := []string{"snap", "rm", "--force", fmt.Sprintf("%s/%s@%s", poolName, imageName, snapName)}
	_, stderr, err := runRBDCommand(args...)
	if err != nil {
		return fmt.Errorf("failed to delete RBD snapshot: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

func (r *RBDRepository) ListSnapshots(poolName, imageName string) ([]*model.RBDSnapshot, error) {
	args := []string{"snap", "ls", "--format", "json", fmt.Sprintf("%s/%s", poolName, imageName)}
	stdout, stderr, err := runRBDCommand(args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list RBD snapshots: %w, stderr: %s", err, string(stderr))
	}

	var snapshots []*model.RBDSnapshot
	err = json.Unmarshal(stdout, &snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD snapshots: %w", err)
	}

	return snapshots, nil
}

func (r *RBDRepository) ExportDiff(input *model.ExportDiffInput) error {
	args := []string{"export-diff", "-p", input.PoolName, "--read-offset", strconv.Itoa(input.ReadOffset),
		"--read-length", strconv.Itoa(input.ReadLength)}
	if input.FromSnap != nil {
		args = append(args, "--from-snap", *input.FromSnap)
	}
	args = append(args, "--mid-snap-prefix", input.MidSnapPrefix,
		fmt.Sprintf("%s@%s", input.ImageName, input.TargetSnapName), input.OutputFile)

	_, stderr, err := runRBDCommand(args...)
	if err != nil {
		return fmt.Errorf("failed to export diff: %w, stderr: %s", err, string(stderr))
	}
	return nil
}

func (r *RBDRepository) ApplyDiffToBlockDevice(blockDevicePath, diffFilePath, fromSnapName, toSnapName string) error {
	diffFile, err := os.Open(diffFilePath)
	if err != nil {
		return fmt.Errorf("failed to open diff file: %s: %w", diffFilePath, err)
	}
	defer func() { _ = diffFile.Close() }()

	return applyDiffToBlockDevice(blockDevicePath, diffFile, fromSnapName, toSnapName)
}

func (r *RBDRepository) ApplyDiffToRawImage(rawImageFilePath, diffFilePath, fromSnapName, toSnapName string) error {
	diffFile, err := os.Open(diffFilePath)
	if err != nil {
		return fmt.Errorf("failed to open diff file: %s: %w", diffFilePath, err)
	}
	defer func() { _ = diffFile.Close() }()

	expansionUnitSizeParsed := 100 * 1024 * 1024 * 1024 // 100 GiB by default
	if expansionUnitSize := os.Getenv("FIN_RAW_IMG_EXPANSION_UNIT_SIZE"); expansionUnitSize != "" {
		expansionUnitSizeParsed, err = strconv.Atoi(expansionUnitSize)
		if err != nil {
			return fmt.Errorf("failed to parse FIN_RAW_IMG_EXPANSION_UNIT_SIZE: %w", err)
		}
	}

	return applyDiffToRawImage(rawImageFilePath, diffFile, fromSnapName, toSnapName, uint64(expansionUnitSizeParsed))
}

func applyDiffToBlockDevice(blockDevicePath string, diffFile io.Reader, fromSnapName, toSnapName string) error {
	diffFileReader, diffHeader, err := openDiffDataRecords(diffFile, fromSnapName, toSnapName)
	if err != nil {
		return fmt.Errorf("failed to open diff data records: %w", err)
	}

	blockDeviceFile, err := os.OpenFile(blockDevicePath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open block device: %s: %w", blockDevicePath, err)
	}
	defer func() { _ = blockDeviceFile.Close() }()

	// Check if the block device size is enough
	blockDeviceSize, err := unix.IoctlGetInt(int(blockDeviceFile.Fd()), unix.BLKGETSIZE64)
	if err != nil {
		return fmt.Errorf("failed to get block device size: %s: %w", blockDevicePath, err)
	}
	if uint64(blockDeviceSize) < diffHeader.Size {
		return fmt.Errorf("block device size is smaller than diff size: %s", blockDevicePath)
	}

	if err = applyDiffDataRecords(blockDeviceFile, diffFileReader, blockDevicePath, 0, true); err != nil {
		return fmt.Errorf("failed to process diff data: %w", err)
	}

	return nil
}

func applyDiffToRawImage(
	rawImageFilePath string,
	diffFile io.Reader,
	fromSnapName,
	toSnapName string,
	expansionUnitSize uint64,
) error {
	diffFileReader, _, err := openDiffDataRecords(diffFile, fromSnapName, toSnapName)
	if err != nil {
		return fmt.Errorf("failed to open diff data records: %w", err)
	}

	// Open raw image file
	var rawImgFile *os.File
	if _, err := os.Stat(rawImageFilePath); os.IsNotExist(err) {
		rawImgFile, err = os.OpenFile(rawImageFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file: %s: %w", rawImageFilePath, err)
		}
		defer func() { _ = rawImgFile.Close() }()
		if err := unix.Fallocate(int(rawImgFile.Fd()), 0, 0, int64(expansionUnitSize)); err != nil {
			return fmt.Errorf("failed to fallocate: %s: %w", rawImageFilePath, err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to stat raw image file: %w", err)
	} else {
		rawImgFile, err = os.OpenFile(rawImageFilePath, os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file: %s: %w", rawImageFilePath, err)
		}
		defer func() { _ = rawImgFile.Close() }()
	}

	if err := applyDiffDataRecords(rawImgFile, diffFileReader, rawImageFilePath, expansionUnitSize, false); err != nil {
		return fmt.Errorf("failed to read diff data: %w", err)
	}

	return nil
}

func openDiffDataRecords(diffFile io.Reader, fromSnapName, toSnapName string) (*bufio.Reader, *diffMetadata, error) {
	diffFileReader := bufio.NewReader(diffFile)
	diffHeader, err := readDiffHeaderAndMetadata(diffFileReader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read diff header: %w", err)
	}
	if diffHeader.FromSnapName != fromSnapName || diffHeader.ToSnapName != toSnapName {
		return nil, nil, fmt.Errorf("snapshot names do not match: expected %s -> %s, got %s -> %s",
			fromSnapName, toSnapName, diffHeader.FromSnapName, diffHeader.ToSnapName)
	}

	return diffFileReader, diffHeader, nil
}

func applyDiffDataRecords(
	dstFile *os.File,
	diffFileReader *bufio.Reader,
	dstFilePath string,
	expansionUnitSize uint64,
	isDstBlockDevice bool,
) error {
	stat, err := dstFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat raw image file: %s: %w", dstFilePath, err)
	}
	fileSize := stat.Size()
	prevOffset := uint64(0)
	prevLength := uint64(0)
	for {
		tag, err := diffFileReader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to read tag byte: %w", err)
		}
		switch tag {
		case 'w', 'z': // UPDATED DATA or ZERO DATA
			offset, err := readLE64(diffFileReader)
			if err != nil {
				return fmt.Errorf("failed to read offset: %w", err)
			}
			length, err := readLE64(diffFileReader)
			if err != nil {
				return fmt.Errorf("failed to read length: %w", err)
			}

			// Detect overlapping ranges
			if prevOffset+prevLength > offset {
				return fmt.Errorf("overlapping ranges detected: previous (%d, %d), current (%d, %d)",
					prevOffset, prevLength, offset, length)
			}
			prevOffset = offset
			prevLength = length

			// Expand raw image file if necessary
			if !isDstBlockDevice && offset+length > uint64(fileSize) {
				fileSize = int64(math.Ceil(float64(offset+length)/float64(expansionUnitSize)) * float64(expansionUnitSize))
				if err := unix.Fallocate(int(dstFile.Fd()), 0, 0, fileSize); err != nil {
					return fmt.Errorf("failed to fallocate: %s: %w", dstFilePath, err)
				}
			}

			// Write data to destination file
			if tag == 'w' {
				if _, err := dstFile.Seek(int64(offset), io.SeekStart); err != nil {
					return fmt.Errorf("failed to seek in destination file: %s: %w", dstFilePath, err)
				}
				if _, err := io.CopyN(dstFile, diffFileReader, int64(length)); err != nil {
					return fmt.Errorf("failed to write to destination file: %s: %w", dstFilePath, err)
				}
			} else if isDstBlockDevice {
				err := zerooutBlockDevice(dstFile, offset, length)
				if err != nil {
					return fmt.Errorf("failed to discard block device: %s: %w", dstFilePath, err)
				}
			} else if err := unix.Fallocate(
				int(dstFile.Fd()),
				unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE,
				int64(offset),
				int64(length),
			); err != nil {
				return fmt.Errorf("failed to write zero data to destination file: %s: %w", dstFilePath, err)
			}

		case 'e': // END
			return nil

		default:
			return fmt.Errorf("unexpected tag: %c", tag)
		}
	}
}

func zerooutBlockDevice(dstFile *os.File, offset, length uint64) error {
	blockSize, err := unix.IoctlGetInt(int(dstFile.Fd()), unix.BLKSSZGET)
	if err != nil {
		return fmt.Errorf("failed to get block size: %w", err)
	}

	// Align offset to block size (round up)
	bsMask := uint64(blockSize - 1)
	alignedHead := offset
	if alignedHead&bsMask != 0 {
		alignedHead = alignedHead&^bsMask + uint64(blockSize)
	}

	// Align (offset + length) to block size (round down)
	alignedTail := offset + length
	if alignedTail&bsMask != 0 {
		alignedTail = alignedTail &^ bsMask
	}

	if alignedHead < alignedTail {
		discardRange := [2]uint64{alignedHead, alignedTail - alignedHead}
		if err := unix.IoctlSetInt(
			int(dstFile.Fd()),
			unix.BLKZEROOUT,
			int(uintptr(unsafe.Pointer(&discardRange[0]))),
		); err != nil {
			return fmt.Errorf("failed to discard block device: %w", err)
		}
		if offset != alignedHead {
			if err := zeroFill(dstFile, int64(offset), int64(alignedHead-offset)); err != nil {
				return fmt.Errorf("failed to zero fill block device: %w", err)
			}
		}
		if alignedTail != offset+length {
			if err := zeroFill(dstFile, int64(alignedTail), int64((offset+length)-alignedTail)); err != nil {
				return fmt.Errorf("failed to zero fill block device: %w", err)
			}
		}
	} else if err := zeroFill(dstFile, int64(offset), int64(length)); err != nil {
		return fmt.Errorf("failed to zero fill block device: %w", err)
	}
	return nil
}

func zeroFill(dstFile *os.File, offset, length int64) error {
	if _, err := dstFile.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	if _, err := io.CopyN(dstFile, &zeroReader{}, length); err != nil {
		return fmt.Errorf("failed to write zeroes: %w", err)
	}

	return nil
}

// zeroReader is an implementation of the io.Reader interface that always returns zero bytes.
type zeroReader struct{}

func (r *zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func readLE64(diffFile *bufio.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(diffFile, buf[:]); err != nil {
		return 0, fmt.Errorf("failed to read le64: %w", err)
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}

func readLE32(diffFile *bufio.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(diffFile, buf[:]); err != nil {
		return 0, fmt.Errorf("failed to read le32: %w", err)
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

type diffMetadata struct {
	FromSnapName string
	ToSnapName   string
	Size         uint64
}

func readDiffHeaderAndMetadata(diffFile *bufio.Reader) (*diffMetadata, error) {
	// Check Header
	headerBytes := make([]byte, 12)
	if _, err := io.ReadFull(diffFile, headerBytes); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	if header := string(headerBytes); header != "rbd diff v1\n" {
		return nil, fmt.Errorf("invalid header: %s", header)
	}

	// Read diff metadata
	diffMetadata := &diffMetadata{}
	for {
		tag, err := diffFile.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read tag: %w", err)
		}

		switch tag {
		case 'f', 't': // FROM SNAP or TO SNAP
			length, err := readLE32(diffFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read name length: %w", err)
			}

			nameBytes := make([]byte, length)
			if _, err := io.ReadFull(diffFile, nameBytes); err != nil {
				return nil, fmt.Errorf("failed to read name: %w", err)
			}

			if tag == 'f' {
				diffMetadata.FromSnapName = string(nameBytes)
			} else {
				diffMetadata.ToSnapName = string(nameBytes)
			}

		case 's': // SIZE
			var err error
			diffMetadata.Size, err = readLE64(diffFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read size: %w", err)
			}

		default:
			if err := diffFile.UnreadByte(); err != nil {
				return nil, fmt.Errorf("failed to unread byte: %w", err)
			}
			return diffMetadata, nil
		}
	}
}

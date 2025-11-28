package ceph

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/cybozu-go/fin/internal/pkg/csumio"
	"github.com/cybozu-go/fin/internal/pkg/zeroreader"
	"golang.org/x/sys/unix"
)

var (
	DefaultExpansionUnitSize uint64 = 100 * 1024 * 1024 * 1024 // 100 GiB
	ErrChecksumMismatch             = errors.New("checksum mismatch")
	csumZero                 uint64
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

func (r *RBDRepository) ExportDiff(input *model.ExportDiffInput) (io.ReadCloser, error) {
	args := []string{
		"export-diff",
		"-p", input.PoolName,
		"--read-offset", strconv.FormatUint(input.ReadOffset, 10),
		"--read-length", strconv.FormatUint(input.ReadLength, 10),
	}
	if input.FromSnap != nil {
		args = append(args, "--from-snap", *input.FromSnap)
	}
	args = append(args, "--mid-snap-prefix", input.MidSnapPrefix,
		fmt.Sprintf("%s@%s", input.ImageName, input.TargetSnapName), "-")

	cmd := exec.Command("rbd", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get export-diff stdout: %w", err)
	}
	stderrBuf := &bytes.Buffer{}
	cmd.Stderr = stderrBuf
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		return nil, fmt.Errorf("failed to start export-diff: %w, stderr: %s", err, stderrBuf.String())
	}
	return &commandReadCloser{
		ReadCloser: stdout,
		cmd:        cmd,
		op:         strings.Join(slices.Concat([]string{"rbd"}, cmd.Args), " "),
		stderr:     stderrBuf,
	}, nil
}

type commandReadCloser struct {
	io.ReadCloser
	cmd    *exec.Cmd
	op     string
	stderr *bytes.Buffer
}

func (c *commandReadCloser) Close() error {
	if c.ReadCloser != nil {
		if _, err := io.Copy(io.Discard, c.ReadCloser); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("failed to drain %s output: %w", c.op, err)
		}
	}
	err := c.cmd.Wait()
	if err != nil {
		return fmt.Errorf("failed to %s: %w, stderr: %s", c.op, err, c.stderr.String())
	}
	return nil
}

func (r *RBDRepository) ApplyDiffToBlockDevice(blockDevicePath, diffFilePath, fromSnapName, toSnapName string, diffChecksumChunkSize uint64) error {
	diffFile, err := os.Open(diffFilePath)
	if err != nil {
		return fmt.Errorf("failed to open diff file: %s: %w", diffFilePath, err)
	}
	defer func() { _ = diffFile.Close() }()

	diffChecksumPath := nlv.ChecksumFilePath(diffFilePath)
	diffChecksumFile, err := os.Open(diffChecksumPath)
	if err != nil {
		return fmt.Errorf("failed to open checksum file: %s: %w", diffChecksumPath, err)
	}
	defer func() { _ = diffChecksumFile.Close() }()
	diffReader, err := csumio.NewReader(diffFile, diffChecksumFile, int(diffChecksumChunkSize), false)
	if err != nil {
		return fmt.Errorf("failed to create checksum reader: %w", err)
	}

	if err := applyDiffToBlockDevice(blockDevicePath, diffReader, fromSnapName, toSnapName); err != nil {
		return err
	}
	return nil
}

func (r *RBDRepository) ApplyDiffToRawImage(
	rawImageFilePath,
	diffFilePath,
	fromSnapName,
	toSnapName string,
	expansionUnitSize uint64,
	rawChecksumChunkSize,
	diffChecksumChunkSize uint64,
	enableChecksumVerify bool,
) error {
	f, err := os.Open(diffFilePath)
	if err != nil {
		return fmt.Errorf("failed to open diff file: %s: %w", diffFilePath, err)
	}
	defer func() { _ = f.Close() }()

	var cf *os.File
	var diffFile io.Reader = f
	if enableChecksumVerify {
		checksumFilePath := getChecksumFilePath(diffFilePath)
		cf, err = os.Open(checksumFilePath)
		if err != nil {
			return fmt.Errorf("failed to open checksum file: %s: %w", checksumFilePath, err)
		}
		defer func() { _ = cf.Close() }()
	}

	diffFile, err = csumio.NewReader(diffFile, cf, int(diffChecksumChunkSize), enableChecksumVerify)
	if err != nil {
		return fmt.Errorf("failed to create csumreader: %w", err)
	}

	return applyDiffToRawImage(
		rawImageFilePath,
		diffFile,
		fromSnapName,
		toSnapName,
		expansionUnitSize,
		rawChecksumChunkSize,
		enableChecksumVerify,
	)
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

	if err = applyDiffDataRecords(blockDeviceFile, diffFileReader, 0, true, false, 0, nil); err != nil {
		return fmt.Errorf("failed to apply diff records to %s: %w", blockDevicePath, err)
	}

	return nil
}

func applyDiffToRawImage(
	rawImageFilePath string,
	diffFile io.Reader,
	fromSnapName,
	toSnapName string,
	expansionUnitSize,
	rawChecksumChunkSize uint64,
	enableChecksumVerify bool,
) error {
	csumZero = calcZeroChecksum(rawChecksumChunkSize)

	diffFileReader, _, err := openDiffDataRecords(diffFile, fromSnapName, toSnapName)
	if err != nil {
		return fmt.Errorf("failed to open diff data records: %w", err)
	}

	var rawImgFile *os.File
	var rawChecksumFile *os.File

	rawStat, err := os.Stat(rawImageFilePath)
	rawExist := !os.IsNotExist(err)
	if err != nil && rawExist {
		return fmt.Errorf("failed to stat raw image file: %w", err)
	}

	var rawCreationIncomplete bool
	if rawExist && enableChecksumVerify {
		rawCreationIncomplete, err = isRawImageCreationIncomplete(rawImageFilePath, rawStat, expansionUnitSize, rawChecksumChunkSize)
		if err != nil {
			return err
		}
	}

	if !rawExist || rawCreationIncomplete {
		if enableChecksumVerify {
			checksumFilePath := getChecksumFilePath(rawImageFilePath)
			rawChecksumFile, err = os.OpenFile(checksumFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				return fmt.Errorf("failed to create checksum file: %s: %w", checksumFilePath, err)
			}
			defer func() { _ = rawChecksumFile.Close() }()

			numChunks := expansionUnitSize / rawChecksumChunkSize
			if expansionUnitSize%rawChecksumChunkSize != 0 {
				numChunks++
			}
			for i := uint64(0); i < numChunks; i++ {
				if err := writeChecksumAtChunk(rawChecksumFile, i, csumZero); err != nil {
					return fmt.Errorf("failed to initialize checksum file: %w", err)
				}
			}
			if err := rawChecksumFile.Sync(); err != nil {
				return fmt.Errorf("failed to sync checksum file: %w", err)
			}
		}
		flag := os.O_CREATE | os.O_TRUNC
		if enableChecksumVerify {
			flag |= os.O_RDWR
		} else {
			flag |= os.O_WRONLY
		}
		rawImgFile, err = os.OpenFile(rawImageFilePath, flag, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file: %s: %w", rawImageFilePath, err)
		}
		defer func() { _ = rawImgFile.Close() }()

		if err := unix.Fallocate(int(rawImgFile.Fd()), 0, 0, int64(expansionUnitSize)); err != nil {
			return fmt.Errorf("failed to fallocate: %s: %w", rawImageFilePath, err)
		}
		if err := rawImgFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync %s: %w", rawImageFilePath, err)
		}
	} else {
		rawImgFile, err = os.OpenFile(rawImageFilePath, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file: %s: %w", rawImageFilePath, err)
		}
		defer func() { _ = rawImgFile.Close() }()

		// Open checksum file if verification is enabled
		if enableChecksumVerify {
			checksumFilePath := getChecksumFilePath(rawImageFilePath)
			rawChecksumFile, err = os.OpenFile(checksumFilePath, os.O_RDWR, 0644)
			if err != nil {
				return fmt.Errorf("failed to open checksum file %s: %w", checksumFilePath, err)
			}
			defer func() { _ = rawChecksumFile.Close() }()
		}
	}

	err = applyDiffDataRecords(
		rawImgFile,
		diffFileReader,
		expansionUnitSize,
		false,
		enableChecksumVerify,
		rawChecksumChunkSize,
		rawChecksumFile,
	)
	if err != nil {
		return fmt.Errorf("failed to apply diff records to %q: %w", rawImageFilePath, err)
	}

	return nil
}

func openDiffDataRecords(diffFile io.Reader, fromSnapName, toSnapName string) (*bufio.Reader, *diffMetadata, error) {
	diffFileReader := bufio.NewReader(diffFile)
	diffHeader, err := readDiffHeaderAndMetadata(diffFileReader)
	if err != nil {
		if errors.Is(err, csumio.ErrChecksumMismatch) {
			err = errors.Join(err, ErrChecksumMismatch)
		}
		return nil, nil, fmt.Errorf("failed to read diff header: %w", err)
	}
	if diffHeader.FromSnapName != fromSnapName || diffHeader.ToSnapName != toSnapName {
		return nil, nil, fmt.Errorf("snapshot names do not match: expected %s -> %s, got %s -> %s",
			fromSnapName, toSnapName, diffHeader.FromSnapName, diffHeader.ToSnapName)
	}

	return diffFileReader, diffHeader, nil
}

//nolint:gocyclo
func applyDiffDataRecords(
	dstFile *os.File,
	diffFileReader *bufio.Reader,
	expansionUnitSize uint64,
	isDstBlockDevice bool,
	enableChecksumVerification bool,
	rawChecksumChunkSize uint64,
	rawChecksumFile *os.File,
) error {
	if enableChecksumVerification {
		if isDstBlockDevice {
			return fmt.Errorf("checksum verification is not supported for block devices")
		}
		if rawChecksumChunkSize == 0 {
			return fmt.Errorf("raw checksum chunk size must be > 0 when checksum verification is enabled")
		}
		if rawChecksumFile == nil {
			return fmt.Errorf("checksum file must be provided when checksum verification is enabled")
		}
	}
	stat, err := dstFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat destination file: %w", err)
	}
	fileSize := stat.Size()
	prevOffset := uint64(0)
	prevLength := uint64(0)

	for {
		tag, err := diffFileReader.ReadByte()
		if err != nil {
			if errors.Is(err, csumio.ErrChecksumMismatch) {
				return fmt.Errorf("failed to read tag byte: %w: %w", ErrChecksumMismatch, err)
			}
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
				return fmt.Errorf("overlapping or unsorted ranges detected: previous (%d, %d), current (%d, %d)",
					prevOffset, prevLength, offset, length)
			}
			prevOffset = offset
			prevLength = length

			// Expand raw image file if necessary
			if !isDstBlockDevice && offset+length > uint64(fileSize) {
				targetFileSize := int64(math.Ceil(float64(offset+length)/float64(expansionUnitSize)) * float64(expansionUnitSize))

				if enableChecksumVerification {
					info, err := rawChecksumFile.Stat()
					if err != nil {
						return fmt.Errorf("failed to stat checksum file: %w", err)
					}
					currentChunks := uint64(info.Size()) / csumio.ChecksumLen
					neededChunks := uint64(targetFileSize) / rawChecksumChunkSize
					if uint64(targetFileSize)%rawChecksumChunkSize != 0 {
						neededChunks++
					}
					for currentChunks < neededChunks {
						if err := writeChecksumAtChunk(rawChecksumFile, currentChunks, csumZero); err != nil {
							return fmt.Errorf("failed to extend checksum file: %w", err)
						}
						currentChunks++
					}
					if err := rawChecksumFile.Sync(); err != nil {
						return fmt.Errorf("failed to sync checksum file: %w", err)
					}
				}
				fileSize = targetFileSize
				if err := unix.Fallocate(int(dstFile.Fd()), 0, 0, fileSize); err != nil {
					return fmt.Errorf("failed to fallocate raw image file: %w", err)
				}
			}

			if enableChecksumVerification {
				if length == 0 {
					continue
				}

				startChunkIndex := offset / rawChecksumChunkSize
				endChunkIndex := (offset + length - 1) / rawChecksumChunkSize

				for chunkIndex := startChunkIndex; chunkIndex <= endChunkIndex; chunkIndex++ {
					chunkStartPos := chunkIndex * rawChecksumChunkSize
					chunkEndPos := chunkStartPos + rawChecksumChunkSize
					writeStartPos := max(offset, chunkStartPos)
					writeEndPos := min(offset+length, chunkEndPos)
					writeSize := writeEndPos - writeStartPos

					if writeSize == rawChecksumChunkSize {
						if err := processEntireChunkWrite(
							dstFile,
							rawChecksumFile,
							diffFileReader,
							tag,
							chunkIndex,
							chunkStartPos,
							rawChecksumChunkSize,
						); err != nil {
							return err
						}
					} else {
						if err := processPartialChunkWrite(
							dstFile,
							rawChecksumFile,
							diffFileReader,
							tag,
							chunkIndex,
							chunkStartPos,
							writeStartPos,
							writeSize,
							rawChecksumChunkSize,
						); err != nil {
							return err
						}
					}
				}

				continue
			}

			// Write data to destination file
			if tag == 'w' {
				if _, err := dstFile.Seek(int64(offset), io.SeekStart); err != nil {
					return fmt.Errorf("failed to seek in destination file: %w", err)
				}
				if _, err := io.CopyN(dstFile, diffFileReader, int64(length)); err != nil {
					return fmt.Errorf("failed to write to destination file: %w", err)
				}
			} else if isDstBlockDevice {
				err := zerooutBlockDevice(dstFile, offset, length)
				if err != nil {
					return fmt.Errorf("failed to discard block device: %w", err)
				}
			} else if length > 0 {
				if err := unix.Fallocate(
					int(dstFile.Fd()),
					unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE,
					int64(offset),
					int64(length),
				); err != nil {
					return fmt.Errorf("failed to write zero data to destination file: %w", err)
				}
			}

		case 'e': // END
			return nil

		default:
			return fmt.Errorf("unexpected tag: %c", tag)
		}
	}
}

func processEntireChunkWrite(
	dstFile *os.File,
	dstChecksumFile *os.File,
	diffFileReader *bufio.Reader,
	tag byte,
	chunkIndex uint64,
	chunkStartPos uint64,
	rawChecksumChunkSize uint64,
) error {
	if tag == 'w' { // UPDATED DATA
		chunkData := make([]byte, rawChecksumChunkSize)
		_, err := io.ReadFull(diffFileReader, chunkData)
		if err != nil {
			if errors.Is(err, csumio.ErrChecksumMismatch) {
				return fmt.Errorf("failed to read chunk data: %w: %w", ErrChecksumMismatch, err)
			}
			return fmt.Errorf("failed to read chunk data: %w", err)
		}

		csumUpdated := xxhash.Sum64(chunkData)

		// Update checksum file
		if err := writeChecksumAtChunk(dstChecksumFile, chunkIndex, csumUpdated); err != nil {
			return err
		}

		// Update raw.img
		if _, err := dstFile.Seek(int64(chunkStartPos), io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek in destination file: %w", err)
		}
		if _, err := dstFile.Write(chunkData); err != nil {
			return fmt.Errorf("failed to write to destination file: %w", err)
		}
	} else { // ZERO DATA
		// Update checksum file
		if err := writeChecksumAtChunk(dstChecksumFile, chunkIndex, csumZero); err != nil {
			return err
		}

		// Write zeros using fallocate
		if err := unix.Fallocate(
			int(dstFile.Fd()),
			unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE,
			int64(chunkStartPos),
			int64(rawChecksumChunkSize),
		); err != nil {
			return fmt.Errorf("failed to write zero data: %w", err)
		}
	}

	return nil
}

func processPartialChunkWrite(
	rawFile *os.File,
	rawChecksumFile *os.File,
	diffFileReader *bufio.Reader,
	tag byte,
	chunkIndex uint64,
	chunkStartPos uint64,
	writeStartPos uint64,
	writeSize uint64,
	rawChecksumChunkSize uint64,
) error {
	// Read the raw image checksum and its current chunk contents.
	csumStored, err := readChecksumAtIndex(rawChecksumFile, chunkIndex)
	if err != nil {
		return err
	}
	chunkBuf := make([]byte, rawChecksumChunkSize)
	if _, err := rawFile.Seek(int64(chunkStartPos), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to chunk start: %w", err)
	}
	if _, err := io.ReadFull(rawFile, chunkBuf); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("failed to read current chunk data: %w", err)
		}
	}

	csumCurrent := xxhash.Sum64(chunkBuf)

	// Compute the offset between chunkStartPos (chunk start) and writeStartPos (write start)
	bufOffset := writeStartPos - chunkStartPos

	// Update chunkBuf according to record type
	if tag == 'w' { // UPDATED DATA
		writeBuf := chunkBuf[bufOffset : bufOffset+writeSize]
		if _, err := io.ReadFull(diffFileReader, writeBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf(
					"diff data truncated (chunk=%d, needed=%d): %w",
					chunkIndex, writeSize, err,
				)
			}
			if errors.Is(err, csumio.ErrChecksumMismatch) {
				return fmt.Errorf("failed to read partial chunk data: %w: %w", ErrChecksumMismatch, err)
			}
			return fmt.Errorf("failed to read partial chunk data: %w", err)
		}
	} else { // ZERO DATA
		for i := bufOffset; i < bufOffset+writeSize; i++ {
			chunkBuf[i] = 0
		}
	}

	csumUpdated := xxhash.Sum64(chunkBuf)

	// Crash-safe update method: csumStored reflects raw.img.csum, csumCurrent is the data we read,
	// and csumUpdated is what the data will look like after applying the diff. Comparing them lets
	// retries recover cleanly after crashes without extra journaling.
	if csumStored == csumCurrent && csumCurrent == csumUpdated {
		// Both files already updated in a previous run; nothing left to do.
		return nil
	} else if csumStored == csumUpdated && csumCurrent != csumUpdated {
		// Checksum already advanced; data will be rewritten below.
	} else if csumStored == csumCurrent && csumUpdated != csumCurrent {
		// Data and checksum both still old; proceed with the write.
	} else {
		return ErrChecksumMismatch
	}

	// Update the checksum first.
	if err := writeChecksumAtChunk(rawChecksumFile, chunkIndex, csumUpdated); err != nil {
		return err
	}

	// Ensure the checksum is persisted to storage.
	if err := rawChecksumFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync checksum file: %w", err)
	}

	// Once the checksum is persisted we can safely overwrite the raw.img.
	if _, err := rawFile.Seek(int64(writeStartPos), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to write start: %w", err)
	}
	if _, err := rawFile.Write(chunkBuf[bufOffset : bufOffset+writeSize]); err != nil {
		return fmt.Errorf("failed to write updated chunk data: %w", err)
	}

	return nil
}

func writeChecksumAtChunk(checksumFile *os.File, chunkIndex uint64, checksum uint64) error {
	buf := make([]byte, csumio.ChecksumLen)
	binary.LittleEndian.PutUint64(buf, checksum)
	if _, err := checksumFile.Seek(int64(chunkIndex*csumio.ChecksumLen), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek in checksum file: %w", err)
	}
	if _, err := checksumFile.Write(buf); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}
	return nil
}

func readChecksumAtIndex(checksumFile *os.File, chunkIndex uint64) (uint64, error) {
	buf := make([]byte, csumio.ChecksumLen)
	if _, err := checksumFile.Seek(int64(chunkIndex*csumio.ChecksumLen), io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek in checksum file: %w", err)
	}
	if _, err := io.ReadFull(checksumFile, buf); err != nil {
		return 0, fmt.Errorf("failed to read checksum: %w", err)
	}
	return binary.LittleEndian.Uint64(buf), nil
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
			if err := zeroFill(dstFile, offset, alignedHead-offset); err != nil {
				return fmt.Errorf("failed to zero fill block device: %w", err)
			}
		}
		if alignedTail != offset+length {
			if err := zeroFill(dstFile, alignedTail, (offset+length)-alignedTail); err != nil {
				return fmt.Errorf("failed to zero fill block device: %w", err)
			}
		}
	} else if err := zeroFill(dstFile, offset, length); err != nil {
		return fmt.Errorf("failed to zero fill block device: %w", err)
	}
	return nil
}

func zeroFill(dstFile *os.File, offset, length uint64) error {
	if _, err := dstFile.Seek(int64(offset), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	if _, err := io.CopyN(dstFile, zeroreader.New(), int64(length)); err != nil {
		return fmt.Errorf("failed to write zeroes: %w", err)
	}

	return nil
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

func calcZeroChecksum(chunkSize uint64) uint64 {
	zeroBuf := make([]byte, chunkSize)
	return xxhash.Sum64(zeroBuf)
}

func getChecksumFilePath(imageFilePath string) string {
	return imageFilePath + ".csum"
}

// isRawImageCreationIncomplete judges whether new creation or recreation of raw.img and its checksum file is needed
// based on their sizes. Among the following cases, only case 1 returns true:
//
// case 1: rawChunks == 0 && checksumChunks > 0 && checksumChunks <= expansionUnitChunks
//
//	raw.img was never created or is empty, but a checksum file exists. This indicates the previous
//	creation attempt was interrupted after creating the checksum file but before or during raw.img creation.
//	Both files must be recreated.
//
// case 2: expansionUnitChunks <= rawChunks && rawChunks < checksumChunks
//
//	raw.img exists but is smaller than what the checksum file indicates. This means the previous creation
//	was interrupted while writing raw.img. Recreation is skipped; applyDiffDataRecords should complete
//	the expansion (returns false).
//
// other cases:
//
//	All other cases indicate the files are in a consistent state, so return false and skip recreation.
func isRawImageCreationIncomplete(rawImageFilePath string, rawStat os.FileInfo, expansionUnitSize, rawChecksumChunkSize uint64) (bool, error) {
	expansionUnitChunks := expansionUnitSize / rawChecksumChunkSize

	checksumFilePath := getChecksumFilePath(rawImageFilePath)
	checksumStat, err := os.Stat(checksumFilePath)
	if os.IsNotExist(err) {
		return false, ErrChecksumMismatch
	} else if err != nil {
		return false, fmt.Errorf("failed to stat checksum file %s: %w", checksumFilePath, err)
	}

	rawChunks := (uint64(rawStat.Size()) + rawChecksumChunkSize - 1) / rawChecksumChunkSize
	checksumChunks := uint64(checksumStat.Size()) / csumio.ChecksumLen

	// This case means raw.img and its checksum were never created at all, or the previous run stopped midway. Both files must be recreated.
	if uint64(0) == rawChunks && rawChunks < checksumChunks && checksumChunks <= expansionUnitChunks {
		return true, nil
	}

	return false, nil
}

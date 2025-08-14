package fake

import (
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"time"

	"github.com/cybozu-go/fin/internal/diffgenerator"
	"github.com/cybozu-go/fin/internal/infrastructure/ceph"
	"github.com/cybozu-go/fin/internal/model"
)

type writtenHistory struct {
	snapshotID int
	volumeSize uint64
	offset     uint64
	data       []byte
}

type RBDRepository2 struct {
	r                   *rand.ChaCha8
	blockSize           uint64
	super               model.RBDRepository
	poolName, imageName string
	snapshots           []*model.RBDSnapshot
	writtenHistories    []*writtenHistory
}

var _ model.RBDRepository = &RBDRepository2{}

func NewRBDRepository2(poolName, imageName string) *RBDRepository2 {
	s := make([]byte, 32)
	_, err := cryptorand.Read(s)
	if err != nil {
		panic(fmt.Sprintf("failed to generate random seed for RBDRepository2: %v", err))
	}
	var seed [32]byte
	copy(seed[:], s)

	return &RBDRepository2{
		r:         rand.NewChaCha8(seed),
		blockSize: 1024,
		super:     ceph.NewRBDRepository(),
		poolName:  poolName,
		imageName: imageName,
		// snapshots having snapshot ID starting from 1
		snapshots:        []*model.RBDSnapshot{},
		writtenHistories: []*writtenHistory{},
	}
}

func (r *RBDRepository2) CreateSnapshotWithRandomData(snapName string, volumeSize uint64) (
	*model.RBDSnapshot, []byte, error) {
	if volumeSize%r.blockSize != 0 {
		return nil, nil, fmt.Errorf("volume size must be a multiple of block size")
	}
	if len(r.snapshots) != 0 && r.snapshots[len(r.snapshots)-1].Size > volumeSize {
		return nil, nil, fmt.Errorf("new snapshot size must be greater than or equal to the last snapshot size")
	}

	// Generate random data for the snapshot
	p1 := rand.Uint64() % volumeSize
	p2 := rand.Uint64() % volumeSize
	if p1 > p2 {
		p1, p2 = p2, p1
	}
	offset := p1
	length := p2 - p1
	data := make([]byte, length)
	_, err := r.r.Read(data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate random data for snapshot: %v", err)
	}

	snapshotID := len(r.snapshots) + 1
	r.writtenHistories = append(r.writtenHistories, &writtenHistory{
		snapshotID: snapshotID,
		volumeSize: volumeSize,
		offset:     offset,
		data:       data,
	})

	// Create the snapshot
	snapshot := &model.RBDSnapshot{
		ID:        snapshotID,
		Name:      snapName,
		Size:      volumeSize,
		Timestamp: model.NewRBDTimeStamp(time.Now()),
	}
	r.snapshots = append(r.snapshots, snapshot)

	volume, _, err := r.getSnapshotVolume(snapshotID)
	return snapshot, volume, err
}

func (r *RBDRepository2) ListSnapshots(poolName, imageName string) ([]*model.RBDSnapshot, error) {
	if poolName != r.poolName || imageName != r.imageName {
		return nil, model.ErrNotFound
	}
	return r.snapshots, nil
}

func (r *RBDRepository2) ExportDiff(input *model.ExportDiffInput) error {
	if input.PoolName != r.poolName {
		return fmt.Errorf("pool name mismatch: expected %s, got %s", r.poolName, input.PoolName)
	}
	if input.ImageName != r.imageName {
		return fmt.Errorf("image name mismatch: expected %s, got %s", r.imageName, input.ImageName)
	}
	if input.ReadOffset%r.blockSize != 0 || input.ReadLength%r.blockSize != 0 {
		return fmt.Errorf("read offset and length must be multiples of block size (%d)", r.blockSize)
	}

	fromSnapshotID := 0
	if input.FromSnap != nil {
		fromSnapshot, err := r.getSnapshotByName(*input.FromSnap)
		if err != nil {
			return err
		}
		fromSnapshotID = fromSnapshot.ID
	}
	targetSnapshot, err := r.getSnapshotByName(input.TargetSnapName)
	if err != nil {
		return err
	}

	volume, volumeMap, err := r.getSnapshotVolume(targetSnapshot.ID)
	if err != nil {
		return err
	}

	fromSnapName := ""
	if input.ReadOffset != 0 {
		fromSnapName = fmt.Sprintf("%s-offset-%d", input.MidSnapPrefix, input.ReadOffset)
	} else if input.FromSnap != nil {
		fromSnapName = *input.FromSnap
	}
	toSnapName := input.TargetSnapName
	if input.ReadLength > 0 && input.ReadOffset+input.ReadLength < targetSnapshot.Size {
		toSnapName = fmt.Sprintf("%s-offset-%d", input.MidSnapPrefix, input.ReadOffset+input.ReadLength)
	}

	records := make([]*diffgenerator.DataRecord, 0)
	tail := input.ReadOffset + input.ReadLength
	if tail > targetSnapshot.Size {
		tail = targetSnapshot.Size
	}
	for i := input.ReadOffset / r.blockSize; i < tail/r.blockSize; i++ {
		if volumeMap[i] < fromSnapshotID || targetSnapshot.ID < volumeMap[i] {
			continue
		}
		offset := i * r.blockSize
		for ; i < tail/r.blockSize; i++ {
			if volumeMap[i] < fromSnapshotID || targetSnapshot.ID < volumeMap[i] {
				break
			}
		}
		length := (i * r.blockSize) - offset
		records = append(
			records,
			diffgenerator.NewUpdatedDataRecord(offset, length, volume[offset:offset+length]),
		)
	}

	diffReader, err := diffgenerator.Run(
		diffgenerator.WithFromSnapName(fromSnapName),
		diffgenerator.WithToSnapName(toSnapName),
		diffgenerator.WithImageSize(targetSnapshot.Size),
		diffgenerator.WithRecords(records),
	)
	if err != nil {
		return fmt.Errorf("failed to create diff reader: %w", err)
	}

	// Write the diff to the output file
	f, err := os.Create(input.OutputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", input.OutputFile, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(fmt.Sprintf("failed to close output file %s: %v", input.OutputFile, err))
		}
	}()
	_, err = io.Copy(f, diffReader)
	if err != nil {
		return fmt.Errorf("failed to write diff to output file %s: %w", input.OutputFile, err)
	}

	return nil
}

func (r *RBDRepository2) ApplyDiffToBlockDevice(blockDevicePath, diffFilePath, fromSnapName, toSnapName string) error {
	return r.super.ApplyDiffToBlockDevice(blockDevicePath, diffFilePath, fromSnapName, toSnapName)
}

func (r *RBDRepository2) ApplyDiffToRawImage(rawImageFilePath, diffFilePath, fromSnapName, toSnapName string) error {
	return r.super.ApplyDiffToRawImage(rawImageFilePath, diffFilePath, fromSnapName, toSnapName)
}

func (r *RBDRepository2) getSnapshotVolume(snapshotID int) ([]byte, []int, error) {
	var snapshot *model.RBDSnapshot
	for _, s := range r.snapshots {
		if s.ID == snapshotID {
			snapshot = s
			break
		}
	}
	if snapshot == nil {
		return nil, nil, fmt.Errorf("snapshot with ID %d not found", snapshotID)
	}
	volume := make([]byte, snapshot.Size)
	volumeMap := make([]int, roundUpDiv(snapshot.Size, r.blockSize))

	for _, history := range r.writtenHistories {
		if history.snapshotID > snapshotID {
			break
		}
		// copy data to the volume
		copy(volume[history.offset:history.offset+uint64(len(history.data))], history.data)
		// update volume map
		for i := uint64(0); i < roundUpDiv(history.volumeSize, r.blockSize); i++ {
			if history.offset < (i+1)*r.blockSize && i*r.blockSize < history.offset+uint64(len(history.data)) {
				volumeMap[i] = history.snapshotID
			}
		}
	}

	return volume, volumeMap, nil
}

func (r *RBDRepository2) getSnapshotByName(snapName string) (*model.RBDSnapshot, error) {
	for _, snapshot := range r.snapshots {
		if snapshot.Name == snapName {
			return snapshot, nil
		}
	}

	return nil, fmt.Errorf("snapshot with name %s not found", snapName)
}

func roundUpDiv(x, y uint64) uint64 {
	if x%y == 0 {
		return x / y
	}
	return x/y + 1
}

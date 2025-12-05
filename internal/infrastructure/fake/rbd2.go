package fake

import (
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/cybozu-go/fin/internal/diffgenerator"
	"github.com/cybozu-go/fin/internal/infrastructure/ceph"
	"github.com/cybozu-go/fin/internal/model"
)

type writtenHistory struct {
	// snapshotID indicates which snapshot writtenHistory is included in.
	snapshotID int
	volumeSize uint64
	offset     uint64
	data       []byte
}

type RBDRepository2 struct {
	r *rand.ChaCha8

	// if this is set, all methods return this error
	err error
	// key: pool/image
	locks map[string][]*model.RBDLock

	// In this fake, divideSize is used as the basis size for calculating
	// which areas on the volume have been rewritten by which snapshots or are unused.
	// The divideSize is a specific idea for fake. It's not related to sector or block size.
	// Although Ceph might have a similar mechanism for having metadata.
	divideSize          uint64
	poolName, imageName string
	snapshots           []*model.RBDSnapshot
	writtenHistories    []*writtenHistory
}

var _ model.RBDRepository = &RBDRepository2{}
var _ model.RBDSnapshotRepository = &RBDRepository2{}
var _ model.RBDImageLocker = &RBDRepository2{}

func NewRBDRepository2(poolName, imageName string) *RBDRepository2 {
	s := make([]byte, 32)
	_, err := cryptorand.Read(s)
	if err != nil {
		panic(fmt.Sprintf("failed to generate random seed for RBDRepository2: %v", err))
	}
	var seed [32]byte
	copy(seed[:], s)

	return &RBDRepository2{
		r:          rand.NewChaCha8(seed),
		locks:      make(map[string][]*model.RBDLock),
		divideSize: 1024,
		poolName:   poolName,
		imageName:  imageName,
		// snapshots having snapshot ID starting from 1
		snapshots:        []*model.RBDSnapshot{},
		writtenHistories: []*writtenHistory{},
	}
}

func (r *RBDRepository2) SetError(err error) {
	r.err = err
}

func (r *RBDRepository2) CreateSnapshot(poolName, imageName, snapName string) error {
	if poolName != r.poolName || imageName != r.imageName {
		return errors.New("invalid pool or image")
	}

	size := r.divideSize * 4
	if len(r.snapshots) > 0 {
		size += r.snapshots[len(r.snapshots)-1].Size + r.divideSize*2
	}
	_, _, err := r.CreateSnapshotWithRandomData(snapName, size)
	return err
}

func (r *RBDRepository2) CreateSnapshotWithRandomData(snapName string, volumeSize uint64) (
	*model.RBDSnapshot, []byte, error) {
	if volumeSize%r.divideSize != 0 {
		return nil, nil, fmt.Errorf("volume size must be a multiple of block size")
	}
	if len(r.snapshots) != 0 && r.snapshots[len(r.snapshots)-1].Size > volumeSize {
		return nil, nil, fmt.Errorf("new snapshot size must be greater than or equal to the last snapshot size")
	}

	// Generate random data for the snapshot
	p1 := rand.Uint64N(volumeSize)
	p2 := rand.Uint64N(volumeSize)
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

func (r *RBDRepository2) RemoveSnapshot(poolName, imageName, snapName string) error {
	if poolName != r.poolName || imageName != r.imageName {
		return errors.New("invalid pool or image")
	}
	i := slices.IndexFunc(r.snapshots, func(snapshot *model.RBDSnapshot) bool {
		return snapshot.Name == snapName
	})
	if i == -1 {
		return model.ErrNotFound
	}
	r.snapshots = slices.Delete(r.snapshots, i, i+1)
	return nil
}

func (r *RBDRepository2) LockAdd(pool, image, lockID string) error {
	if r.err != nil {
		return r.err
	}

	key := pool + "/" + image
	if r.locks[key] == nil {
		r.locks[key] = []*model.RBDLock{}
	}

	if len(r.locks[key]) > 0 {
		return fmt.Errorf("lock already exists: %s", lockID)
	}

	r.locks[key] = append(r.locks[key], &model.RBDLock{
		LockID: lockID,
		Locker: fmt.Sprintf("client:%d", rand.Int64()), // ignore collision for test
		Address: fmt.Sprintf("%d,%d,%d,%d:%d/%d",
			rand.Int32N(256), rand.Int32N(256), rand.Int32N(256), rand.Int32N(256),
			rand.Int32N(65536), rand.Int64()),
	})

	return nil
}

func (r *RBDRepository2) LockRm(pool, image string, lock *model.RBDLock) error {
	if r.err != nil {
		return r.err
	}

	key := pool + "/" + image
	for _, l := range r.locks[key] {
		if l.LockID == lock.LockID && l.Locker == lock.Locker {
			r.locks[key] = slices.DeleteFunc(r.locks[key], func(lockItem *model.RBDLock) bool {
				return lockItem.LockID == lock.LockID && lockItem.Locker == lock.Locker
			})
			return nil
		}
	}

	return fmt.Errorf("lock not found: %s", lock.LockID)
}

func (r *RBDRepository2) LockLs(pool, image string) ([]*model.RBDLock, error) {
	if r.err != nil {
		return nil, r.err
	}

	key := pool + "/" + image
	if locks, ok := r.locks[key]; ok {
		return locks, nil
	}
	return []*model.RBDLock{}, nil
}

func (r *RBDRepository2) ExportDiff(input *model.ExportDiffInput) (io.ReadCloser, error) {
	if input.PoolName != r.poolName {
		return nil, fmt.Errorf("pool name mismatch: expected %s, got %s", r.poolName, input.PoolName)
	}
	if input.ImageName != r.imageName {
		return nil, fmt.Errorf("image name mismatch: expected %s, got %s", r.imageName, input.ImageName)
	}
	// This limitation exists to simplify the algorithm.
	if input.ReadOffset%r.divideSize != 0 || input.ReadLength%r.divideSize != 0 {
		return nil, fmt.Errorf("read offset and length must be multiples of divide size (%d)", r.divideSize)
	}

	fromSnapshotID := 0
	if input.FromSnap != nil {
		fromSnapshot, err := r.getSnapshotByName(*input.FromSnap)
		if err != nil {
			return nil, err
		}
		fromSnapshotID = fromSnapshot.ID
	}
	targetSnapshot, err := r.getSnapshotByName(input.TargetSnapName)
	if err != nil {
		return nil, err
	}

	volume, volumeMap, err := r.getSnapshotVolume(targetSnapshot.ID)
	if err != nil {
		return nil, err
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
	for i := input.ReadOffset / r.divideSize; i < tail/r.divideSize; i++ {
		if volumeMap[i] < fromSnapshotID || targetSnapshot.ID < volumeMap[i] {
			continue
		}
		offset := i * r.divideSize
		for ; i < tail/r.divideSize; i++ {
			if volumeMap[i] < fromSnapshotID || targetSnapshot.ID < volumeMap[i] {
				break
			}
		}
		length := (i * r.divideSize) - offset
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
		return nil, fmt.Errorf("failed to create diff reader: %w", err)
	}

	return io.NopCloser(diffReader), nil
}

func (r *RBDRepository2) ApplyDiffToBlockDevice(
	blockDevicePath,
	diffFilePath,
	fromSnapName,
	toSnapName string,
	diffChecksumChunkSize uint64,
) error {
	return ceph.NewRBDRepository().ApplyDiffToBlockDevice(
		blockDevicePath,
		diffFilePath,
		fromSnapName,
		toSnapName,
		diffChecksumChunkSize,
	)
}

func (r *RBDRepository2) ApplyDiffToRawImage(
	rawImageFilePath,
	diffFilePath,
	fromSnapName,
	toSnapName string,
	expansionUnitSize,
	rawChecksumChunkSize,
	diffChecksumChunkSize uint64,
	enableChecksumVerify bool,
) error {
	return ceph.NewRBDRepository().ApplyDiffToRawImage(
		rawImageFilePath,
		diffFilePath,
		fromSnapName,
		toSnapName,
		expansionUnitSize,
		rawChecksumChunkSize,
		diffChecksumChunkSize,
		enableChecksumVerify,
	)
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
	volumeMap := make([]int, roundUpDiv(snapshot.Size, r.divideSize))

	for _, history := range r.writtenHistories {
		if history.snapshotID > snapshotID {
			break
		}
		// copy data to the volume
		copy(volume[history.offset:history.offset+uint64(len(history.data))], history.data)
		// update volume map
		for i := uint64(0); i < roundUpDiv(history.volumeSize, r.divideSize); i++ {
			if history.offset < (i+1)*r.divideSize && i*r.divideSize < history.offset+uint64(len(history.data)) {
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

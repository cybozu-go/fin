package fake

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/cybozu-go/fin/test/utils"
	"github.com/stretchr/testify/require"
)

const (
	poolName              = "testpool"
	imageName             = "testimage"
	midSnapName           = "testmid-snap"
	rawChecksumChunkSize  = 64 * 1024       // 64 KiB
	diffChecksumChunkSize = 2 * 1024 * 1024 // 2 MiB
)

func TestRBDRepository2_getSnapshotVolume(t *testing.T) {

	rbdRepo := NewRBDRepository2(poolName, imageName)

	// append test data
	rbdRepo.divideSize = 4
	rbdRepo.snapshots = []*model.RBDSnapshot{
		{ID: 1, Name: "snap1", Size: 12},
		{ID: 2, Name: "snap2", Size: 16},
		{ID: 3, Name: "snap3", Size: 16},
	}
	rbdRepo.writtenHistories = []*writtenHistory{
		{
			snapshotID: 1,
			volumeSize: 12,
			offset:     3,
			data:       []byte("1111"),
		},
		{
			snapshotID: 2,
			volumeSize: 16,
			offset:     5,
			data:       []byte("bbb"),
		},
		{
			snapshotID: 2,
			volumeSize: 16,
			offset:     13,
			data:       []byte("BBB"),
		},
		{
			snapshotID: 3,
			volumeSize: 16,
			offset:     8,
			data:       []byte("3"),
		},
	}

	testCases := []struct {
		snapshotID        int
		expectedVolume    []byte
		expectedVolumeMap []int
	}{
		{
			snapshotID:        1,
			expectedVolume:    []byte{0, 0, 0, '1', '1', '1', '1', 0, 0, 0, 0, 0},
			expectedVolumeMap: []int{1, 1, 0},
		},
		{
			snapshotID:        2,
			expectedVolume:    []byte{0, 0, 0, '1', '1', 'b', 'b', 'b', 0, 0, 0, 0, 0, 'B', 'B', 'B'},
			expectedVolumeMap: []int{1, 2, 0, 2},
		},
		{
			snapshotID:        3,
			expectedVolume:    []byte{0, 0, 0, '1', '1', 'b', 'b', 'b', '3', 0, 0, 0, 0, 'B', 'B', 'B'},
			expectedVolumeMap: []int{1, 2, 3, 2},
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("snapshotID=%d", tc.snapshotID), func(t *testing.T) {
			volume, volumeMap, err := rbdRepo.getSnapshotVolume(tc.snapshotID)
			require.NoError(t, err, "failed to get snapshot volume")
			require.Equal(t, tc.expectedVolume, volume, "snapshot volume mismatch")
			require.Equal(t, tc.expectedVolumeMap, volumeMap, "snapshot volume map mismatch")
		})
	}
}

type testVolume struct {
	snapName string
	size     uint64
	data     []byte
}

func TestRBDRepository2_exportDiff_random(t *testing.T) {
	rbdRepo := NewRBDRepository2(poolName, imageName)

	// prepare 3 random snapshots
	volumes := []*testVolume{
		{snapName: "snap1", size: 5 * 1024},
		{snapName: "snap2", size: 5 * 1024},
		{snapName: "snap3", size: 8 * 1024},
	}

	for _, v := range volumes {
		ss, data, err := rbdRepo.CreateSnapshotWithRandomData(v.snapName, v.size)
		require.NoError(t, err, "failed to create snapshot with random data")
		require.NotEqual(t, 0, ss.ID, "snapshot ID should not be zero")
		require.Equal(t, v.snapName, ss.Name, "snapshot name mismatch")
		require.Equal(t, v.size, ss.Size, "snapshot size mismatch")
		require.Equal(t, v.size, uint64(len(data)))
		v.data = data
	}

	// check ListSnapshots
	snapshots, err := rbdRepo.ListSnapshots(poolName, imageName)
	require.NoError(t, err, "failed to list snapshots")
	require.Len(t, snapshots, 3, "should have 3 snapshots")
	for i, snap := range snapshots {
		require.Equal(t, i+1, snap.ID, "snapshot ID mismatch")
		require.Equal(t, volumes[i].snapName, snap.Name, "snapshot name mismatch")
		require.Equal(t, volumes[i].size, snap.Size, "snapshot size mismatch")
	}

	testRBDRepository2_exportDiff(t, rbdRepo, volumes)
}

func TestRBDRepository2_exportDiff_specify(t *testing.T) {
	rbdRepo := NewRBDRepository2(poolName, imageName)

	volumes := []*testVolume{
		{
			snapName: "snap1",
			size:     5 * 1024,
			// 0x00 * 1000 + 0x11 * 2072 + 0x00 * 2048
			data: bytes.Join([][]byte{
				bytes.Repeat([]byte{0x00}, 1000),
				bytes.Repeat([]byte{0x11}, 2072),
				bytes.Repeat([]byte{0x00}, 2048),
			}, nil),
		},
		// snap2 have overlapping data with snap1
		{
			snapName: "snap2",
			size:     5 * 1024,
			// 0x00 * 1000 + 0x11 * 24 + 0x22 * 3072 + 0x00 * 1024
			data: bytes.Join([][]byte{
				bytes.Repeat([]byte{0x00}, 1000),
				bytes.Repeat([]byte{0x11}, 24),
				bytes.Repeat([]byte{0x22}, 3072),
				bytes.Repeat([]byte{0x00}, 1024),
			}, nil),
		},
		// snap3 expand the volume and doesn't have overlapping data with snap1 and snap2
		{
			snapName: "snap3",
			size:     8 * 1024,
			// 0x00 * 1000 + 0x11 * 24 + 0x22 * 3072 + 0x00 * 2048 + 0x33 * 2048
			data: bytes.Join([][]byte{
				bytes.Repeat([]byte{0x00}, 1000),
				bytes.Repeat([]byte{0x11}, 24),
				bytes.Repeat([]byte{0x22}, 3072),
				bytes.Repeat([]byte{0x00}, 2048),
				bytes.Repeat([]byte{0x33}, 2048),
			}, nil),
		},
	}
	for i, v := range volumes {
		rbdRepo.snapshots = append(rbdRepo.snapshots, &model.RBDSnapshot{
			ID:        i + 1,
			Name:      v.snapName,
			Size:      v.size,
			Timestamp: model.NewRBDTimeStamp(time.Now()),
		})
	}
	rbdRepo.writtenHistories = []*writtenHistory{
		{
			snapshotID: 1,
			volumeSize: 5 * 1024,
			offset:     1000,
			// 0x11 * 2072
			data: bytes.Repeat([]byte{0x11}, 2072),
		},
		{
			snapshotID: 2,
			volumeSize: 5 * 1024,
			offset:     1024,
			// 0x22 * 3072
			data: bytes.Repeat([]byte{0x22}, 3072),
		},
		{
			snapshotID: 3,
			volumeSize: 8 * 1024,
			offset:     1024 * 6,
			// 0x33 * 2048
			data: bytes.Repeat([]byte{0x33}, 2048),
		},
	}

	testRBDRepository2_exportDiff(t, rbdRepo, volumes)
}

func testRBDRepository2_exportDiff(t *testing.T, rbdRepo *RBDRepository2, volumes []*testVolume) {
	t.Helper()

	workDir := t.TempDir()

	// prepare export diffs
	exportDiffs := []*model.ExportDiffInput{
		{
			ReadOffset:     0,
			ReadLength:     5 * 1024,
			FromSnap:       nil,
			TargetSnapName: volumes[0].snapName,
		},
		{
			ReadOffset:     0,
			ReadLength:     3 * 1024,
			FromSnap:       nil,
			TargetSnapName: volumes[1].snapName,
		},
		{
			ReadOffset:     3 * 1024,
			ReadLength:     3 * 1024,
			FromSnap:       nil,
			TargetSnapName: volumes[1].snapName,
		},
		{
			ReadOffset:     0,
			ReadLength:     3 * 1024,
			FromSnap:       &volumes[1].snapName,
			TargetSnapName: volumes[2].snapName,
		},
		{
			ReadOffset:     3 * 1024,
			ReadLength:     3 * 1024,
			FromSnap:       &volumes[1].snapName,
			TargetSnapName: volumes[2].snapName,
		},
		{
			ReadOffset:     6 * 1024,
			ReadLength:     3 * 1024,
			FromSnap:       &volumes[1].snapName,
			TargetSnapName: volumes[2].snapName,
		},
		{
			ReadOffset:     0,
			ReadLength:     8 * 1024,
			FromSnap:       &volumes[0].snapName,
			TargetSnapName: volumes[2].snapName,
		},
	}

	outputFiles := []string{
		filepath.Join(workDir, "snap1-all.bin"),
		filepath.Join(workDir, "snap2-offset-0.bin"),
		filepath.Join(workDir, "snap2-offset-3072.bin"),
		filepath.Join(workDir, "snap3-from2-offset-0.bin"),
		filepath.Join(workDir, "snap3-from2-offset-3072.bin"),
		filepath.Join(workDir, "snap3-from2-offset-6144.bin"),
		filepath.Join(workDir, "snap3-from1-all.bin"),
	}

	for i, e := range exportDiffs {
		e.PoolName = poolName
		e.ImageName = imageName
		e.MidSnapPrefix = midSnapName
		stream, err := rbdRepo.ExportDiff(e)
		require.NoError(t, err, "failed to export diff for snapshot %s", e.TargetSnapName)

		err = backup.WriteDiffPartAndCloseStream(
			stream,
			outputFiles[i],
			nlv.ChecksumFilePath(outputFiles[i]),
			diffChecksumChunkSize,
		)
		require.NoError(t, err, "failed to write diff part for snapshot %s", e.TargetSnapName)
	}

	type diffInfo struct {
		file         string
		fromSnapName string
		toSnapName   string
	}
	testCases := []struct {
		name               string
		diffsToApply       []diffInfo
		expectedVolumeHead []byte
	}{
		{
			name: "full backup by one diff file",
			diffsToApply: []diffInfo{
				{
					file:         "snap1-all.bin",
					fromSnapName: "",
					toSnapName:   volumes[0].snapName,
				},
			},
			expectedVolumeHead: volumes[0].data,
		},
		{
			name: "full backup by multiple diff files",
			diffsToApply: []diffInfo{
				{
					file:         "snap2-offset-0.bin",
					fromSnapName: "",
					toSnapName:   fmt.Sprintf("%s-offset-%d", midSnapName, 3*1024),
				},
				{
					file:         "snap2-offset-3072.bin",
					fromSnapName: fmt.Sprintf("%s-offset-%d", midSnapName, 3*1024),
					toSnapName:   volumes[1].snapName,
				},
			},
			expectedVolumeHead: volumes[1].data,
		},
		{
			name: "incremental backup with expanded volume",
			diffsToApply: []diffInfo{
				{
					file:         "snap2-offset-0.bin",
					fromSnapName: "",
					toSnapName:   fmt.Sprintf("%s-offset-%d", midSnapName, 3*1024),
				},
				{
					file:         "snap2-offset-3072.bin",
					fromSnapName: fmt.Sprintf("%s-offset-%d", midSnapName, 3*1024),
					toSnapName:   volumes[1].snapName,
				},
				{
					file:         "snap3-from2-offset-0.bin",
					fromSnapName: volumes[1].snapName,
					toSnapName:   fmt.Sprintf("%s-offset-%d", midSnapName, 3*1024),
				},
				{
					file:         "snap3-from2-offset-3072.bin",
					fromSnapName: fmt.Sprintf("%s-offset-%d", midSnapName, 3*1024),
					toSnapName:   fmt.Sprintf("%s-offset-%d", midSnapName, 6*1024),
				},
				{
					file:         "snap3-from2-offset-6144.bin",
					fromSnapName: fmt.Sprintf("%s-offset-%d", midSnapName, 6*1024),
					toSnapName:   volumes[2].snapName,
				},
			},
			expectedVolumeHead: volumes[2].data,
		},
		{
			name: "incremental backup skipping some history",
			diffsToApply: []diffInfo{
				{
					file:         "snap1-all.bin",
					fromSnapName: "",
					toSnapName:   volumes[0].snapName,
				},
				{
					file:         "snap3-from1-all.bin",
					fromSnapName: volumes[0].snapName,
					toSnapName:   volumes[2].snapName,
				},
			},
			expectedVolumeHead: volumes[2].data,
		},
	}

	for ti, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// create a new block device for applying diffs
			rawImagePath := filepath.Join(workDir, fmt.Sprintf("raw-image-%d.bin", ti))
			defer func() {
				require.NoError(t, os.Remove(rawImagePath), "failed to remove raw image file")
			}()

			// apply diffs to a new block device
			for _, diff := range tc.diffsToApply {
				diffFilePath := filepath.Join(workDir, diff.file)
				err := rbdRepo.ApplyDiffToRawImage(
					rawImagePath,
					diffFilePath,
					diff.fromSnapName,
					diff.toSnapName,
					utils.RawImgExpansionUnitSize,
					rawChecksumChunkSize,
					diffChecksumChunkSize,
					false,
				)
				require.NoError(t, err, "failed to apply diff %s", diff.file)
			}

			// check the resulting volume
			resultData, err := os.ReadFile(rawImagePath)
			require.NoError(t, err, "failed to read raw image file")
			require.Equal(t, tc.expectedVolumeHead, resultData[:len(tc.expectedVolumeHead)], "resulting volume data mismatch")
			require.Equal(t, make([]byte, len(resultData)-len(tc.expectedVolumeHead)), resultData[len(tc.expectedVolumeHead):],
				"extra data in resulting volume")
		})
	}
}

package fake

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cybozu-go/fin/internal/model"
	"github.com/stretchr/testify/require"
)

func TestRBDRepository2_getSnapshotVolume(t *testing.T) {
	const (
		poolName  = "testpool"
		imageName = "testimage"
		snapName  = "test-snapshot"
	)

	rbdRepo := NewRBDRepository2(poolName, imageName)

	// append test data
	rbdRepo.blockSize = 4
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
			data:       []byte("3456"),
		},
		{
			snapshotID: 2,
			volumeSize: 16,
			offset:     5,
			data:       []byte("abc"),
		},
		{
			snapshotID: 2,
			volumeSize: 16,
			offset:     13,
			data:       []byte("xyz"),
		},
		{
			snapshotID: 3,
			volumeSize: 16,
			offset:     8,
			data:       []byte("A"),
		},
	}

	testCases := []struct {
		snapshotID        int
		expectedVolume    []byte
		expectedVolumeMap []int
	}{
		{
			snapshotID:        1,
			expectedVolume:    []byte{0, 0, 0, '3', '4', '5', '6', 0, 0, 0, 0, 0},
			expectedVolumeMap: []int{1, 1, 0},
		},
		{
			snapshotID:        2,
			expectedVolume:    []byte{0, 0, 0, '3', '4', 'a', 'b', 'c', 0, 0, 0, 0, 0, 'x', 'y', 'z'},
			expectedVolumeMap: []int{1, 2, 0, 2},
		},
		{
			snapshotID:        3,
			expectedVolume:    []byte{0, 0, 0, '3', '4', 'a', 'b', 'c', 'A', 0, 0, 0, 0, 'x', 'y', 'z'},
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

func TestRBDRepository2(t *testing.T) {
	const (
		poolName    = "testpool"
		imageName   = "testimage"
		midSnapName = "testmid-snap"
	)

	workDir, err := os.MkdirTemp("", "rbd2_test")
	require.NoError(t, err, "failed to create temporary directory for RBDRepository2 test")
	defer func() {
		require.NoError(t, os.RemoveAll(workDir))
	}()

	rbdRepo := NewRBDRepository2(poolName, imageName)

	// prepare 3 random snapshots
	volumes := []*struct {
		snapName string
		size     uint64
		data     []byte
	}{
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

	// prepare export diffs
	exportDiffs := []*model.ExportDiffInput{
		{
			ReadOffset:     0,
			ReadLength:     5 * 1024,
			FromSnap:       nil,
			TargetSnapName: volumes[0].snapName,
			OutputFile:     filepath.Join(workDir, "snap1-all.bin"),
		},
		{
			ReadOffset:     0,
			ReadLength:     3 * 1024,
			FromSnap:       nil,
			TargetSnapName: volumes[1].snapName,
			OutputFile:     filepath.Join(workDir, "snap2-offset-0.bin"),
		},
		{
			ReadOffset:     3 * 1024,
			ReadLength:     3 * 1024,
			FromSnap:       nil,
			TargetSnapName: volumes[1].snapName,
			OutputFile:     filepath.Join(workDir, "snap2-offset-3072.bin"),
		},
		{
			ReadOffset:     0,
			ReadLength:     3 * 1024,
			FromSnap:       &volumes[1].snapName,
			TargetSnapName: volumes[2].snapName,
			OutputFile:     filepath.Join(workDir, "snap3-by2-offset-0.bin"),
		},
		{
			ReadOffset:     3 * 1024,
			ReadLength:     3 * 1024,
			FromSnap:       &volumes[1].snapName,
			TargetSnapName: volumes[2].snapName,
			OutputFile:     filepath.Join(workDir, "snap3-by2-offset-3072.bin"),
		},
		{
			ReadOffset:     6 * 1024,
			ReadLength:     3 * 1024,
			FromSnap:       &volumes[1].snapName,
			TargetSnapName: volumes[2].snapName,
			OutputFile:     filepath.Join(workDir, "snap3-2-offset-6144.bin"),
		},
		{
			ReadOffset:     0,
			ReadLength:     8 * 1024,
			FromSnap:       &volumes[0].snapName,
			TargetSnapName: volumes[2].snapName,
			OutputFile:     filepath.Join(workDir, "snap3-1-all.bin"),
		},
	}

	for _, e := range exportDiffs {
		e.PoolName = poolName
		e.ImageName = imageName
		e.MidSnapPrefix = midSnapName
		err = rbdRepo.ExportDiff(e)
		require.NoError(t, err, "failed to export diff for snapshot %s", e.TargetSnapName)
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
					file:         "snap3-by2-offset-0.bin",
					fromSnapName: volumes[1].snapName,
					toSnapName:   fmt.Sprintf("%s-offset-%d", midSnapName, 3*1024),
				},
				{
					file:         "snap3-by2-offset-3072.bin",
					fromSnapName: fmt.Sprintf("%s-offset-%d", midSnapName, 3*1024),
					toSnapName:   fmt.Sprintf("%s-offset-%d", midSnapName, 6*1024),
				},
				{
					file:         "snap3-2-offset-6144.bin",
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
					file:         "snap3-1-all.bin",
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
			blockDevicePath := filepath.Join(workDir, fmt.Sprintf("block-device-%d.bin", ti))
			defer func() {
				require.NoError(t, os.Remove(blockDevicePath), "failed to remove block device file")
			}()

			// apply diffs to a new block device
			for _, diff := range tc.diffsToApply {
				diffFilePath := filepath.Join(workDir, diff.file)
				err = rbdRepo.ApplyDiffToRawImage(blockDevicePath, diffFilePath, diff.fromSnapName, diff.toSnapName)
				require.NoError(t, err, "failed to apply diff %s", diff.file)
			}

			// check the resulting volume
			resultData, err := os.ReadFile(blockDevicePath)
			require.NoError(t, err, "failed to read block device file")
			require.Equal(t, tc.expectedVolumeHead, resultData[:len(tc.expectedVolumeHead)], "resulting volume data mismatch")
			require.Equal(t, make([]byte, len(resultData)-len(tc.expectedVolumeHead)), resultData[len(tc.expectedVolumeHead):],
				"extra data in resulting volume")
		})
	}
}

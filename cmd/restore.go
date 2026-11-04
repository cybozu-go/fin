package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cybozu-go/fin/internal/infrastructure/ceph"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	rvol "github.com/cybozu-go/fin/internal/infrastructure/restore"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/restore"
	"github.com/spf13/cobra"
)

var restoreCmd = &cobra.Command{
	Use: "restore",
	RunE: func(cmd *cobra.Command, args []string) error {
		return restoreJobMain()
	},
}

func init() {
	rootCmd.AddCommand(restoreCmd)
}

func restoreJobMain() error {
	pvcName := os.Getenv("BACKUP_TARGET_PVC_NAME")
	if pvcName == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_NAME environment variable is not set")
	}
	pvcNamespace := os.Getenv("BACKUP_TARGET_PVC_NAMESPACE")
	if pvcNamespace == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_NAMESPACE environment variable is not set")
	}
	rootPath := filepath.Join(nlv.VolumePath, pvcNamespace, pvcName)
	pvcUID := os.Getenv("BACKUP_TARGET_PVC_UID")
	if pvcUID == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_UID environment variable is not set")
	}
	vol, err := openVolume(rootPath)
	if err != nil {
		return err
	}
	defer vol.Close()
	restoreVol := rvol.NewRestoreVolume(rvol.VolumePath)

	rbdRepo := ceph.NewRBDRepository()

	actionUID := os.Getenv("ACTION_UID")
	if actionUID == "" {
		return fmt.Errorf("ACTION_UID environment variable is not set")
	}

	rawImageChunkSizeStr := os.Getenv("RAW_IMAGE_CHUNK_SIZE")
	if rawImageChunkSizeStr == "" {
		return fmt.Errorf("RAW_IMAGE_CHUNK_SIZE environment variable is not set")
	}
	rawImageChunkSize, err := strconv.ParseUint(rawImageChunkSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid RAW_IMAGE_CHUNK_SIZE: %w", err)
	}

	targetSnapshotIDStr := os.Getenv("TARGET_SNAPSHOT_ID")
	if targetSnapshotIDStr == "" {
		return fmt.Errorf("TARGET_SNAPSHOT_ID environment variable is not set")
	}
	targetSnapshotID, err := strconv.Atoi(targetSnapshotIDStr)
	if err != nil {
		return fmt.Errorf("invalid TARGET_SNAPSHOT_ID: %w", err)
	}

	enableChecksumVerify, err := getEnableChecksumVerify()
	if err != nil {
		return err
	}

	r := restore.NewRestore(&input.Restore{
		Repo:                 vol.finRepo,
		RBDRepo:              rbdRepo,
		NodeLocalVolumeRepo:  vol.nlvRepo,
		ActionUID:            actionUID,
		TargetSnapshotID:     targetSnapshotID,
		TargetPVCUID:         pvcUID,
		RawImageChunkSize:    rawImageChunkSize,
		EnableChecksumVerify: enableChecksumVerify,
		RestoreVol:           restoreVol,
	})

	return performUntilLocked(r.Perform, "restore job completed successfully", "failed to perform restore")
}

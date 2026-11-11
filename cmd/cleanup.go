package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job/cleanup"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/spf13/cobra"
)

var cleanupCmd = &cobra.Command{
	Use: "cleanup",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cleanupJobMain()
	},
}

func init() {
	rootCmd.AddCommand(cleanupCmd)
}

func cleanupJobMain() error {
	pvcName := os.Getenv("BACKUP_TARGET_PVC_NAME")
	if pvcName == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_NAME environment variable is not set")
	}
	pvcNamespace := os.Getenv("BACKUP_TARGET_PVC_NAMESPACE")
	if pvcNamespace == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_NAMESPACE environment variable is not set")
	}
	pvcUID := os.Getenv("BACKUP_TARGET_PVC_UID")
	if pvcUID == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_UID environment variable is not set")
	}

	rootPath := filepath.Join(nlv.VolumePath, pvcNamespace, pvcName)

	actionUID := os.Getenv("ACTION_UID")
	if actionUID == "" {
		return fmt.Errorf("ACTION_UID environment variable is not set")
	}
	targetSnapshotIDStr := os.Getenv("TARGET_SNAPSHOT_ID")
	if targetSnapshotIDStr == "" {
		return fmt.Errorf("TARGET_SNAPSHOT_ID environment variable is not set")
	}
	targetSnapshotID, err := strconv.Atoi(targetSnapshotIDStr)
	if err != nil {
		return fmt.Errorf("invalid TARGET_SNAPSHOT_ID: %w", err)
	}

	return runWithVolume(rootPath, func(vol *volume) error {
		d := cleanup.NewCleanup(&input.Cleanup{
			Repo:                vol.finRepo,
			NodeLocalVolumeRepo: vol.nlvRepo,
			ActionUID:           actionUID,
			TargetSnapshotID:    targetSnapshotID,
			TargetPVCUID:        pvcUID,
		})
		return d.Perform()
	}, "Cleanup job completed successfully", "failed to perform cleanup")
}

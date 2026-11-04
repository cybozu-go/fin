package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cybozu-go/fin/internal/infrastructure/ceph"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/verification"
	"github.com/spf13/cobra"
)

var verificationCmd = &cobra.Command{
	Use: "verification",
	RunE: func(cmd *cobra.Command, args []string) error {
		return verificationJobMain()
	},
}

func init() {
	rootCmd.AddCommand(verificationCmd)
}

func verificationJobMain() error {
	pvcName := os.Getenv("BACKUP_TARGET_PVC_NAME")
	if pvcName == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_NAME environment variable is not set")
	}
	pvcNamespace := os.Getenv("BACKUP_TARGET_PVC_NAMESPACE")
	if pvcNamespace == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_NAMESPACE environment variable is not set")
	}
	rootPath := filepath.Join(nlv.VolumePath, pvcNamespace, pvcName)
	vol, err := openVolume(rootPath)
	if err != nil {
		return err
	}
	defer vol.Close()

	rbdRepo := ceph.NewRBDRepository()

	actionUID := os.Getenv("ACTION_UID")
	if actionUID == "" {
		return fmt.Errorf("ACTION_UID environment variable is not set")
	}

	backupSnapshotIDStr := os.Getenv("BACKUP_SNAPSHOT_ID")
	if backupSnapshotIDStr == "" {
		return fmt.Errorf("BACKUP_SNAPSHOT_ID environment variable is not set")
	}
	backupSnapshotID, err := strconv.Atoi(backupSnapshotIDStr)
	if err != nil {
		return fmt.Errorf("invalid BACKUP_SNAPSHOT_ID: %w", err)
	}

	pvcUID := os.Getenv("BACKUP_TARGET_PVC_UID")
	if pvcUID == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_UID environment variable is not set")
	}

	expansionUnitSize, err := getExpansionUnitSize()
	if err != nil {
		return err
	}

	enableChecksumVerify, err := getEnableChecksumVerify()
	if err != nil {
		return err
	}

	v := verification.NewVerification(&input.Verification{
		Repo:                 vol.finRepo,
		RBDRepo:              rbdRepo,
		NLVRepo:              vol.nlvRepo,
		ActionUID:            actionUID,
		TargetSnapshotID:     backupSnapshotID,
		TargetPVCUID:         pvcUID,
		ExpansionUnitSize:    expansionUnitSize,
		EnableChecksumVerify: enableChecksumVerify,
	})
	return performUntilLocked(v.Perform, "verification job completed successfully", "failed to perform verification")
}

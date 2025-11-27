package cmd

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cybozu-go/fin/internal/infrastructure/ceph"
	"github.com/cybozu-go/fin/internal/infrastructure/db"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job"
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
	nlvRepo, err := nlv.NewNodeLocalVolumeRepository(rootPath)
	if err != nil {
		return fmt.Errorf("failed to create NodeLocalVolumeRepository: %w", err)
	}
	defer func() { _ = nlvRepo.Close() }()

	finRepo, err := db.New(nlvRepo.GetDBPath())
	if err != nil {
		return fmt.Errorf("failed to create repository: %w", err)
	}
	defer func() { _ = finRepo.Close() }()

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

	v := verification.NewVerification(&input.Verification{
		Repo:                 finRepo,
		RBDRepo:              rbdRepo,
		NLVRepo:              nlvRepo,
		ActionUID:            actionUID,
		TargetSnapshotID:     backupSnapshotID,
		TargetPVCUID:         pvcUID,
		ExpansionUnitSize:    expansionUnitSize,
		EnableChecksumVerify: defaultEnableChecksumVerify, // this could be configurable in the future
	})
	for {
		err = v.Perform()
		if err == nil {
			slog.Info("verification job completed successfully")
			return nil
		}
		if !errors.Is(err, job.ErrCantLock) {
			return fmt.Errorf("failed to perform verification: %w", err)
		}
		slog.Warn("failed to acquire lock, will retry.")
		time.Sleep(job.RetryInterval)
	}
}

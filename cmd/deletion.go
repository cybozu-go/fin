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
	"github.com/cybozu-go/fin/internal/job/deletion"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/spf13/cobra"
)

var deletionCmd = &cobra.Command{
	Use: "deletion",
	RunE: func(cmd *cobra.Command, args []string) error {
		return deletionJobMain()
	},
}

func init() {
	rootCmd.AddCommand(deletionCmd)
}

func deletionJobMain() error {
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
	targetSnapshotIDStr := os.Getenv("TARGET_SNAPSHOT_ID")
	if targetSnapshotIDStr == "" {
		return fmt.Errorf("TARGET_SNAPSHOT_ID environment variable is not set")
	}
	targetSnapshotID, err := strconv.Atoi(targetSnapshotIDStr)
	if err != nil {
		return fmt.Errorf("invalid TARGET_SNAPSHOT_ID: %w", err)
	}

	expansionUnitSize, err := getExpansionUnitSize()
	if err != nil {
		return err
	}

	d := deletion.NewDeletion(&input.Deletion{
		Repo:                finRepo,
		RBDRepo:             rbdRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           actionUID,
		TargetSnapshotID:    targetSnapshotID,
		TargetPVCUID:        pvcUID,
		ExpansionUnitSize:   expansionUnitSize,
	})
	for {
		err = d.Perform()
		if err == nil {
			slog.Info("Deletion job completed successfully")
			return nil
		}
		if !errors.Is(err, model.ErrBusy) {
			return fmt.Errorf("failed to perform deletion: %w", err)
		}
		slog.Warn("failed to acquire lock, will retry.")
		time.Sleep(job.RetryInterval)
	}
}

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
	rvol "github.com/cybozu-go/fin/internal/infrastructure/restore"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/job/restore"
	"github.com/cybozu-go/fin/internal/model"
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
	nlvRepo, err := nlv.NewNodeLocalVolumeRepository(rootPath)
	if err != nil {
		return fmt.Errorf("failed to create NodeLocalVolumeRepository: %w", err)
	}
	defer func() { _ = nlvRepo.Close() }()
	pvcUID := os.Getenv("BACKUP_TARGET_PVC_UID")
	if pvcUID == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_UID environment variable is not set")
	}

	finRepo, err := db.New(nlvRepo.GetDBPath())
	if err != nil {
		return fmt.Errorf("failed to create repository: %w", err)
	}
	defer func() { _ = finRepo.Close() }()
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

	r := restore.NewRestore(&input.Restore{
		Repo:                finRepo,
		RBDRepo:             rbdRepo,
		NodeLocalVolumeRepo: nlvRepo,
		ActionUID:           actionUID,
		TargetSnapshotID:    targetSnapshotID,
		TargetPVCUID:        pvcUID,
		RawImageChunkSize:   rawImageChunkSize,
		RestoreVol:          restoreVol,
	})

	for {
		err = r.Perform()
		if err == nil {
			slog.Info("restore job completed successfully")
			return nil
		}
		if !errors.Is(err, model.ErrBusy) {
			return fmt.Errorf("failed to perform restore: %w", err)
		}
		slog.Warn("failed to acquire lock, will retry.")
		time.Sleep(job.RetryInterval)
	}
}

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
	"github.com/cybozu-go/fin/internal/infrastructure/kubernetes"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/spf13/cobra"
)

var backupCmd = &cobra.Command{
	Use: "backup",
	RunE: func(cmd *cobra.Command, args []string) error {
		return backupJobMain()
	},
}

func init() {
	rootCmd.AddCommand(backupCmd)
}

func backupJobMain() error {
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

	clientSet, err := getClientSet()
	if err != nil {
		return fmt.Errorf("failed to get Kubernetes clientset: %w", err)
	}
	k8sRepo := kubernetes.NewKubernetesRepository(clientSet)
	rbdRepo := ceph.NewRBDRepository()

	actionUID := os.Getenv("ACTION_UID")
	if actionUID == "" {
		return fmt.Errorf("ACTION_UID environment variable is not set")
	}

	rbdPool := os.Getenv("RBD_POOL")
	if rbdPool == "" {
		return fmt.Errorf("RBD_POOL environment variable is not set")
	}

	rbdImageName := os.Getenv("RBD_IMAGE_NAME")
	if rbdImageName == "" {
		return fmt.Errorf("RBD_IMAGE_NAME environment variable is not set")
	}

	backupSnapshotIDStr := os.Getenv("BACKUP_SNAPSHOT_ID")
	if backupSnapshotIDStr == "" {
		return fmt.Errorf("BACKUP_SNAPSHOT_ID environment variable is not set")
	}
	backupSnapshotID, err := strconv.Atoi(backupSnapshotIDStr)
	if err != nil {
		return fmt.Errorf("invalid BACKUP_SNAPSHOT_ID: %w", err)
	}

	sourceCandidateSnapshotIDStr := os.Getenv("SOURCE_CANDIDATE_SNAPSHOT_ID")
	var sourceCandidateSnapshotID *int
	if sourceCandidateSnapshotIDStr != "" {
		id, err := strconv.Atoi(sourceCandidateSnapshotIDStr)
		if err != nil {
			return fmt.Errorf("invalid SOURCE_CANDIDATE_SNAPSHOT_ID: %w", err)
		}
		sourceCandidateSnapshotID = &id
	}

	pvcUID := os.Getenv("BACKUP_TARGET_PVC_UID")
	if pvcUID == "" {
		return fmt.Errorf("BACKUP_TARGET_PVC_UID environment variable is not set")
	}

	maxPartSizeStr := os.Getenv("MAX_PART_SIZE")
	if maxPartSizeStr == "" {
		return fmt.Errorf("MAX_PART_SIZE environment variable is not set")
	}
	maxPartSize, err := strconv.ParseUint(maxPartSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid MAX_PART_SIZE: %w", err)
	}

	b := backup.NewBackup(&input.Backup{
		Repo:                      finRepo,
		KubernetesRepo:            k8sRepo,
		RBDRepo:                   rbdRepo,
		NodeLocalVolumeRepo:       nlvRepo,
		ActionUID:                 actionUID,
		TargetRBDPoolName:         rbdPool,
		TargetRBDImageName:        rbdImageName,
		TargetSnapshotID:          backupSnapshotID,
		SourceCandidateSnapshotID: sourceCandidateSnapshotID,
		TargetPVCName:             pvcName,
		TargetPVCNamespace:        pvcNamespace,
		TargetPVCUID:              pvcUID,
		MaxPartSize:               maxPartSize,
	})
	for {
		err = b.Perform()
		if err == nil {
			slog.Info("Backup job completed successfully")
			return nil
		}
		if !errors.Is(err, model.ErrBusy) {
			return fmt.Errorf("failed to perform backup: %w", err)
		}
		slog.Warn("failed to acquire lock, will retry.")
		time.Sleep(job.RetryInterval)
	}
}

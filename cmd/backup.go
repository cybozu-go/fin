package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cybozu-go/fin/internal/infrastructure/ceph"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/job/backup"
	"github.com/cybozu-go/fin/internal/job/input"
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
	vol, err := openVolume(rootPath)
	if err != nil {
		return err
	}
	defer vol.Close()

	clientSet, err := getClientSet()
	if err != nil {
		return fmt.Errorf("failed to get Kubernetes clientset: %w", err)
	}
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

	expansionUnitSize, err := getExpansionUnitSize()
	if err != nil {
		return err
	}

	rawChecksumChunkSize, err := getRawChecksumChunkSize()
	if err != nil {
		return err
	}

	diffChecksumChunkSize, err := getDiffChecksumChunkSize()
	if err != nil {
		return err
	}

	enableChecksumVerify, err := getEnableChecksumVerify()
	if err != nil {
		return err
	}

	b := backup.NewBackup(&input.Backup{
		Repo:                      vol.finRepo,
		K8sClient:                 clientSet,
		RBDRepo:                   rbdRepo,
		NodeLocalVolumeRepo:       vol.nlvRepo,
		ActionUID:                 actionUID,
		TargetRBDPoolName:         rbdPool,
		TargetRBDImageName:        rbdImageName,
		TargetSnapshotID:          backupSnapshotID,
		SourceCandidateSnapshotID: sourceCandidateSnapshotID,
		TargetPVCName:             pvcName,
		TargetPVCNamespace:        pvcNamespace,
		TargetPVCUID:              pvcUID,
		MaxPartSize:               maxPartSize,
		ExpansionUnitSize:         expansionUnitSize,
		RawChecksumChunkSize:      rawChecksumChunkSize,
		DiffChecksumChunkSize:     diffChecksumChunkSize,
		EnableChecksumVerify:      enableChecksumVerify,
	})
	return performUntilLocked(b.Perform, "Backup job completed successfully", "failed to perform backup")
}

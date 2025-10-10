package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/cybozu-go/fin/internal/job/createfinbackup"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/spf13/cobra"
	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var createFinBackupCmd = &cobra.Command{
	Use: "create-fin-backup",
	RunE: func(cmd *cobra.Command, args []string) error {
		return createFinBackupJobMain(cmd)
	},
}

var (
	fbcName      string
	fbcNamespace string
)

func init() {
	createFinBackupCmd.Flags().StringVar(&fbcName, "fin-backup-config-name", "", "FinBackupConfig name")
	createFinBackupCmd.Flags().StringVar(&fbcNamespace, "fin-backup-config-namespace", "", "FinBackupConfig namespace")
	rootCmd.AddCommand(createFinBackupCmd)
}

func createFinBackupJobMain(cmd *cobra.Command) error {
	if fbcName == "" {
		return fmt.Errorf("--fin-backup-config-name is required")
	}
	if fbcNamespace == "" {
		return fmt.Errorf("--fin-backup-config-namespace is required")
	}

	jobName := os.Getenv("JOB_NAME")
	if jobName == "" {
		return fmt.Errorf("JOB_NAME environment variable is not set")
	}
	podNamespace := os.Getenv("POD_NAMESPACE")
	if podNamespace == "" {
		return fmt.Errorf("POD_NAMESPACE environment variable is not set")
	}

	k8sClient, err := getControllerClient()
	if err != nil {
		return fmt.Errorf("failed to create controller client: %w", err)
	}

	// Lookup the Job resource to find the cronjob-scheduled-timestamp annotation
	ctx := cmd.Context()
	job := &batchv1.Job{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: podNamespace, Name: jobName}, job); err != nil {
		return fmt.Errorf("failed to get Job %s/%s: %w", podNamespace, jobName, err)
	}

	tsStr, ok := job.Annotations["batch.kubernetes.io/cronjob-scheduled-timestamp"]
	if !ok {
		return fmt.Errorf(
			"job %s/%s missing annotation batch.kubernetes.io/cronjob-scheduled-timestamp",
			podNamespace,
			jobName,
		)
	}
	jobCreatedAt, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return fmt.Errorf("invalid batch.kubernetes.io/cronjob-scheduled-timestamp: %w", err)
	}

	in := &input.CreateFinBackup{
		FinBackupConfigName:         fbcName,
		FinBackupConfigNamespace:    fbcNamespace,
		CurrentJobName:              jobName,
		CurrentJobCreationTimestamp: jobCreatedAt,
		CtrlClient:                  k8sClient,
	}
	c := createfinbackup.NewCreateFinBackup(in)
	if err := c.Perform(); err != nil {
		slog.Error("create-finbackup failed", "err", err)
		return err
	}
	slog.Info("create-finbackup-job completed successfully")
	return nil
}

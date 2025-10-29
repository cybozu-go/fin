package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/cybozu-go/fin/internal/job/createfinbackup"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/spf13/cobra"
)

var createFinBackupCmd = &cobra.Command{
	Use: "create-finbackup-job",
	RunE: func(cmd *cobra.Command, args []string) error {
		return createFinBackupJobMain()
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

func createFinBackupJobMain() error {
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

	in := &input.CreateFinBackup{
		FinBackupConfigName:      fbcName,
		FinBackupConfigNamespace: fbcNamespace,
		JobName:                  jobName,
		JobNamespace:             podNamespace,
		CtrlClient:               k8sClient,
	}
	c := createfinbackup.NewCreateFinBackup(in)
	if err := c.Perform(); err != nil {
		slog.Error("create-finbackup failed", "err", err)
		return err
	}
	slog.Info("create-finbackup-job completed successfully")
	return nil
}

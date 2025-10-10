package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/cybozu-go/fin/internal/job/createfinbackup"
	"github.com/cybozu-go/fin/internal/job/input"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	finv1 "github.com/cybozu-go/fin/api/v1"
)

var createFinBackupCmd = &cobra.Command{
	Use: "createfinbackup",
	RunE: func(cmd *cobra.Command, args []string) error {
		return createFinBackupJobMain()
	},
}

func init() {
	rootCmd.AddCommand(createFinBackupCmd)
}

func createFinBackupJobMain() error {
	fbcName := os.Getenv("FINBACKUPCONFIG_NAME")
	if fbcName == "" {
		return fmt.Errorf("FINBACKUPCONFIG_NAME environment variable is not set")
	}
	fbcNamespace := os.Getenv("FINBACKUPCONFIG_NAMESPACE")
	if fbcNamespace == "" {
		return fmt.Errorf("FINBACKUPCONFIG_NAMESPACE environment variable is not set")
	}
	jobName := os.Getenv("CURRENT_JOB_NAME")
	if jobName == "" {
		return fmt.Errorf("CURRENT_JOB_NAME environment variable is not set")
	}
	jobCreatedAtStr := os.Getenv("CURRENT_JOB_CREATION_TIMESTAMP")
	if jobCreatedAtStr == "" {
		return fmt.Errorf("CURRENT_JOB_CREATION_TIMESTAMP environment variable is not set")
	}
	jobCreatedAt, err := time.Parse(time.RFC3339, jobCreatedAtStr)
	if err != nil {
		return fmt.Errorf("invalid CURRENT_JOB_CREATION_TIMESTAMP: %w", err)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = finv1.AddToScheme(scheme)

	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create controller-runtime client: %w", err)
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
	slog.Info("create-finbackup job completed successfully")
	return nil
}

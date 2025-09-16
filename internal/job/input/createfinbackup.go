package input

import (
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type CreateFinBackup struct {
	FinBackupConfigName         string
	FinBackupConfigNamespace    string
	CurrentJobName              string
	CurrentJobCreationTimestamp time.Time
	CtrlClient                  client.Client
}

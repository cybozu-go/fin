package input

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type CreateFinBackup struct {
	FinBackupConfigName      string
	FinBackupConfigNamespace string
	JobName                  string
	JobNamespace             string
	CtrlClient               client.Client
}

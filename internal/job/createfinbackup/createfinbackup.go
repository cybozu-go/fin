package createfinbackup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/controller"
	"github.com/cybozu-go/fin/internal/job/input"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	annotationFinBackupConfigName = "fin.cybozu.io/fbc-name"
	annotationFinBackupConfigNS   = "fin.cybozu.io/fbc-namespace"

	prefixLength = 229
	suffixLength = 8
)

type CreateFinBackup struct {
	client       client.Client
	fbcName      string
	fbcNamespace string
	jobName      string
	jobNamespace string
}

func NewCreateFinBackup(in *input.CreateFinBackup) *CreateFinBackup {
	return &CreateFinBackup{
		client:       in.CtrlClient,
		fbcName:      in.FinBackupConfigName,
		fbcNamespace: in.FinBackupConfigNamespace,
		jobName:      in.JobName,
		jobNamespace: in.JobNamespace,
	}
}

// Perform creates FinBackup according to the FinBackupConfig.
func (c *CreateFinBackup) Perform() error {
	ctx := context.Background()

	var fbc finv1.FinBackupConfig
	if err := c.client.Get(ctx, types.NamespacedName{Namespace: c.fbcNamespace, Name: c.fbcName}, &fbc); err != nil {
		return fmt.Errorf("failed to get FinBackupConfig %s/%s: %w", c.fbcNamespace, c.fbcName, err)
	}

	// Lookup the Job resource to find the cronjob-scheduled-timestamp annotation
	jobCreatedAt, err := c.getJobCreationTimestamp(ctx)
	if err != nil {
		return fmt.Errorf("failed to get job creation timestamp: %w", err)
	}

	fb := newFinBackupFromConfig(&fbc, c.jobName, jobCreatedAt)
	if err := c.client.Create(ctx, fb); err != nil {
		if k8serrors.IsAlreadyExists(err) {
			slog.Info("FinBackup already exists, do nothing", "name", fb.Name, "namespace", fb.Namespace)
			return nil
		}
		return fmt.Errorf("failed to create FinBackup from FinBackupConfig %s/%s: %w", c.fbcNamespace, c.fbcName, err)
	}
	return nil
}

// getJobCreationTimestamp retrieves the Job creation timestamp from the
// batch.kubernetes.io/cronjob-scheduled-timestamp annotation
func (c *CreateFinBackup) getJobCreationTimestamp(ctx context.Context) (time.Time, error) {
	job := &batchv1.Job{}
	if err := c.client.Get(ctx, types.NamespacedName{Namespace: c.jobNamespace, Name: c.jobName}, job); err != nil {
		return time.Time{}, fmt.Errorf("failed to get Job %s/%s: %w", c.jobNamespace, c.jobName, err)
	}

	tsStr, ok := job.Annotations["batch.kubernetes.io/cronjob-scheduled-timestamp"]
	if !ok {
		return time.Time{}, fmt.Errorf(
			"job %s/%s missing annotation batch.kubernetes.io/cronjob-scheduled-timestamp",
			c.jobNamespace,
			c.jobName,
		)
	}

	jobCreatedAt, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid batch.kubernetes.io/cronjob-scheduled-timestamp: %w", err)
	}

	return jobCreatedAt, nil
}

// constructFinBackupName builds the FinBackup name according to the spec:
// <first 229 chars of fbc name>-<JobCreationTimestamp YYYYMMDDhhmmss>-<last 8 chars of job name>
func constructFinBackupName(fbcName, jobName string, jobCreatedAt time.Time) string {
	if len(fbcName) > prefixLength {
		fbcName = fbcName[:prefixLength]
	}
	ts := jobCreatedAt.Format("20060102150405")
	if len(jobName) > suffixLength {
		jobName = jobName[len(jobName)-suffixLength:]
	}
	return strings.Join([]string{fbcName, ts, jobName}, "-")
}

func newFinBackupFromConfig(fbc *finv1.FinBackupConfig, jobName string, jobCreatedAt time.Time) *finv1.FinBackup {
	fbName := constructFinBackupName(fbc.GetName(), jobName, jobCreatedAt)
	return &finv1.FinBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fbName,
			Namespace: fbc.Namespace,
			Labels: map[string]string{
				controller.LabelFinBackupConfigUID: string(fbc.UID),
			},
			Annotations: map[string]string{
				annotationFinBackupConfigName: fbc.Name,
				annotationFinBackupConfigNS:   fbc.Namespace,
			},
		},
		Spec: finv1.FinBackupSpec{
			PVC:          fbc.Spec.PVC,
			PVCNamespace: fbc.Spec.PVCNamespace,
			Node:         fbc.Spec.Node,
		},
	}
}

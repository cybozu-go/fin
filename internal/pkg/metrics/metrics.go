package metrics

import (
	"sync"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	metricNamespace = "fin"

	backupKindFull        = "full"
	backupKindIncremental = "incremental"

	// Labels
	pvcLabel        = "pvc"
	pvcNSLabel      = "pvc_namespace"
	nsLabel         = "namespace"
	fbcLabel        = "finbackupconfig"
	cephNSLabel     = "ceph_namespace"
	nodeLabel       = "node"
	backupKindLabel = "backup_create_kind"
)

var (
	backupDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "backup_duration_seconds",
			Help:      "Duration of backup operations in seconds",
			Buckets:   []float64{600, 1800, 3600, 10800, 21600, 43200, 86400, 172800}, // 10m, 30m, 1h, 3h, 6h, 12h, 24h, 48h
		},
		[]string{pvcLabel, pvcNSLabel, cephNSLabel},
	)

	backupCreateStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "backup_create_status",
			Help:      "Current backup execution state per PVC and backup kind",
		},
		[]string{cephNSLabel, pvcNSLabel, pvcLabel, backupKindLabel},
	)

	registerOnce sync.Once
)

func SetBackupDurationSeconds(fb *finv1.FinBackup, untilCondition, cephNamespace string, fullBackup bool) {
	if fb == nil {
		return
	}
	start := meta.FindStatusCondition(fb.Status.Conditions, finv1.BackupConditionBackupInProgress)
	end := meta.FindStatusCondition(fb.Status.Conditions, untilCondition)
	if start == nil || end == nil {
		return
	}
	duration := end.LastTransitionTime.Sub(start.LastTransitionTime.Time)
	backupDurationSeconds.WithLabelValues(fb.Spec.PVC, fb.Spec.PVCNamespace, cephNamespace).Observe(float64(duration.Seconds()))
}

func SetBackupCreateStatus(fb *finv1.FinBackup, cephNamespace string, inProgress, fullBackup bool) {
	if fb == nil {
		return
	}
	var fullValue, incrementalValue float64
	if inProgress {
		if fullBackup {
			fullValue = 1.0
		} else {
			incrementalValue = 1.0
		}
	}
	backupCreateStatus.WithLabelValues(cephNamespace, fb.Spec.PVCNamespace, fb.Spec.PVC, backupKindFull).Set(fullValue)
	backupCreateStatus.WithLabelValues(cephNamespace, fb.Spec.PVCNamespace, fb.Spec.PVC, backupKindIncremental).Set(incrementalValue)
}

func Register() {
	registerOnce.Do(func() {
		metrics.Registry.MustRegister(
			backupCreateStatus,
			backupDurationSeconds,
		)
	})
}

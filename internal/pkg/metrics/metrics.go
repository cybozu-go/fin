package metrics

import (
	"sync"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	statusLabel     = "status"
	restoreLabel    = "finrestore"
	conditionLabel  = "condition"
)

var (
	finbackupconfigInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "finbackupconfig_info",
			Help:      "Information about FinBackupConfig",
		},
		[]string{cephNSLabel, pvcLabel, pvcNSLabel, nsLabel, fbcLabel, nodeLabel},
	)

	backupDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "backup_duration_seconds",
			Help:      "Duration of backup operations in seconds",
			Buckets:   []float64{600, 1800, 3600, 10800, 21600, 43200, 86400, 172800}, // 10m, 30m, 1h, 3h, 6h, 12h, 24h, 48h
		},
		[]string{cephNSLabel, pvcLabel, pvcNSLabel},
	)

	backupCreateStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "backup_create_status",
			Help:      "Current backup execution state per PVC and backup kind",
		},
		[]string{cephNSLabel, pvcNSLabel, pvcLabel, backupKindLabel},
	)

	restoreInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "restore_info",
			Help:      "Information about FinRestore and associated PVC",
		},
		[]string{cephNSLabel, nsLabel, restoreLabel, pvcNSLabel, pvcLabel},
	)

	restoreStatusCondition = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "restore_status_condition",
			Help:      "Current restore status condition per FinRestore",
		},
		[]string{nsLabel, restoreLabel, conditionLabel, statusLabel},
	)

	restoreCreationTimeStamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "restore_creation_timestamp",
			Help:      "Creation timestamp of the FinRestore resource",
		},
		[]string{nsLabel, restoreLabel},
	)

	registerOnce sync.Once
)

func SetFinBackupConfigInfo(fbc *finv1.FinBackupConfig, cephNamespace string) {
	if fbc == nil {
		return
	}
	finbackupconfigInfo.WithLabelValues(cephNamespace, fbc.Spec.PVC, fbc.Spec.PVCNamespace, fbc.Namespace, fbc.Name, fbc.Spec.Node).Set(1)
}

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
	backupDurationSeconds.WithLabelValues(cephNamespace, fb.Spec.PVC, fb.Spec.PVCNamespace).Observe(float64(duration.Seconds()))
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

func SetRestoreInfo(fr *finv1.FinRestore, cephNamespace, pvcNamespace, pvcName string) {
	if fr == nil {
		return
	}
	restoreInfo.WithLabelValues(cephNamespace, fr.Namespace, fr.Name, pvcNamespace, pvcName).Set(1)
}

func SetRestoreStatusCondition(fr *finv1.FinRestore) {
	if fr == nil {
		return
	}

	statuses := []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse, metav1.ConditionUnknown}
	for _, cond := range fr.Status.Conditions {
		for _, status := range statuses {
			value := 0.0
			if cond.Status == status {
				value = 1.0
			}
			restoreStatusCondition.WithLabelValues(fr.Namespace, fr.Name, cond.Type, string(status)).Set(value)
		}
	}
}

func SetRestoreCreationTimestamp(fr *finv1.FinRestore) {
	if fr == nil {
		return
	}
	if fr.CreationTimestamp.IsZero() {
		return
	}
	restoreCreationTimeStamp.WithLabelValues(fr.Namespace, fr.Name).Set(float64(fr.CreationTimestamp.Unix()))
}

func DeleteRestoreMetrics(fr *finv1.FinRestore, cephNamespace, pvcNamespace, pvcName string) {
	if fr == nil {
		return
	}

	restoreInfo.DeleteLabelValues(cephNamespace, fr.Namespace, fr.Name, pvcNamespace, pvcName)

	statuses := []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse, metav1.ConditionUnknown}
	for _, cond := range fr.Status.Conditions {
		for _, status := range statuses {
			restoreStatusCondition.DeleteLabelValues(fr.Namespace, fr.Name, cond.Type, string(status))
		}
	}
	restoreCreationTimeStamp.DeleteLabelValues(fr.Namespace, fr.Name)
}

func Register() {
	registerOnce.Do(func() {
		metrics.Registry.MustRegister(
			finbackupconfigInfo,
			backupCreateStatus,
			backupDurationSeconds,
			restoreInfo,
			restoreStatusCondition,
			restoreCreationTimeStamp,
		)
	})
}

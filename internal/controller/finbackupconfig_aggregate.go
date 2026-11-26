package controller

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/finbackupbuilder"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *FinBackupConfigReconciler) updateStatus(ctx context.Context, fbc *finv1.FinBackupConfig) error {
	var fbs finv1.FinBackupList
	if err := r.List(
		ctx,
		&fbs,
		client.InNamespace(fbc.Namespace),
		client.MatchingLabels{finbackupbuilder.LabelFinBackupConfigUID: string(fbc.UID)},
	); err != nil {
		return fmt.Errorf("failed to list FinBackups: %w", err)
	}

	var activeFinBackups []*finv1.FinBackup
	for i := range fbs.Items {
		fb := &fbs.Items[i]
		if fb.DeletionTimestamp.IsZero() {
			activeFinBackups = append(activeFinBackups, fb)
		}
	}
	newStatus := calculateFinBackupConfigStatus(fbc, activeFinBackups)
	if equality.Semantic.DeepEqual(fbc.Status, newStatus) {
		return nil
	}
	fbc.Status = newStatus
	if err := r.Status().Update(ctx, fbc); err != nil {
		return fmt.Errorf("failed to update the status: %w", err)
	}
	return nil
}

// updateStatusOfGonePVC updates the status as updateStatus does when the PVC of fbc is
// gone, checking the Ceph cluster with the PVC manifest stored in a FinBackup of fbc.
// The FinBackups can still be deleted with those manifests, so the status would
// otherwise keep reporting deleted ones. Without any manifest it leaves the status
// as it is, since another instance may manage fbc.
func (r *FinBackupConfigReconciler) updateStatusOfGonePVC(ctx context.Context, fbc *finv1.FinBackupConfig) error {
	var fbs finv1.FinBackupList
	if err := r.List(
		ctx,
		&fbs,
		client.InNamespace(fbc.Namespace),
		client.MatchingLabels{finbackupbuilder.LabelFinBackupConfigUID: string(fbc.UID)},
	); err != nil {
		return fmt.Errorf("failed to list FinBackups: %w", err)
	}

	for i := range fbs.Items {
		fb := &fbs.Items[i]
		if fb.Status.PVCManifest == "" {
			continue
		}
		var pvc corev1.PersistentVolumeClaim
		if err := json.Unmarshal([]byte(fb.Status.PVCManifest), &pvc); err != nil {
			return fmt.Errorf("failed to unmarshal the PVC manifest of FinBackup %s: %w", fb.Name, err)
		}
		ok, err := checkCephCluster(ctx, r.Client, &pvc, r.managedCephClusterID)
		if err != nil {
			return fmt.Errorf("failed to check Ceph cluster: %w", err)
		}
		if !ok {
			return nil
		}
		return r.updateStatus(ctx, fbc)
	}
	return nil
}

// calculateFinBackupConfigStatus returns a copy of fbc.Status with
// the backup summaries recalculated from activeFinBackups.
func calculateFinBackupConfigStatus(fbc *finv1.FinBackupConfig, activeFinBackups []*finv1.FinBackup) finv1.FinBackupConfigStatus {
	status := *fbc.Status.DeepCopy()
	status.LatestBackup = finBackupSummary(latestFinBackup(activeFinBackups))
	return status
}

// latestFinBackup returns the FinBackup in fbs with the greatest creation
// timestamp, or nil when fbs is empty.
func latestFinBackup(fbs []*finv1.FinBackup) *finv1.FinBackup {
	if len(fbs) == 0 {
		return nil
	}

	// Timestamps are serialized with second precision, so the UID breaks ties to keep
	// the choice independent of the list order.
	return slices.MaxFunc(fbs, func(a, b *finv1.FinBackup) int {
		return cmp.Or(
			a.CreationTimestamp.Compare(b.CreationTimestamp.Time),
			cmp.Compare(a.UID, b.UID),
		)
	})
}

// finBackupSummary returns the summary of fb. A pointer to a zero metav1.Time
// would be serialized as null rather than omitted.
func finBackupSummary(fb *finv1.FinBackup) *finv1.FinBackupSummary {
	if fb == nil {
		return nil
	}
	summary := &finv1.FinBackupSummary{Ref: finv1.FinBackupReference{Name: fb.Name, UID: fb.UID}}
	if !fb.Status.CreatedAt.IsZero() {
		summary.SnapshotTime = fb.Status.CreatedAt.DeepCopy()
	}
	return summary
}

// enqueueFinBackupConfigOfFinBackup returns a request for the FinBackupConfig named by
// the annotation of fb, or nil when fb does not have it. FinBackups have no owner
// reference and their label holds only the UID, so the name comes from the annotation.
// The namespace is that of fb, where its FinBackupConfig creates it.
func enqueueFinBackupConfigOfFinBackup(_ context.Context, fb client.Object) []ctrl.Request {
	name := fb.GetAnnotations()[finbackupbuilder.AnnotationFinBackupConfigName]
	if name == "" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: fb.GetNamespace(), Name: name}}}
}

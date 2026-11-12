package controller

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/finbackupbuilder"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *FinBackupConfigReconciler) updateBackupSummary(ctx context.Context, fbc *finv1.FinBackupConfig) error {
	var fbs finv1.FinBackupList
	if err := r.List(ctx, &fbs, client.InNamespace(fbc.Namespace),
		client.MatchingLabels{finbackupbuilder.LabelFinBackupConfigUID: string(fbc.UID)}); err != nil {
		return fmt.Errorf("failed to list FinBackups: %w", err)
	}

	newStatus := fbc.Status.DeepCopy()
	newStatus.LatestBackupRef, newStatus.LatestBackupSnapshotTime = refAndSnapshotTime(
		latestFinBackup(relatedFinBackups(fbc, fbs.Items)))
	if equality.Semantic.DeepEqual(&fbc.Status, newStatus) {
		return nil
	}
	fbc.Status = *newStatus
	if err := r.Status().Update(ctx, fbc); err != nil {
		return fmt.Errorf("failed to update the backup summary: %w", err)
	}
	return nil
}

func relatedFinBackups(fbc *finv1.FinBackupConfig, fbs []finv1.FinBackup) []*finv1.FinBackup {
	var related []*finv1.FinBackup
	for i := range fbs {
		fb := &fbs[i]
		if fb.Labels[finbackupbuilder.LabelFinBackupConfigUID] != string(fbc.UID) ||
			!fb.DeletionTimestamp.IsZero() {
			continue
		}
		related = append(related, fb)
	}
	return related
}

// The name is not parsed for the scheduled time, since its format is up to the creator.
func latestFinBackup(fbs []*finv1.FinBackup) *finv1.FinBackup {
	if len(fbs) == 0 {
		return nil
	}
	latest := slices.MaxFunc(fbs, func(a, b *finv1.FinBackup) int {
		return cmp.Or(
			a.CreationTimestamp.Compare(b.CreationTimestamp.Time),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.UID, b.UID),
		)
	})
	return latest
}

func refAndSnapshotTime(fb *finv1.FinBackup) (*finv1.FinBackupReference, *metav1.Time) {
	if fb == nil {
		return nil, nil
	}
	ref := &finv1.FinBackupReference{Name: fb.Name, UID: fb.UID}
	if fb.Status.CreatedAt.IsZero() {
		return ref, nil
	}
	return ref, fb.Status.CreatedAt.DeepCopy()
}

package controller

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/finbackupbuilder"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// updateStatus lists the FinBackups of fbc and writes the status calculated from
// them. It writes nothing when the status is unchanged.
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

	newStatus := calculateFinBackupConfigStatus(fbc, fbs.Items)
	if equality.Semantic.DeepEqual(fbc.Status, newStatus) {
		return nil
	}
	fbc.Status = newStatus
	if err := r.Status().Update(ctx, fbc); err != nil {
		return fmt.Errorf("failed to update the status: %w", err)
	}
	return nil
}

// calculateFinBackupConfigStatus returns the status of fbc with the fields
// aggregated from fbs replaced. The other fields, such as node, are kept.
func calculateFinBackupConfigStatus(fbc *finv1.FinBackupConfig, fbs []finv1.FinBackup) finv1.FinBackupConfigStatus {
	related := relatedFinBackups(fbc, fbs)

	status := *fbc.Status.DeepCopy()
	status.LatestBackup = finBackupSummary(latestFinBackup(related))
	status.LatestVerifiedBackup = finBackupSummary(latestVerifiedFinBackup(related))
	return status
}

// relatedFinBackups returns the FinBackups in fbs that were created for fbc and are not
// being deleted.
// The annotations are checked as well as the label, since FinBackup events are mapped
// by the annotations.
func relatedFinBackups(fbc *finv1.FinBackupConfig, fbs []finv1.FinBackup) []*finv1.FinBackup {
	var related []*finv1.FinBackup
	for i := range fbs {
		fb := &fbs[i]
		if fb.Labels[finbackupbuilder.LabelFinBackupConfigUID] != string(fbc.UID) ||
			fb.Annotations[finbackupbuilder.AnnotationFinBackupConfigName] != fbc.Name ||
			fb.Annotations[finbackupbuilder.AnnotationFinBackupConfigNamespace] != fbc.Namespace ||
			!fb.DeletionTimestamp.IsZero() {
			continue
		}
		related = append(related, fb)
	}
	return related
}

// latestFinBackup returns the FinBackup in fbs created last, or nil when fbs is empty.
// The name is not parsed for the scheduled time, since its format is up to the creator.
func latestFinBackup(fbs []*finv1.FinBackup) *finv1.FinBackup {
	if len(fbs) == 0 {
		return nil
	}
	// creationTimestamp has only second precision. The UID breaks a tie only so that the
	// choice does not depend on the order of fbs. The name is not used, since its format
	// may change.
	return slices.MaxFunc(fbs, func(a, b *finv1.FinBackup) int {
		return cmp.Or(
			a.CreationTimestamp.Compare(b.CreationTimestamp.Time),
			cmp.Compare(a.UID, b.UID),
		)
	})
}

// latestVerifiedFinBackup returns the FinBackup in fbs with the latest snapshot among
// those whose Verified condition is true, or nil when none is. The snapshot time
// decides rather than the creation time, so that the reported snapshot time is the
// newest among the verified FinBackups.
func latestVerifiedFinBackup(fbs []*finv1.FinBackup) *finv1.FinBackup {
	verified := slices.DeleteFunc(slices.Clone(fbs), func(fb *finv1.FinBackup) bool {
		return !fb.IsVerifiedTrue()
	})
	if len(verified) == 0 {
		return nil
	}
	return slices.MaxFunc(verified, func(a, b *finv1.FinBackup) int {
		return cmp.Or(
			a.Status.CreatedAt.Compare(b.Status.CreatedAt.Time),
			cmp.Compare(a.UID, b.UID),
		)
	})
}

// finBackupSummary returns the summary of fb, or nil when fb is nil. The snapshot
// time is left unset until the snapshot is taken.
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
// the annotations of fb, or nil when fb does not have them. Only the annotations are
// read, so that a FinBackup event costs no API request.
func enqueueFinBackupConfigOfFinBackup(_ context.Context, fb client.Object) []ctrl.Request {
	name := fb.GetAnnotations()[finbackupbuilder.AnnotationFinBackupConfigName]
	namespace := fb.GetAnnotations()[finbackupbuilder.AnnotationFinBackupConfigNamespace]
	if name == "" || namespace == "" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}}
}

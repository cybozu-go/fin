package controller

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/finbackupbuilder"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *FinBackupConfigReconciler) updateBackupSummary(ctx context.Context, fbc *finv1.FinBackupConfig) error {
	var fbs finv1.FinBackupList
	if err := r.List(ctx, &fbs, client.InNamespace(fbc.Namespace),
		client.MatchingLabels{finbackupbuilder.LabelFinBackupConfigUID: string(fbc.UID)}); err != nil {
		return fmt.Errorf("failed to list FinBackups: %w", err)
	}

	related := relatedFinBackups(fbc, fbs.Items)
	newStatus := fbc.Status.DeepCopy()
	latest := latestFinBackup(related)
	newStatus.LatestBackupRef, newStatus.LatestBackupSnapshotTime = refAndSnapshotTime(latest)
	newStatus.PendingBackups = countPendingFinBackups(related)
	newStatus.LastSuccessfulBackupRef, newStatus.LastSuccessfulBackupSnapshotTime = refAndSnapshotTime(
		lastSuccessfulFinBackup(related))
	// SetStatusCondition keeps lastTransitionTime unless the status changes.
	meta.SetStatusCondition(&newStatus.Conditions, latestBackupSucceededCondition(latest))
	if equality.Semantic.DeepEqual(&fbc.Status, newStatus) {
		return nil
	}
	fbc.Status = *newStatus
	if err := r.Status().Update(ctx, fbc); err != nil {
		return fmt.Errorf("failed to update the backup summary: %w", err)
	}
	return nil
}

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

// The name is not parsed for the scheduled time, since its format is up to the creator.
func latestFinBackup(fbs []*finv1.FinBackup) *finv1.FinBackup {
	if len(fbs) == 0 {
		return nil
	}
	return slices.MaxFunc(fbs, compareCreation)
}

// The snapshot time decides rather than the creation, so that an older FinBackup
// succeeding late does not hide a newer snapshot.
func lastSuccessfulFinBackup(fbs []*finv1.FinBackup) *finv1.FinBackup {
	var succeeded []*finv1.FinBackup
	for _, fb := range fbs {
		if finBackupSucceeded(fb) {
			succeeded = append(succeeded, fb)
		}
	}
	if len(succeeded) == 0 {
		return nil
	}
	return slices.MaxFunc(succeeded, func(a, b *finv1.FinBackup) int {
		return cmp.Or(a.Status.CreatedAt.Compare(b.Status.CreatedAt.Time), compareCreation(a, b))
	})
}

func compareCreation(a, b *finv1.FinBackup) int {
	return cmp.Or(
		a.CreationTimestamp.Compare(b.CreationTimestamp.Time),
		cmp.Compare(a.Name, b.Name),
		cmp.Compare(a.UID, b.UID),
	)
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

// A long wait or a retried error is not a failure: only the FinBackup status decides.
func countPendingFinBackups(fbs []*finv1.FinBackup) int32 {
	var n int32
	for _, fb := range fbs {
		if !finBackupSucceeded(fb) && !finBackupFailedOrUnverified(fb) {
			n++
		}
	}
	return n
}

// Without the snapshot recorded, which snapshot the FinBackup holds cannot be told.
func finBackupSucceeded(fb *finv1.FinBackup) bool {
	return fb.IsStoredToNode() && fb.IsVerifiedTrue() &&
		!fb.IsChecksumMismatched() && !fb.IsMetadataCorrupted() &&
		snapshotRecorded(fb)
}

func snapshotRecorded(fb *finv1.FinBackup) bool {
	return fb.Status.SnapID != nil && !fb.Status.CreatedAt.IsZero()
}

// A corruption comes first, since it overrides a past verification. ObservedGeneration
// is unset, since a FinBackup does not record the generation it was created from.
func latestBackupSucceededCondition(latest *finv1.FinBackup) metav1.Condition {
	condition := metav1.Condition{
		Type:   finv1.FinBackupConfigConditionLatestBackupSucceeded,
		Status: metav1.ConditionUnknown,
	}
	// Reasons that mirror a FinBackup condition reuse its type.
	switch {
	case latest == nil:
		condition.Reason, condition.Message = "NoBackups", "No FinBackup exists"
	case latest.IsMetadataCorrupted():
		condition.Status = metav1.ConditionFalse
		condition.Reason, condition.Message = finv1.BackupConditionMetadataCorrupted, "The metadata of the latest FinBackup is corrupted"
	case latest.IsChecksumMismatched():
		condition.Status = metav1.ConditionFalse
		condition.Reason, condition.Message = finv1.BackupConditionChecksumMismatched, "The data of the latest FinBackup has a checksum mismatch"
	case latest.IsVerifiedFalse():
		condition.Status = metav1.ConditionFalse
		condition.Reason, condition.Message = "VerificationFailed", "The latest FinBackup failed its verification"
	case latest.IsStoredToNode() && latest.IsVerifiedTrue() && !snapshotRecorded(latest):
		condition.Reason, condition.Message = "SnapshotMetadataUnavailable",
			"The latest FinBackup is verified, but its snapshot is not recorded"
	case latest.IsStoredToNode() && latest.IsVerifiedTrue():
		condition.Status = metav1.ConditionTrue
		condition.Reason, condition.Message = "BackupSucceeded", "The latest FinBackup is stored and verified"
	case latest.IsVerificationSkipped():
		condition.Status = metav1.ConditionFalse
		condition.Reason, condition.Message = finv1.BackupConditionVerificationSkipped, "The latest FinBackup skipped its verification"
	case !snapshotRecorded(latest):
		condition.Reason, condition.Message = "SnapshotPending", "The latest FinBackup has not taken its snapshot"
	case !latest.IsStoredToNode():
		condition.Reason, condition.Message = "BackupPending", "The latest FinBackup has not finished storing data"
	default:
		condition.Reason, condition.Message = "VerificationPending", "The latest FinBackup is waiting for its verification"
	}
	return condition
}

func finBackupFailedOrUnverified(fb *finv1.FinBackup) bool {
	return fb.IsVerifiedFalse() || fb.IsChecksumMismatched() || fb.IsMetadataCorrupted() ||
		fb.IsVerificationSkipped()
}

// Only the annotations are read, so that a FinBackup event costs no API request.
func enqueueFinBackupConfigOfFinBackup(_ context.Context, fb client.Object) []ctrl.Request {
	name := fb.GetAnnotations()[finbackupbuilder.AnnotationFinBackupConfigName]
	namespace := fb.GetAnnotations()[finbackupbuilder.AnnotationFinBackupConfigNamespace]
	if name == "" || namespace == "" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}}
}

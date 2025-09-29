package v1

import (
	"context"
	"fmt"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

//nolint:lll
// +kubebuilder:webhook:path=/validate-fin-cybozu-io-v1-finbackupdeletion,mutating=false,failurePolicy=fail,sideEffects=None,groups=fin.cybozu.io,resources=finbackups,verbs=delete,versions=v1,name=finbackupdeletion.fin.cybozu.io,admissionReviewVersions=v1
// +kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackups,verbs=list;get;watch

type FinBackupCustomValidator struct {
	client    client.Client
	apiReader client.Reader
}

var _ webhook.CustomValidator = &FinBackupCustomValidator{}

// SetupFinBackupDeletionWebhookWithManager registers the webhook for FinBackupDeletion in the manager.
func SetupFinBackupDeletionWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&finv1.FinBackup{}).
		WithValidator(&FinBackupCustomValidator{client: mgr.GetClient(), apiReader: mgr.GetAPIReader()}).
		Complete()
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type FinBackup.
func (v *FinBackupCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	logger := log.FromContext(ctx)

	target, ok := obj.(*finv1.FinBackup)
	if !ok {
		return nil, fmt.Errorf("expected a FinBackup object but got %T", obj)
	}

	// Retrieve the list of FinBackups with the same target.Spec.Node and targetPVCName.
	// Deny deletion if any FinBackup has a snapID smaller than target.SnapID.
	var list finv1.FinBackupList

	// Use apiReader to retrieve the most up-to-date information
	if err := v.apiReader.List(ctx, &list, client.InNamespace(target.Namespace)); err != nil {
		logger.Error(err, "failed to list FinBackups")
		return nil, err
	}

	targetPVCName := target.Spec.PVC
	targetPVCNamespace := target.Spec.PVCNamespace
	targetResourceName := fmt.Sprintf("FinBackup(name=%s, namespace=%s)", target.Name, target.Namespace)

	if targetPVCName == "" {
		return nil, fmt.Errorf("deletion denied: missing spec.pvc on %s", targetResourceName)
	}
	if targetPVCNamespace == "" {
		return nil, fmt.Errorf("deletion denied: missing spec.pvcNamespace on %s", targetResourceName)
	}

	if target.Status.SnapID == nil {
		return nil, fmt.Errorf("deletion denied: %s has no valid snapID", targetResourceName)
	}
	targetSnapID := *target.Status.SnapID

	relatedBackups := make([]*finv1.FinBackup, 0, len(list.Items))
	for i := range list.Items {
		b := &list.Items[i]

		if b.Spec.Node != target.Spec.Node ||
			b.Spec.PVC != targetPVCName ||
			b.Spec.PVCNamespace != targetPVCNamespace {
			continue
		}

		if b.Status.SnapID == nil {
			relatedResourceName := fmt.Sprintf("FinBackup(name=%s, namespace=%s)", b.Name, b.Namespace)
			return nil, fmt.Errorf("deletion denied: related %s (node=%s, pvc=%s/%s) has no snapID",
				relatedResourceName, b.Spec.Node, b.Spec.PVCNamespace, b.Spec.PVC)
		}

		relatedBackups = append(relatedBackups, b)
	}

	if len(relatedBackups) == 0 {
		return nil, nil
	}

	minSnapID := targetSnapID
	for _, backup := range relatedBackups {
		snapID := *backup.Status.SnapID
		if snapID < minSnapID {
			minSnapID = snapID
		}
	}

	if targetSnapID == minSnapID {
		return nil, nil
	}

	return nil, fmt.Errorf(
		"deletion denied: %s is not a full backup (node=%s, pvc=%s/%s); deletable full backup has snapID=%d",
		targetResourceName, target.Spec.Node, targetPVCNamespace, targetPVCName, minSnapID,
	)
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type FinBackup.
func (v *FinBackupCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type FinBackup.
func (v *FinBackupCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (
	admission.Warnings, error) {
	return nil, nil
}

package v1

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	finv1 "github.com/cybozu-go/fin/api/v1"
)

// nolint:unused
// log is for logging in this package.
var finbackuplog = log.Log.WithName("finbackup-resource")

// SetupFinBackupWebhookWithManager registers the webhook for FinBackup in the manager.
func SetupFinBackupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&finv1.FinBackup{}).
		WithValidator(&FinBackupCustomValidator{client: mgr.GetClient(), apiReader: mgr.GetAPIReader()}).
		Complete()
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-fin-cybozu-io-v1-finbackup,mutating=false,failurePolicy=fail,sideEffects=None,groups=fin.cybozu.io,resources=finbackups,verbs=create;update;delete,versions=v1,name=vfinbackup-v1.kb.io,admissionReviewVersions=v1
// +kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackups,verbs=get;list;watch

// FinBackupCustomValidator struct is responsible for validating the FinBackup resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type FinBackupCustomValidator struct {
	client    client.Client
	apiReader client.Reader
}

var _ webhook.CustomValidator = &FinBackupCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type FinBackup.
func (v *FinBackupCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	finbackup, ok := obj.(*finv1.FinBackup)
	if !ok {
		return nil, fmt.Errorf("expected a FinBackup object but got %T", obj)
	}
	finbackuplog.Info("Validation for FinBackup upon creation", "name", finbackup.GetName())

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type FinBackup.
func (v *FinBackupCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	finbackup, ok := newObj.(*finv1.FinBackup)
	if !ok {
		return nil, fmt.Errorf("expected a FinBackup object for the newObj but got %T", newObj)
	}
	finbackuplog.Info("Validation for FinBackup upon update", "name", finbackup.GetName())

	return nil, nil
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

	// Here we use apiReader instead of the cached client.
	// The reason is to ensure that this FinBackup is truly the oldest one.
	// Since apiReader sends requests directly to the API server,
	// there is a risk of hitting the API server’s rate limit.
	// However, such issues can usually be resolved by retries.
	// Therefore, we intentionally use apiReader here with this risk in mind.
	var list finv1.FinBackupList
	if err := v.apiReader.List(ctx, &list, client.InNamespace(target.Namespace)); err != nil {
		logger.Error(err, "failed to list FinBackups")
		return nil, err
	}

	relatedBackups := make([]finv1.FinBackup, 0, len(list.Items))
	for _, b := range list.Items {
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

	fullBackup := slices.MinFunc(relatedBackups, func(a, b finv1.FinBackup) int {
		return cmp.Compare(*a.Status.SnapID, *b.Status.SnapID)
	})

	if *target.Status.SnapID == *fullBackup.Status.SnapID {
		return nil, nil
	}

	return nil, fmt.Errorf(
		"deletion denied: %s is not a full backup (node=%s, pvc=%s/%s); deletable full backup has snapID=%d",
		targetResourceName, target.Spec.Node, targetPVCNamespace, targetPVCName, *fullBackup.Status.SnapID,
	)
}

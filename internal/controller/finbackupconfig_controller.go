package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	finv1 "github.com/cybozu-go/fin/api/v1"
)

// FinBackupConfigReconciler reconciles a FinBackupConfig object
type FinBackupConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func NewFinBackupConfigReconciler(
	client client.Client,
	scheme *runtime.Scheme,
) *FinBackupConfigReconciler {
	return &FinBackupConfigReconciler{
		Client: client,
		Scheme: scheme,
	}
}

//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackupconfigs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackupconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackupconfigs/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the FinBackupConfig object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *FinBackupConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	// TODO(user): your logic here

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *FinBackupConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&finv1.FinBackupConfig{}).
		Complete(r)
}

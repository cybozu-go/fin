package controller

import (
	"context"
	"fmt"
	"time"

	"errors"

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	finv1 "github.com/cybozu-go/fin/api/v1"
)

const (
	FinRestoreFinalizerName = "finrestore.fin.cybozu.io/finalizer"

	// annotations
	annotationFinRestoreName      = "fin.cybozu.io/finrestore-name"
	annotationFinRestoreNamespace = "fin.cybozu.io/finrestore-namespace"
)

// FinRestoreReconciler reconciles a FinRestore object
type FinRestoreReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	cephClusterNamespace string
	podImage             string
	rawImageChunkSize    *resource.Quantity
}

func NewFinRestoreReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	namespace string,
	podImage string,
	rawImageChunkSize *resource.Quantity,
) *FinRestoreReconciler {
	return &FinRestoreReconciler{
		Client:               client,
		Scheme:               scheme,
		cephClusterNamespace: namespace,
		podImage:             podImage,
		rawImageChunkSize:    rawImageChunkSize,
	}
}

func restoreJobName(restore *finv1.FinRestore) string {
	return "fin-restore-" + string(restore.GetUID())
}

//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finrestores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finrestores/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finrestores/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get
//+kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the FinRestore object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *FinRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var restore finv1.FinRestore
	err := r.Get(ctx, req.NamespacedName, &restore)
	if k8serrors.IsNotFound(err) {
		logger.Info("FinRestore resource not found", "error", err)
		return ctrl.Result{}, nil
	}
	if err != nil {
		logger.Error(err, "failed to get FinRestore")
		return ctrl.Result{}, err
	}

	if restore.GetNamespace() != r.cephClusterNamespace {
		logger.Info("restore is not managed by the target Ceph cluster",
			"restore", restore.Name, "namespace", restore.Namespace, "clusterNamespace", r.cephClusterNamespace)
		return ctrl.Result{}, nil
	}

	if restore.DeletionTimestamp.IsZero() {
		return r.reconcileCreateOrUpdate(ctx, &restore)
	} else {
		return r.reconcileDelete(ctx, &restore)
	}
}

func (r *FinRestoreReconciler) reconcileCreateOrUpdate(ctx context.Context,
	restore *finv1.FinRestore) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if restore.IsReady() {
		return ctrl.Result{}, nil
	}

	controllerutil.AddFinalizer(restore, FinRestoreFinalizerName)
	err := r.Update(ctx, restore)
	if err != nil {
		logger.Error(err, "failed to add finalizer")
		return ctrl.Result{}, err
	}

	var backup finv1.FinBackup
	err = r.Get(ctx, client.ObjectKey{Name: restore.Spec.Backup, Namespace: restore.Namespace}, &backup)
	if err != nil {
		logger.Error(err, "failed to get FinBackup", "name", restore.Spec.Backup, "namespace", restore.Namespace)
		return ctrl.Result{}, err
	}

	if !backup.IsReady() {
		logger.Info("backup is not ready to use", "backup", backup.Name, "namespace", backup.Namespace)

		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if err := r.createOrUpdateRestorePVC(ctx, restore, &backup); err != nil {
		logger.Error(err, "failed to create restore PV")
	}

	if err := r.createOrUpdateRestoreJobPV(ctx, restore); err != nil {
		logger.Error(err, "failed to create restore job PV")
		return ctrl.Result{}, err
	}

	if err := r.createOrUpdateRestoreJobPVC(ctx, restore); err != nil {
		logger.Error(err, "failed to create restore job PVC")
		return ctrl.Result{}, err
	}

	err = r.createOrUpdateRestoreJob(ctx, restore, &backup, r.rawImageChunkSize)
	if err != nil {
		logger.Error(err, "failed to create or update restore job")
		return ctrl.Result{}, err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{Namespace: backup.GetNamespace(), Name: restoreJobName(restore)}, &job)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get restore job: %w", err)
	}

	done, err := jobCompleted(&job)
	if err != nil {
		logger.Error(err, "restore job failed")
		return ctrl.Result{}, err
	}
	if !done {
		return ctrl.Result{}, nil
	}

	meta.SetStatusCondition(&restore.Status.Conditions, metav1.Condition{
		Type:    finv1.RestoreConditionReadyToUse,
		Status:  metav1.ConditionTrue,
		Reason:  "RestoreCompleted",
		Message: "Restore completed successfully",
	})
	err = r.Status().Update(ctx, restore)
	if err != nil {
		logger.Error(err, "failed to update status", "status", restore.Status)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *FinRestoreReconciler) createOrUpdateRestoreJob(
	ctx context.Context, restore *finv1.FinRestore, backup *finv1.FinBackup,
	rawImageChunkSize *resource.Quantity,
) error {
	return errors.New("not implemented")
}

func (r *FinRestoreReconciler) createOrUpdateRestorePVC(ctx context.Context, restore *finv1.FinRestore,
	backup *finv1.FinBackup) error {
	return errors.New("not implemented")
}

func (r *FinRestoreReconciler) createOrUpdateRestoreJobPV(ctx context.Context, restore *finv1.FinRestore) error {
	return errors.New("not implemented")
}

func (r *FinRestoreReconciler) createOrUpdateRestoreJobPVC(ctx context.Context, restore *finv1.FinRestore) error {
	return errors.New("not implemented")
}

func (r *FinRestoreReconciler) reconcileDelete(ctx context.Context, restore *finv1.FinRestore) (ctrl.Result, error) {
	// TODO: We must implement this function later.
	return ctrl.Result{}, errors.New("not implemented")
}

// SetupWithManager sets up the controller with the Manager.
func (r *FinRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&finv1.FinRestore{}).
		Watches(&batchv1.Job{},
			handler.EnqueueRequestsFromMapFunc(enqueueOnJobEvent(
				annotationFinRestoreName, annotationFinRestoreNamespace)),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: enqueueOnJobCompletion,
			})).
		Complete(r)
}

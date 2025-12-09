package controller

import (
	"context"
	"fmt"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/pkg/metrics"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	finBackupConfigFinalizerName = "finbackupconfig.fin.cybozu.io/finalizer"
)

// FinBackupConfigReconciler reconciles a FinBackupConfig object
type FinBackupConfigReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	managedCephClusterID string
	overwriteFBCSchedule string
	podImage             string
	serviceAccountName   string
}

func NewFinBackupConfigReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	overwriteFBCSchedule string,
	managedCephClusterID string,
	podImage string,
	serviceAccountName string,
) *FinBackupConfigReconciler {
	return &FinBackupConfigReconciler{
		Client:               client,
		Scheme:               scheme,
		managedCephClusterID: managedCephClusterID,
		overwriteFBCSchedule: overwriteFBCSchedule,
		podImage:             podImage,
		serviceAccountName:   serviceAccountName,
	}
}

//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackupconfigs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackupconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackupconfigs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

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
	logger := log.FromContext(ctx)

	var fbc finv1.FinBackupConfig
	if err := r.Get(ctx, req.NamespacedName, &fbc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !fbc.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&fbc, finBackupConfigFinalizerName) {
			controllerutil.RemoveFinalizer(&fbc, finBackupConfigFinalizerName)
			if err := r.Update(ctx, &fbc); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from FinBackupConfig: %w", err)
			}
		}
		metrics.DeleteFinBackupConfigInfo(&fbc, r.managedCephClusterID)
		return ctrl.Result{}, nil
	}

	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: fbc.Spec.PVCNamespace, Name: fbc.Spec.PVC}, &pvc); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get PVC %s/%s: %w", fbc.Spec.PVCNamespace, fbc.Spec.PVC, err)
	}

	ok, err := checkCephCluster(ctx, r.Client, &pvc, r.managedCephClusterID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check Ceph cluster: %w", err)
	}
	if !ok {
		logger.Info("the target pvc is not managed by this controller")
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&fbc, finBackupConfigFinalizerName) {
		controllerutil.AddFinalizer(&fbc, finBackupConfigFinalizerName)
		if err := r.Update(ctx, &fbc); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer to FinBackupConfig: %w", err)
		}
	}

	metrics.SetFinBackupConfigInfo(&fbc, r.managedCephClusterID)

	image := r.podImage
	serviceAccountName := r.serviceAccountName
	if err := r.createOrUpdateCronJob(ctx, &fbc, fbc.Namespace, serviceAccountName, image); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create or update CronJob: %w", err)
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *FinBackupConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&finv1.FinBackupConfig{}).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}

func (r *FinBackupConfigReconciler) createOrUpdateCronJob(
	ctx context.Context,
	fbc *finv1.FinBackupConfig,
	namespace string,
	serviceAccountName string,
	image string,
) error {
	cronJobName := "fbc-" + string(fbc.UID)

	cronJob := &batchv1.CronJob{}
	cronJob.SetName(cronJobName)
	cronJob.SetNamespace(namespace)

	_, err := ctrl.CreateOrUpdate(ctx, r.Client, cronJob, func() error {
		cronJob.Spec.Schedule = fbc.Spec.Schedule
		if r.overwriteFBCSchedule != "" {
			cronJob.Spec.Schedule = r.overwriteFBCSchedule
		}
		cronJob.Spec.Suspend = &fbc.Spec.Suspend
		var startingDeadlineSeconds int64 = 3600
		cronJob.Spec.StartingDeadlineSeconds = &startingDeadlineSeconds
		cronJob.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		var backoffLimit int32 = 65535
		cronJob.Spec.JobTemplate.Spec.BackoffLimit = &backoffLimit

		podSpec := &cronJob.Spec.JobTemplate.Spec.Template.Spec
		podSpec.ServiceAccountName = serviceAccountName
		podSpec.RestartPolicy = corev1.RestartPolicyOnFailure

		if len(podSpec.Containers) == 0 {
			podSpec.Containers = make([]corev1.Container, 1)
		}
		container := &podSpec.Containers[0]
		container.Name = "create-finbackup-job"
		container.Image = image
		container.ImagePullPolicy = corev1.PullIfNotPresent
		container.Command = []string{
			"/manager",
			"create-finbackup-job",
			"--fin-backup-config-name=" + fbc.GetName(),
			"--fin-backup-config-namespace=" + fbc.GetNamespace(),
		}

		container.Env = []corev1.EnvVar{
			{
				Name: "JOB_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.labels['batch.kubernetes.io/job-name']",
					},
				},
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
		}

		if err := controllerutil.SetControllerReference(fbc, cronJob, r.Scheme); err != nil {
			return fmt.Errorf("failed to set owner reference on CronJob: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create CronJob: %s: %w", cronJobName, err)
	}
	return nil
}

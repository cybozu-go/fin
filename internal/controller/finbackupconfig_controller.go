package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	finv1 "github.com/cybozu-go/fin/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var errEmptyClusterID error = errors.New("cluster ID is empty")

// FinBackupConfigReconciler reconciles a FinBackupConfig object
type FinBackupConfigReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	finCephClusterID string
}

func NewFinBackupConfigReconciler(
	cli client.Client,
	scheme *runtime.Scheme,
) *FinBackupConfigReconciler {
	return &FinBackupConfigReconciler{
		Client: cli,
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
	logger := log.FromContext(ctx)

	var fbc finv1.FinBackupConfig
	if err := r.Get(ctx, req.NamespacedName, &fbc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: fbc.Namespace, Name: fbc.Spec.PVC}, &pvc); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get PVC %s/%s: %w", fbc.Namespace, fbc.Spec.PVC, err)
	}

	clusterID, err := getCephClusterIDFromPVC(ctx, r.Client, &pvc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get Ceph cluster ID: %s: %s: %w", fbc.Namespace, fbc.Spec.PVC, err)
	}
	if clusterID != r.finCephClusterID {
		logger.Info("the target pvc is not managed by this controller")
		return ctrl.Result{}, nil
	}

	pod, err := getRunningPod(ctx, r.Client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get running pod: %w", err)
	}
	if len(pod.Spec.Containers) == 0 {
		return ctrl.Result{}, fmt.Errorf("running pod has no containers")
	}
	image := pod.Spec.Containers[0].Image

	serviceAccountName := os.Getenv("CREATE_FINBACKUP_JOB_SERVICE_ACCOUNT")

	if err := r.createOrUpdateCronJob(ctx, &fbc, fbc.Namespace, serviceAccountName, image); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create or update CronJob: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *FinBackupConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&finv1.FinBackupConfig{}).
		Watches(
			&batchv1.CronJob{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, cronJob client.Object) []reconcile.Request {
				name := cronJob.GetName()
				const prefix = "fbc-"
				if !strings.HasPrefix(name, prefix) {
					return []reconcile.Request{}
				}
				uid := name[len(prefix):]
				var fbcs finv1.FinBackupConfigList
				if err := r.List(ctx, &fbcs, &client.ListOptions{
					FieldSelector: fields.OneTermEqualSelector(".metadata.uid", uid),
				}); err != nil {
					return []reconcile.Request{}
				}
				if len(fbcs.Items) != 1 {
					return []reconcile.Request{}
				}
				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      fbcs.Items[0].Name,
							Namespace: fbcs.Items[0].Namespace,
						},
					},
				}
			}),
		).
		Complete(r)
}

func getCephClusterIDFromSCName(ctx context.Context, k8sClient client.Client, storageClassName string) (string, error) {
	var storageClass storagev1.StorageClass
	err := k8sClient.Get(ctx, types.NamespacedName{Name: storageClassName}, &storageClass)
	if err != nil {
		return "", fmt.Errorf("failed to get StorageClass: %s: %w", storageClassName, err)
	}

	if !strings.HasSuffix(storageClass.Provisioner, ".rbd.csi.ceph.com") {
		return "", fmt.Errorf("SC is not managed by RBD: %s: %w", storageClassName, errEmptyClusterID)
	}
	clusterID, ok := storageClass.Parameters["clusterID"]
	if !ok {
		return "", fmt.Errorf("clusterID not found: %s: %w", storageClassName, errEmptyClusterID)
	}

	return clusterID, nil
}

func getCephClusterIDFromPVC(
	ctx context.Context,
	k8sClient client.Client,
	pvc *corev1.PersistentVolumeClaim,
) (string, error) {
	logger := log.FromContext(ctx)

	storageClassName := pvc.Spec.StorageClassName
	if storageClassName == nil {
		logger.Info("not managed storage class", "namespace", pvc.Namespace, "pvc", pvc.Name)
		return "", nil
	}

	clusterID, err := getCephClusterIDFromSCName(ctx, k8sClient, *storageClassName)
	if err != nil {
		logger.Info("failed to get ceph cluster ID from StorageClass name",
			"error", err, "namespace", pvc.Namespace, "pvc", pvc.Name, "storageClassName", *storageClassName)
		if errors.Is(err, errEmptyClusterID) {
			return "", nil
		}
		return "", err
	}

	return clusterID, nil
}

var getRunningPod func(ctx context.Context, client client.Client) (*corev1.Pod, error) = getRunningPodImpl

func getRunningPodImpl(ctx context.Context, client client.Client) (*corev1.Pod, error) {
	name, ok := os.LookupEnv("POD_NAME")
	if !ok {
		return nil, fmt.Errorf("POD_NAME not found")
	}
	namespace, ok := os.LookupEnv("POD_NAMESPACE")
	if !ok {
		return nil, fmt.Errorf("POD_NAMESPACE not found")
	}
	var pod corev1.Pod
	if err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &pod); err != nil {
		return nil, fmt.Errorf("failed to get pod: %w", err)
	}
	return &pod, nil
}

func (r *FinBackupConfigReconciler) createOrUpdateCronJob(
	ctx context.Context,
	fbc *finv1.FinBackupConfig,
	namespace string,
	serviceAccountName string,
	image string,
) error {
	logger := log.FromContext(ctx)
	cronJobName := "fbc-" + string(fbc.UID)

	cronJob := &batchv1.CronJob{}
	cronJob.SetName(cronJobName)
	cronJob.SetNamespace(namespace)

	op, err := ctrl.CreateOrUpdate(ctx, r.Client, cronJob, func() error {
		// schedule & suspend
		cronJob.Spec.Schedule = fbc.Spec.Schedule
		cronJob.Spec.Suspend = &fbc.Spec.Suspend
		cronJob.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		var startingDeadlineSeconds int64 = 3600
		cronJob.Spec.StartingDeadlineSeconds = &startingDeadlineSeconds
		var backoffLimit int32 = 65535
		cronJob.Spec.JobTemplate.Spec.BackoffLimit = &backoffLimit

		podSpec := &cronJob.Spec.JobTemplate.Spec.Template.Spec
		podSpec.ServiceAccountName = serviceAccountName
		podSpec.RestartPolicy = corev1.RestartPolicyOnFailure

		if len(podSpec.Containers) == 0 {
			podSpec.Containers = append(podSpec.Containers, corev1.Container{})
		}
		container := &podSpec.Containers[0]
		container.Name = "create-finbackup-job"
		container.Image = image
		container.ImagePullPolicy = corev1.PullIfNotPresent
		container.Command = []string{
			"/fin-controller",
			"create-finbackup-job",
			"--fin-backup-config-name", fbc.GetName(),
			"--fin-backup-config-namespace", fbc.GetNamespace(),
		}

		envName := "JobName"
		envIndex := -1
		for i, e := range container.Env {
			if e.Name == envName {
				envIndex = i
				break
			}
		}
		if envIndex == -1 {
			container.Env = append(container.Env, corev1.EnvVar{Name: envName})
			envIndex = len(container.Env) - 1
		}
		env := &container.Env[envIndex]
		if env.ValueFrom == nil {
			env.ValueFrom = &corev1.EnvVarSource{}
		}
		if env.ValueFrom.FieldRef == nil {
			env.ValueFrom.FieldRef = &corev1.ObjectFieldSelector{}
		}
		env.ValueFrom.FieldRef.FieldPath = "metadata.labels['batch.kubernetes.io/job-name']"

		envName = "JobCreationTimestamp"
		envIndex = -1
		for i, e := range container.Env {
			if e.Name == envName {
				envIndex = i
				break
			}
		}
		if envIndex == -1 {
			container.Env = append(container.Env, corev1.EnvVar{Name: envName})
			envIndex = len(container.Env) - 1
		}
		env = &container.Env[envIndex]
		if env.ValueFrom == nil {
			env.ValueFrom = &corev1.EnvVarSource{}
		}
		if env.ValueFrom.FieldRef == nil {
			env.ValueFrom.FieldRef = &corev1.ObjectFieldSelector{}
		}
		env.ValueFrom.FieldRef.FieldPath = "metadata.creationTimestamp"

		if err := controllerutil.SetControllerReference(fbc, cronJob, r.Scheme); err != nil {
			return fmt.Errorf("failed to set owner reference on CronJob: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create CronJob: %s: %w", cronJobName, err)
	}
	if op != controllerutil.OperationResultNone {
		logger.Info(fmt.Sprintf("CronJob successfully created or updated: %s", cronJobName))
	}
	return nil
}

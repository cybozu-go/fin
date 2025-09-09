package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	rvol "github.com/cybozu-go/fin/internal/infrastructure/restore"
)

const (
	FinRestoreFinalizerName = "finrestore.fin.cybozu.io/finalizer"

	// labels
	labelComponentRestoreJob = "restore-job"

	// annotations
	annotationFinRestoreName      = "fin.cybozu.io/finrestore-name"
	annotationFinRestoreNamespace = "fin.cybozu.io/finrestore-namespace"
	annotationRestoredBy          = "fin.cybozu.io/restored-by"
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

func restoreJobPVCName(restore *finv1.FinRestore) string {
	return restoreJobName(restore)
}

func restoreJobPVName(restore *finv1.FinRestore) string {
	return restoreJobName(restore)
}

func restorePVCName(restore *finv1.FinRestore) string {
	if restore.Spec.PVC != "" {
		return restore.Spec.PVC
	}
	return restore.Name
}

func restorePVCNamespace(restore *finv1.FinRestore) string {
	if restore.Spec.PVCNamespace != "" {
		return restore.Spec.PVCNamespace
	}
	return restore.Namespace
}

//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finrestores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finrestores/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finrestores/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch

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

	if restore.DeletionTimestamp.IsZero() {
		return r.reconcileCreateOrUpdate(ctx, &restore)
	} else {
		return r.reconcileDelete(ctx, &restore)
	}
}

func (r *FinRestoreReconciler) reconcileCreateOrUpdate(
	ctx context.Context,
	restore *finv1.FinRestore,
) (ctrl.Result, error) {
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

	src, err := r.getRestorePVCSource(&backup)
	if err != nil {
		logger.Error(err, "failed to get restore PVC source from backup",
			"backup", backup.Name, "namespace", backup.Namespace)
		return ctrl.Result{}, err
	}
	if err := r.createOrUpdateRestorePVC(ctx, restore, &backup, src); err != nil {
		logger.Error(err, "failed to create restore PVC")
		return ctrl.Result{}, err
	}

	var restorePVC corev1.PersistentVolumeClaim
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: restorePVCNamespace(restore),
		Name:      restorePVCName(restore),
	}, &restorePVC); err != nil {
		logger.Error(err, "failed to get restore PVC")
		return ctrl.Result{}, err
	}
	var restorePV corev1.PersistentVolume
	if err := r.Get(ctx, client.ObjectKey{
		Name: restorePVC.Spec.VolumeName,
	}, &restorePV); err != nil {
		logger.Error(err, "failed to get restore PV")
		return ctrl.Result{}, err
	}

	if err := r.createOrUpdateRestoreJobPV(ctx, restore, &restorePV); err != nil {
		logger.Error(err, "failed to create restore job PV")
		return ctrl.Result{}, err
	}

	if err := r.createOrUpdateRestoreJobPVC(ctx, restore, &restorePVC); err != nil {
		logger.Error(err, "failed to create restore job PVC")
		return ctrl.Result{}, err
	}

	err = r.createOrUpdateRestoreJob(ctx, restore, &backup, r.rawImageChunkSize)
	if err != nil {
		logger.Error(err, "failed to create or update restore job")
		return ctrl.Result{}, err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{Namespace: r.cephClusterNamespace, Name: restoreJobName(restore)}, &job)
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
	var job batchv1.Job
	job.SetName(restoreJobName(restore))
	job.SetNamespace(r.cephClusterNamespace)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentRestoreJob
		job.SetLabels(labels)

		annotations := job.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[annotationFinRestoreName] = restore.GetName()
		annotations[annotationFinRestoreNamespace] = restore.GetNamespace()
		job.SetAnnotations(annotations)

		// Up to this point, we modify the mutable fields. From here on, we
		// modify the immutable fields, which cannot be changed after creation.
		if !job.CreationTimestamp.IsZero() {
			return nil
		}

		job.Spec.BackoffLimit = ptr.To(int32(maxJobBackoffLimit))

		job.Spec.Template.Spec.NodeName = backup.Spec.Node

		job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			RunAsUser:  ptr.To(int64(0)),
			RunAsGroup: ptr.To(int64(0)),
		}

		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure

		job.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "restore",
				Command: []string{"/manager"},
				Args:    []string{"restore"},
				Env: []corev1.EnvVar{
					{
						Name:  "ACTION_UID",
						Value: string(restore.GetUID()),
					},
					{
						Name:  "TARGET_SNAPSHOT_ID",
						Value: strconv.Itoa(*backup.Status.SnapID),
					},
					{
						Name:  "BACKUP_TARGET_PVC_NAME",
						Value: backup.Spec.PVC,
					},
					{
						Name:  "BACKUP_TARGET_PVC_NAMESPACE",
						Value: backup.Spec.PVCNamespace,
					},
					{
						Name:  "BACKUP_TARGET_PVC_UID",
						Value: backup.GetLabels()[labelBackupTargetPVCUID],
					},
					{
						Name:  "RAW_IMAGE_CHUNK_SIZE",
						Value: strconv.FormatInt(rawImageChunkSize.Value(), 10),
					},
				},
				Image:           r.podImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				VolumeMounts: []corev1.VolumeMount{
					{
						MountPath: nlv.VolumePath,
						Name:      "fin-volume",
					},
				},
				VolumeDevices: []corev1.VolumeDevice{
					{
						DevicePath: rvol.VolumePath,
						Name:       "restore-job-volume",
					},
				},
			},
		}

		job.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "fin-volume",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: finVolumePVCName(backup),
					},
				},
			},
			{
				Name: "restore-job-volume",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: restoreJobPVCName(restore),
					},
				},
			},
		}

		return nil
	})
	return err
}

func (r *FinRestoreReconciler) getRestorePVCSource(backup *finv1.FinBackup) (*corev1.PersistentVolumeClaim, error) {
	var src corev1.PersistentVolumeClaim
	if err := json.Unmarshal([]byte(backup.Status.PVCManifest), &src); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pvc stored in FinBackup %s/%s: %w",
			backup.GetNamespace(), backup.GetName(), err)
	}
	return &src, nil
}

func (r *FinRestoreReconciler) createOrUpdateRestorePVC(
	ctx context.Context,
	restore *finv1.FinRestore,
	backup *finv1.FinBackup,
	src *corev1.PersistentVolumeClaim,
) error {
	var pvc corev1.PersistentVolumeClaim
	name := restorePVCName(restore)
	namespace := restorePVCNamespace(restore)
	pvc.SetName(name)
	pvc.SetNamespace(namespace)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &pvc, func() error {
		// Copy essential fields from the source PVC.
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		restoredBy := string(restore.GetUID())
		if pvc.CreationTimestamp.IsZero() {
			pvc.Annotations[annotationRestoredBy] = restoredBy
		} else if pvc.Annotations[annotationRestoredBy] != restoredBy {
			return fmt.Errorf("failed to manage restore pvc due to uid mismatch: %s/%s",
				namespace, name)
		}

		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = map[corev1.ResourceName]resource.Quantity{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] =
			*resource.NewQuantity(*backup.Status.SnapSize, resource.BinarySI)

		// Up to this point, we modify the mutable fields. From here on, we
		// modify the immutable fields, which cannot be changed after creation.
		if !pvc.CreationTimestamp.IsZero() {
			return nil
		}

		pvc.Spec.AccessModes = src.Spec.AccessModes
		pvc.Spec.StorageClassName = src.Spec.StorageClassName
		pvc.Spec.VolumeAttributesClassName = src.Spec.VolumeAttributesClassName
		pvc.Spec.VolumeMode = src.Spec.VolumeMode

		return nil
	})
	return err
}

func (r *FinRestoreReconciler) createOrUpdateRestoreJobPV(
	ctx context.Context,
	restore *finv1.FinRestore,
	restorePV *corev1.PersistentVolume,
) error {
	var pv corev1.PersistentVolume
	pv.SetName(restoreJobPVName(restore))
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &pv, func() error {
		labels := pv.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentRestoreJob
		pv.SetLabels(labels)

		// Up to this point, we modify the mutable fields. From here on, we
		// modify the immutable fields, which cannot be changed after creation.
		if !pv.CreationTimestamp.IsZero() {
			return nil
		}

		pv.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		pv.Spec.Capacity = restorePV.Spec.Capacity
		pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		pv.Spec.StorageClassName = ""

		volumeMode := corev1.PersistentVolumeBlock
		pv.Spec.VolumeMode = &volumeMode

		if pv.Spec.CSI == nil {
			pv.Spec.CSI = &corev1.CSIPersistentVolumeSource{}
		}
		pv.Spec.CSI.Driver = restorePV.Spec.CSI.Driver
		pv.Spec.CSI.ControllerExpandSecretRef = restorePV.Spec.CSI.ControllerExpandSecretRef
		pv.Spec.CSI.NodeStageSecretRef = restorePV.Spec.CSI.NodeStageSecretRef
		pv.Spec.CSI.VolumeHandle = restorePV.Spec.CSI.VolumeAttributes["imageName"]

		if pv.Spec.CSI.VolumeAttributes == nil {
			pv.Spec.CSI.VolumeAttributes = map[string]string{}
		}
		pv.Spec.CSI.VolumeAttributes["clusterID"] = restorePV.Spec.CSI.VolumeAttributes["clusterID"]
		pv.Spec.CSI.VolumeAttributes["imageFeatures"] = restorePV.Spec.CSI.VolumeAttributes["imageFeatures"]
		pv.Spec.CSI.VolumeAttributes["imageFormat"] = restorePV.Spec.CSI.VolumeAttributes["imageFormat"]
		pv.Spec.CSI.VolumeAttributes["pool"] = restorePV.Spec.CSI.VolumeAttributes["pool"]
		pv.Spec.CSI.VolumeAttributes["staticVolume"] = "true"

		return nil
	})
	return err
}

func (r *FinRestoreReconciler) createOrUpdateRestoreJobPVC(
	ctx context.Context,
	restore *finv1.FinRestore,
	restorePVC *corev1.PersistentVolumeClaim,
) error {
	var pvc corev1.PersistentVolumeClaim
	pvc.SetName(restoreJobPVCName(restore))
	pvc.SetNamespace(r.cephClusterNamespace)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &pvc, func() error {
		labels := pvc.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentRestoreJob
		pvc.SetLabels(labels)

		// Up to this point, we modify the mutable fields. From here on, we
		// modify the immutable fields, which cannot be changed after creation.
		if !pvc.CreationTimestamp.IsZero() {
			return nil
		}

		storageClassName := ""
		pvc.Spec.StorageClassName = &storageClassName
		pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		pvc.Spec.Resources = restorePVC.Spec.Resources
		pvc.Spec.VolumeName = restoreJobPVName(restore)

		volumeMode := corev1.PersistentVolumeBlock
		pvc.Spec.VolumeMode = &volumeMode

		return nil
	})
	return err
}

func (r *FinRestoreReconciler) reconcileDelete(ctx context.Context, restore *finv1.FinRestore) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	restoreJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreJobName(restore),
			Namespace: r.cephClusterNamespace,
		},
	}
	propagationPolicy := metav1.DeletePropagationBackground
	err := r.Delete(ctx, restoreJob,
		&client.DeleteOptions{
			PropagationPolicy: &propagationPolicy,
		})
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to delete restore job")
			return ctrl.Result{}, err
		}
	} else {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	restoreJobPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreJobPVCName(restore),
			Namespace: r.cephClusterNamespace,
		},
	}
	err = r.Delete(ctx, restoreJobPVC)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to delete restore job PVC")
			return ctrl.Result{}, err
		}
	} else {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	restoreJobPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: restoreJobPVName(restore),
		},
	}
	err = r.Delete(ctx, restoreJobPV)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to delete restore job PV")
			return ctrl.Result{}, err
		}
	} else {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	controllerutil.RemoveFinalizer(restore, FinRestoreFinalizerName)
	if err = r.Update(ctx, restore); err != nil {
		logger.Error(err, "failed to remove finalizer")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
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

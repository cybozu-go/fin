package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	"github.com/cybozu-go/fin/internal/model"
	"github.com/go-logr/logr"
)

const (
	FinBackupFinalizerName = "finbackup.fin.cybozu.io/finalizer"

	// Labels
	labelBackupTargetPVCUID   = "fin.cybozu.io/backup-target-pvc-uid"
	labelAppNameValue         = "fin"
	labelComponentBackupJob   = "backup-job"
	labelComponentCleanupJob  = "cleanup-job"
	labelComponentDeletionJob = "deletion-job"

	// Annotations
	annotationBackupTargetRBDImage = "fin.cybozu.io/backup-target-rbd-image"
	annotationRBDPool              = "fin.cybozu.io/rbd-pool"
	annotationDiffFrom             = "fin.cybozu.io/diff-from"
	annotationFinBackupName        = "fin.cybozu.io/finbackup-name"
	annotationFinBackupNamespace   = "fin.cybozu.io/finbackup-namespace"
)

var (
	errSnapshotNotFound = errors.New("snapshot not found")
)

// FinBackupReconciler reconciles a FinBackup object
type FinBackupReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	cephClusterNamespace    string
	podImage                string
	maxPartSize             *resource.Quantity
	snapRepo                model.RBDSnapshotRepository
	rawImgExpansionUnitSize uint64
}

func NewFinBackupReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	cephClusterNamespace string,
	podImage string,
	maxPartSize *resource.Quantity,
	snapRepo model.RBDSnapshotRepository,
	rawImgExpansionUnitSize uint64,
) *FinBackupReconciler {
	return &FinBackupReconciler{
		Client:                  client,
		Scheme:                  scheme,
		cephClusterNamespace:    cephClusterNamespace,
		podImage:                podImage,
		maxPartSize:             maxPartSize,
		snapRepo:                snapRepo,
		rawImgExpansionUnitSize: rawImgExpansionUnitSize,
	}
}

//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackups/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the FinBackup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *FinBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var backup finv1.FinBackup
	err := r.Get(ctx, req.NamespacedName, &backup)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "failed to get FinBackup")
		return ctrl.Result{}, err
	}

	if !backup.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &backup)
	}

	if backup.GetNamespace() != r.cephClusterNamespace {
		return ctrl.Result{}, nil
	}

	if backup.IsReady() {
		return ctrl.Result{}, nil
	}

	finalizersUpdated := controllerutil.AddFinalizer(&backup, FinBackupFinalizerName)
	if finalizersUpdated {
		err := r.Update(ctx, &backup)
		if err != nil {
			logger.Error(err, "failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	if backup.Status.SnapID == nil {
		var pvc corev1.PersistentVolumeClaim
		err = r.Get(ctx, client.ObjectKey{Namespace: backup.Spec.PVCNamespace, Name: backup.Spec.PVC}, &pvc)
		if err != nil {
			logger.Error(err, "failed to get backup target PVC")
			return ctrl.Result{}, err
		}

		backup.SetLabels(map[string]string{
			labelBackupTargetPVCUID: string(pvc.GetUID()),
		})

		var pv corev1.PersistentVolume
		err = r.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv)
		if err != nil {
			logger.Error(err, "failed to get backup target PV")
			return ctrl.Result{}, err
		}

		rbdImage := pv.Spec.CSI.VolumeAttributes["imageName"]
		if rbdImage == "" {
			return ctrl.Result{}, errors.New("imageName is not specified in PV")
		}
		rbdPool := pv.Spec.CSI.VolumeAttributes["pool"]
		if rbdPool == "" {
			return ctrl.Result{}, errors.New("pool is not specified in PV")
		}

		backup.SetAnnotations(map[string]string{
			annotationBackupTargetRBDImage: rbdImage,
			annotationRBDPool:              rbdPool,
		})
		err = r.Update(ctx, &backup)
		if err != nil {
			logger.Error(err, "failed to add labels and annotations")
			return ctrl.Result{}, err
		}

		snap, err := r.createSnapshotIfNeeded(rbdPool, rbdImage, snapshotName(&backup))
		if err != nil {
			logger.Error(err, "failed to create or get snapshot")
			return ctrl.Result{}, err
		}
		backup.Status.CreatedAt = metav1.NewTime(snap.Timestamp.Time)
		backup.Status.SnapID = &snap.ID

		pvcManifest, err := json.Marshal(pvc)
		if err != nil {
			logger.Error(err, "failed to marshal PVC manifest")
			return ctrl.Result{}, err
		}
		backup.Status.PVCManifest = string(pvcManifest)
		err = r.Status().Update(ctx, &backup)
		if err != nil {
			logger.Error(err, "failed to update FinBackup status")
			return ctrl.Result{}, err
		}
		// FIXME: The following "requeue after" is temporary code.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	var pvc corev1.PersistentVolumeClaim
	err = r.Get(ctx, client.ObjectKey{Namespace: backup.Spec.PVCNamespace, Name: backup.Spec.PVC}, &pvc)
	if err != nil {
		logger.Error(err, "failed to get backup target PVC")
		return ctrl.Result{}, err
	}

	err = checkPVCUIDConsistency(&backup, &pvc)
	if err != nil {
		logger.Error(err, "backup target PVC UID is inconsistent")
		return ctrl.Result{}, err
	}

	var finBackupList finv1.FinBackupList
	err = r.List(ctx, &finBackupList, &client.ListOptions{
		Namespace:     backup.GetNamespace(),
		LabelSelector: labels.SelectorFromSet(map[string]string{labelBackupTargetPVCUID: string(pvc.GetUID())}),
	})
	if err != nil {
		logger.Error(err, "failed to list FinBackups for the PVC")
		return ctrl.Result{}, err
	}

	if !snapIDPreconditionSatisfied(&logger, &backup, &finBackupList) {
		// FIXME: The following "requeue after" is temporary code.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	diffFromStr, updated := findDiffSourceSnapID(&backup, &finBackupList)
	if updated {
		backup.SetAnnotations(map[string]string{annotationDiffFrom: diffFromStr})
		err = r.Update(ctx, &backup)
		if err != nil {
			logger.Error(err, "failed to update FinBackup with diffFrom annotation")
			return ctrl.Result{}, err
		}
	}

	err = r.createOrUpdateBackupJob(ctx, &backup, diffFromStr, string(pvc.GetUID()), r.maxPartSize)
	if err != nil {
		logger.Error(err, "failed to create or update backup job")
		return ctrl.Result{}, err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{Namespace: backup.GetNamespace(), Name: backupJobName(&backup)}, &job)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	done, err := jobCompleted(&job)
	if err != nil {
		logger.Error(err, "job failed")
		return ctrl.Result{}, err
	}
	if !done {
		return ctrl.Result{}, nil
	}

	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:    finv1.BackupConditionReadyToUse,
		Status:  metav1.ConditionTrue,
		Reason:  "BackupCompleted",
		Message: "Backup completed successfully",
	})

	err = r.Status().Update(ctx, &backup)
	if err != nil {
		logger.Error(err, "failed to update FinBackup status")
		return ctrl.Result{}, err
	}
	logger.Info("FinBackup has become ready to use")

	return ctrl.Result{}, nil
}

func (r *FinBackupReconciler) reconcileDelete(ctx context.Context, backup *finv1.FinBackup) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	backupJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupJobName(backup),
			Namespace: backup.GetNamespace(),
		},
	}
	err := r.Delete(ctx, backupJob)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to delete backup job")
			return ctrl.Result{}, err
		}
	} else {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// If backup job does not exist, proceed to next step
	if backup.Status.SnapID != nil {
		err = r.createOrUpdateCleanupJob(ctx, backup)
		if err != nil {
			logger.Error(err, "failed to create or update cleanup job")
			return ctrl.Result{}, err
		}

		var cleanupJob batchv1.Job
		err = r.Get(ctx, client.ObjectKey{Namespace: backup.GetNamespace(), Name: cleanupJobName(backup)}, &cleanupJob)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get cleanup job: %w", err)
		}
		done, err := jobCompleted(&cleanupJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		err = r.createOrUpdateDeletionJob(ctx, backup)
		if err != nil {
			logger.Error(err, "failed to create or update deletion job")
			return ctrl.Result{}, err
		}

		var deletionJob batchv1.Job
		err = r.Get(ctx, client.ObjectKey{Namespace: backup.GetNamespace(), Name: deletionJobName(backup)}, &deletionJob)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get deletion job: %w", err)
		}
		done, err = jobCompleted(&deletionJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
		}

		err = r.Delete(ctx, &deletionJob)
		if err != nil && !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to delete deletion job")
			return ctrl.Result{}, err
		}
		err = r.Delete(ctx, &cleanupJob)
		if err != nil && !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to delete cleanup job")
			return ctrl.Result{}, err
		}
	}

	rbdPool := backup.GetAnnotations()[annotationRBDPool]
	rbdImage := backup.GetAnnotations()[annotationBackupTargetRBDImage]
	snapshotName := snapshotName(backup)
	if err := r.removeSnapshot(rbdPool, rbdImage, snapshotName); err != nil {
		if !errors.Is(err, errSnapshotNotFound) {
			logger.Error(err, "failed to remove RBD snapshot")
			return ctrl.Result{}, err
		}
		logger.Info("RBD snapshot not found, skipping removal", "pool", rbdPool, "image", rbdImage, "snapName", snapshotName)
	}

	controllerutil.RemoveFinalizer(backup, FinBackupFinalizerName)
	if err = r.Update(ctx, backup); err != nil {
		logger.Error(err, "failed to remove finalizer")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *FinBackupReconciler) createSnapshotIfNeeded(rbdPool, rbdImage, snapName string) (*model.RBDSnapshot, error) {
	snap, err := r.getSnapshot(rbdPool, rbdImage, snapName)
	if err != nil {
		if !errors.Is(err, errSnapshotNotFound) {
			return nil, fmt.Errorf("failed to get snapshot: %w", err)
		}
		err = r.snapRepo.CreateSnapshot(rbdPool, rbdImage, snapName)
		if err != nil {
			return nil, fmt.Errorf("failed to create snapshot: %w", err)
		}
		snap, err = r.getSnapshot(rbdPool, rbdImage, snapName)
		if err != nil {
			return nil, fmt.Errorf("failed to get snapshot after creation: %w", err)
		}
	}
	return snap, nil
}

func (r *FinBackupReconciler) removeSnapshot(rbdPool, rbdImage, snapName string) error {
	_, err := r.getSnapshot(rbdPool, rbdImage, snapName)
	if err != nil {
		return err
	}
	err = r.snapRepo.RemoveSnapshot(rbdPool, rbdImage, snapName)
	if err != nil {
		return fmt.Errorf("failed to remove snapshot: %w", err)
	}
	return nil
}

func (r *FinBackupReconciler) getSnapshot(rbdPool, rbdImage, snapName string) (*model.RBDSnapshot, error) {
	snapshots, err := r.snapRepo.ListSnapshots(rbdPool, rbdImage)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	if len(snapshots) == 0 {
		return nil, fmt.Errorf("%w (snapshot: %s, pool: %s, image: %s)", errSnapshotNotFound, snapName, rbdPool, rbdImage)
	}
	i := slices.IndexFunc(snapshots, func(s *model.RBDSnapshot) bool {
		return s.Name == snapName
	})
	if i == -1 {
		return nil, fmt.Errorf("%w (snapshot: %s, pool: %s, image: %s)", errSnapshotNotFound, snapName, rbdPool, rbdImage)
	}
	return snapshots[i], nil
}

func checkPVCUIDConsistency(backup *finv1.FinBackup, pvc *corev1.PersistentVolumeClaim) error {
	if pvc.GetUID() != types.UID(backup.GetLabels()[labelBackupTargetPVCUID]) {
		return fmt.Errorf("backup target PVC UID does not match (inLabel=%s, actual=%s)",
			backup.GetLabels()[labelBackupTargetPVCUID], pvc.GetUID())
	}
	pvcFromManifest := &corev1.PersistentVolumeClaim{}
	err := json.Unmarshal([]byte(backup.Status.PVCManifest), pvcFromManifest)
	if err != nil {
		return fmt.Errorf("failed to unmarshal PVC manifest: %w", err)
	}
	if pvc.GetUID() != pvcFromManifest.GetUID() {
		return fmt.Errorf("backup target PVC UID does not match (inStatus=%s, actual=%s)",
			pvcFromManifest.GetUID(), pvc.GetUID())
	}
	return nil
}

func snapIDPreconditionSatisfied(
	logger *logr.Logger,
	backup *finv1.FinBackup,
	finBackupList *finv1.FinBackupList,
) bool {
	for _, fb := range finBackupList.Items {
		if fb.GetUID() == backup.GetUID() {
			continue
		}
		if fb.Status.SnapID == nil {
			logger.Info("found another FinBackup with nil SnapID", "name", fb.Name)
			return false
		}
		if *fb.Status.SnapID < *backup.Status.SnapID {
			if !fb.IsReady() &&
				fb.DeletionTimestamp.IsZero() &&
				fb.Spec.Node == backup.Spec.Node {
				logger.Info("found another FinBackup which has smaller SnapID and is not ready",
					"name", fb.Name, "snapID", *fb.Status.SnapID)
				return false
			}
		}
	}
	return true
}

func findDiffSourceSnapID(backup *finv1.FinBackup, finBackupList *finv1.FinBackupList) (string, bool) {
	diffFromStr := backup.GetAnnotations()[annotationDiffFrom]
	if diffFromStr != "" {
		return diffFromStr, false
	}

	var diffFrom *int
	for _, fb := range finBackupList.Items {
		if *fb.Status.SnapID < *backup.Status.SnapID &&
			fb.DeletionTimestamp.IsZero() &&
			fb.Spec.Node == backup.Spec.Node {
			if diffFrom == nil || *diffFrom < *fb.Status.SnapID {
				diffFrom = fb.Status.SnapID
			}
		}
	}
	updated := false
	if diffFrom != nil {
		diffFromStr = strconv.Itoa(*diffFrom)
		updated = true
	}
	return diffFromStr, updated
}

func snapshotName(backup *finv1.FinBackup) string {
	return fmt.Sprintf("fin-backup-%s", backup.GetUID())
}

func backupJobName(backup *finv1.FinBackup) string {
	return "fin-backup-" + string(backup.GetUID())
}

func deletionJobName(backup *finv1.FinBackup) string {
	return "fin-deletion-" + string(backup.GetUID())
}

func cleanupJobName(backup *finv1.FinBackup) string {
	return "fin-cleanup-" + string(backup.GetUID())
}

func (r *FinBackupReconciler) createOrUpdateBackupJob(
	ctx context.Context, backup *finv1.FinBackup, diffFrom string,
	backupTargetPVCUID string, maxPartSize *resource.Quantity,
) error {
	var job batchv1.Job
	job.SetName("fin-backup-" + string(backup.GetUID()))
	job.SetNamespace(r.cephClusterNamespace)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentBackupJob
		job.SetLabels(labels)

		annotations := job.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[annotationFinBackupName] = backup.GetName()
		annotations[annotationFinBackupNamespace] = backup.GetNamespace()
		job.SetAnnotations(annotations)

		job.Spec.BackoffLimit = ptr.To(int32(maxJobBackoffLimit))

		job.Spec.Template.Spec.NodeName = backup.Spec.Node

		job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			FSGroup:      ptr.To(int64(10000)),
			RunAsGroup:   ptr.To(int64(10000)),
			RunAsNonRoot: ptr.To(true),
			RunAsUser:    ptr.To(int64(10000)),
		}

		job.Spec.Template.Spec.ServiceAccountName = "fin-backup-job"

		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure

		job.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "backup",
				Command: []string{"/manager"},
				Args:    []string{"backup"},
				Env: []corev1.EnvVar{
					{
						Name:  "ACTION_UID",
						Value: string(backup.GetUID()),
					},
					{
						Name:  "RBD_POOL",
						Value: backup.GetAnnotations()[annotationRBDPool],
					},
					{
						Name:  "RBD_IMAGE_NAME",
						Value: backup.GetAnnotations()[annotationBackupTargetRBDImage],
					},
					{
						Name:  "BACKUP_SNAPSHOT_ID",
						Value: strconv.Itoa(*backup.Status.SnapID),
					},
					{
						Name:  "SOURCE_CANDIDATE_SNAPSHOT_ID",
						Value: diffFrom,
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
						Value: backupTargetPVCUID,
					},
					{
						Name:  "MAX_PART_SIZE",
						Value: strconv.FormatInt(maxPartSize.Value(), 10),
					},
				},
				Image:           r.podImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				VolumeMounts: []corev1.VolumeMount{
					{
						MountPath: "/etc/ceph",
						Name:      "ceph-config",
					},
					{
						MountPath: nlv.VolumePath,
						Name:      "fin-volume",
					},
				},
			},
			{
				Name:    "toolbox",
				Command: []string{"/bin/bash", "-c", embeddedToolboxScript},
				Env: []corev1.EnvVar{
					{
						Name: "ROOK_CEPH_USERNAME",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								Key: "ceph-username",
								LocalObjectReference: corev1.LocalObjectReference{
									Name: "rook-ceph-mon",
								},
							},
						},
					},
				},
				Image:           r.podImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				SecurityContext: &corev1.SecurityContext{
					RunAsGroup:   ptr.To(int64(2016)),
					RunAsNonRoot: ptr.To(true),
					RunAsUser:    ptr.To(int64(2016)),
				},
				VolumeMounts: []corev1.VolumeMount{
					{
						MountPath: "/var/lib/rook-ceph-mon",
						Name:      "ceph-admin-secret",
						ReadOnly:  true,
					},
					{
						MountPath: "/etc/ceph",
						Name:      "ceph-config",
					},
					{
						MountPath: "/etc/rook",
						Name:      "mon-endpoint-volume",
					},
				},
			},
		}
		if r.rawImgExpansionUnitSize != 0 {
			job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{
				Name:  "FIN_RAW_IMG_EXPANSION_UNIT_SIZE",
				Value: strconv.FormatUint(r.rawImgExpansionUnitSize, 10),
			})
		}

		job.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "ceph-admin-secret",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: "rook-ceph-mon",
						Optional:   ptr.To(false),
						Items: []corev1.KeyToPath{{
							Key:  "ceph-secret",
							Path: "secret.keyring",
						}},
					},
				},
			},
			{
				Name: "ceph-config",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
			{
				Name: "fin-volume",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: finVolumePVCName(backup),
					},
				},
			},
			{
				Name: "mon-endpoint-volume",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						Items: []corev1.KeyToPath{
							{
								Key:  "data",
								Path: "mon-endpoints",
							},
						},
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "rook-ceph-mon-endpoints",
						},
					},
				},
			},
		}

		return nil
	})
	return err
}

//nolint:dupl
func (r *FinBackupReconciler) createOrUpdateDeletionJob(ctx context.Context, backup *finv1.FinBackup) error {
	var job batchv1.Job
	job.SetName(deletionJobName(backup))
	job.SetNamespace(r.cephClusterNamespace)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentDeletionJob
		job.SetLabels(labels)

		job.Spec.BackoffLimit = ptr.To(int32(maxJobBackoffLimit))

		job.Spec.Template.Spec.NodeName = backup.Spec.Node

		job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			FSGroup:      ptr.To(int64(10000)),
			RunAsGroup:   ptr.To(int64(10000)),
			RunAsNonRoot: ptr.To(true),
			RunAsUser:    ptr.To(int64(10000)),
		}

		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure

		job.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "deletion",
				Command: []string{"/manager"},
				Args:    []string{"deletion"},
				Env: []corev1.EnvVar{
					{
						Name:  "ACTION_UID",
						Value: string(backup.GetUID()),
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
				},
				Image:           r.podImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				VolumeMounts: []corev1.VolumeMount{
					{
						MountPath: nlv.VolumePath,
						Name:      "fin-volume",
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
		}

		return nil
	})
	return err
}

//nolint:dupl
func (r *FinBackupReconciler) createOrUpdateCleanupJob(ctx context.Context, backup *finv1.FinBackup) error {
	var job batchv1.Job
	job.SetName(cleanupJobName(backup))
	job.SetNamespace(r.cephClusterNamespace)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentCleanupJob
		job.SetLabels(labels)

		job.Spec.BackoffLimit = ptr.To(int32(maxJobBackoffLimit))

		job.Spec.Template.Spec.NodeName = backup.Spec.Node

		job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			FSGroup:      ptr.To(int64(10000)),
			RunAsGroup:   ptr.To(int64(10000)),
			RunAsNonRoot: ptr.To(true),
			RunAsUser:    ptr.To(int64(10000)),
		}

		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure

		job.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "cleanup",
				Command: []string{"/manager"},
				Args:    []string{"cleanup"},
				Env: []corev1.EnvVar{
					{
						Name:  "ACTION_UID",
						Value: string(backup.GetUID()),
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
				},
				Image:           r.podImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				VolumeMounts: []corev1.VolumeMount{
					{
						MountPath: nlv.VolumePath,
						Name:      "fin-volume",
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
		}

		return nil
	})
	return err
}

// SetupWithManager sets up the controller with the Manager.
func (r *FinBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&finv1.FinBackup{}).
		Watches(&batchv1.Job{},
			handler.EnqueueRequestsFromMapFunc(enqueueOnJobEvent(
				annotationFinBackupName, annotationFinBackupNamespace)),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: enqueueOnJobCompletion,
			})).
		Complete(r)
}

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/infrastructure/nlv"
	"github.com/cybozu-go/fin/internal/model"
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
)

const (
	FinBackupFinalizerName = "finbackup.fin.cybozu.io/finalizer"

	// Labels
	labelBackupTargetPVCUID   = "fin.cybozu.io/backup-target-pvc-uid"
	LabelFinBackupConfigUID   = "fin.cybozu.io/fbc-uid"
	labelAppNameValue         = "fin"
	labelComponentBackupJob   = "backup-job"
	labelComponentCleanupJob  = "cleanup-job"
	labelComponentDeletionJob = "deletion-job"

	// Annotations
	annotationBackupTargetRBDImage = "fin.cybozu.io/backup-target-rbd-image"
	annotationDiffFrom             = "fin.cybozu.io/diff-from"
	annotationFinBackupName        = "fin.cybozu.io/finbackup-name"
	annotationFinBackupNamespace   = "fin.cybozu.io/finbackup-namespace"
	annotationRBDPool              = "fin.cybozu.io/rbd-pool"
	annotationSkipVerify           = "fin.cybozu.io/skip-verify"
	AnnotationFullBackup           = "fin.cybozu.io/full-backup"
)

type verificationJobStatus int

const (
	verificationJobStatusComplete verificationJobStatus = iota
	verificationJobStatusInProgress
	verificationJobStatusFailedWithExitCode2
	verificationJobStatusUnknown
)

var (
	cleanupJobRequeueAfter  = 5 * time.Second
	deletionJobRequeueAfter = 1 * time.Minute

	errNonRetryableReconcile = errors.New("non retryable reconciliation error; " +
		"reconciliation must not keep going nor be retried")
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
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
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

	var pvc corev1.PersistentVolumeClaim
	err = r.Get(ctx, client.ObjectKey{Namespace: backup.Spec.PVCNamespace, Name: backup.Spec.PVC}, &pvc)
	if err != nil {
		logger.Error(err, "failed to get backup target PVC")
		return ctrl.Result{}, err
	}

	ok, err := checkCephCluster(ctx, r, &pvc, r.cephClusterNamespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ok {
		logger.Info("FinBackup skipped: Ceph cluster is not managed by this instance",
			"pvc", fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name),
			"cephClusterNamespace", r.cephClusterNamespace)
		return ctrl.Result{}, nil
	}

	if !backup.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &backup)
	}

	if backup.IsStoredToNode() {
		if backup.DoesVerifiedExist() {
			return ctrl.Result{}, nil
		}
	} else {
		result, err := r.reconcileBackup(ctx, backup, pvc)
		if errors.Is(err, errNonRetryableReconcile) {
			return ctrl.Result{}, nil
		}
		if err != nil || !result.IsZero() {
			return result, err
		}
	}

	verifResult, err := r.reconcileVerification(ctx, backup, pvc)
	if err != nil || !verifResult.IsZero() {
		return verifResult, err
	}

	if err := r.deleteOldFinBackup(ctx, &backup, &pvc); err != nil {
		logger.Error(err, "failed to perform automatic deletion of FinBackup")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// deleteOldFinBackup removes old FinBackup resources generated by FinBackupConfig.
func (r *FinBackupReconciler) deleteOldFinBackup(
	ctx context.Context,
	backup *finv1.FinBackup,
	pvc *corev1.PersistentVolumeClaim,
) error {
	logger := log.FromContext(ctx)
	fbcUID, ok := backup.GetLabels()[LabelFinBackupConfigUID]
	if !ok || fbcUID == "" {
		return nil
	}

	var finBackupList finv1.FinBackupList
	err := r.List(ctx, &finBackupList, &client.ListOptions{
		Namespace: backup.Namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			labelBackupTargetPVCUID: string(pvc.GetUID()),
			LabelFinBackupConfigUID: fbcUID,
		})})
	if err != nil {
		return fmt.Errorf("failed to list FinBackups for automatic deletion: %w", err)
	}

	var candidates []finv1.FinBackup
	for _, fb := range finBackupList.Items {
		if fb.Spec.Node != backup.Spec.Node {
			continue
		}
		if fb.Status.SnapID == nil {
			continue
		}
		if *fb.Status.SnapID < *backup.Status.SnapID {
			candidates = append(candidates, fb)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) > 1 {
		return fmt.Errorf(
			"only one older FinBackup is allowed on node %q (snapID < %d); found %d FinBackups",
			backup.Spec.Node, *backup.Status.SnapID, len(candidates),
		)
	}

	targetFB := &candidates[0]
	if err := r.Delete(ctx, targetFB); err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Info("target FinBackup already deleted", "target", client.ObjectKeyFromObject(targetFB))
			return nil
		}
		return fmt.Errorf("failed to delete FinBackup %s/%s: %w", targetFB.Namespace, targetFB.Name, err)
	}

	logger.Info("initiated deletion for older FinBackup managed by FinBackupConfig",
		"target", client.ObjectKeyFromObject(targetFB))
	return nil
}

func (r *FinBackupReconciler) reconcileBackup(
	ctx context.Context,
	backup finv1.FinBackup,
	pvc corev1.PersistentVolumeClaim,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	finalizersUpdated := controllerutil.AddFinalizer(&backup, FinBackupFinalizerName)
	if finalizersUpdated {
		err := r.Update(ctx, &backup)
		if err != nil {
			logger.Error(err, "failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	ret, err := r.createSnapshot(ctx, &backup)
	if err != nil {
		logger.Error(err, "failed to create snapshot")
		return ctrl.Result{}, err
	}
	if !ret.IsZero() {
		return ret, nil
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
	otherFinBackups := slices.DeleteFunc(finBackupList.Items, func(fb finv1.FinBackup) bool {
		return fb.GetUID() == backup.GetUID()
	})

	if err := snapIDPreconditionSatisfied(&backup, otherFinBackups); err != nil {
		logger.Info("wait for other FinBackups to become ready or be deleted", "reason", err.Error())
		// FIXME: The following "requeue after" is temporary code.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	diffFromStr, updated := findDiffSourceSnapID(&backup, otherFinBackups)
	if updated {
		annotations := backup.GetAnnotations()
		if annotations == nil {
			return ctrl.Result{}, errors.New("annotations must not be empty here")
		}
		annotations[annotationDiffFrom] = diffFromStr
		backup.SetAnnotations(annotations)
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
	err = r.Get(ctx, client.ObjectKey{Namespace: r.cephClusterNamespace, Name: backupJobName(&backup)}, &job)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return ctrl.Result{}, errNonRetryableReconcile
		}
		return ctrl.Result{}, err
	}

	done, err := jobCompleted(&job)
	if err != nil {
		logger.Error(err, "job failed")
		return ctrl.Result{}, err
	}
	if !done {
		return ctrl.Result{}, errNonRetryableReconcile
	}

	updatedBackup := backup.DeepCopy()
	meta.SetStatusCondition(&updatedBackup.Status.Conditions, metav1.Condition{
		Type:    finv1.BackupConditionStoredToNode,
		Status:  metav1.ConditionTrue,
		Reason:  "BackupCompleted",
		Message: "Backup completed successfully",
	})
	err = r.Status().Patch(ctx, updatedBackup, client.MergeFrom(&backup))
	if err != nil {
		logger.Error(err, "failed to update FinBackup status")
		return ctrl.Result{}, err
	}
	logger.Info("FinBackup has become ready to use")

	return ctrl.Result{}, nil
}

func (r *FinBackupReconciler) createSnapshot(ctx context.Context, backup *finv1.FinBackup) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if backup.Status.SnapID != nil {
		return ctrl.Result{}, nil
	}

	var pvc corev1.PersistentVolumeClaim
	err := r.Get(ctx, client.ObjectKey{Namespace: backup.Spec.PVCNamespace, Name: backup.Spec.PVC}, &pvc)
	if err != nil {
		logger.Error(err, "failed to get backup target PVC")
		return ctrl.Result{}, err
	}

	labels := backup.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[labelBackupTargetPVCUID] = string(pvc.GetUID())
	backup.SetLabels(labels)

	rbdPool, rbdImage, err := r.getRBDPoolAndImageFromPVC(ctx, &pvc)
	if err != nil {
		logger.Error(err, "failed to get pool/image from PVC")
		return ctrl.Result{}, err
	}

	annotations := backup.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[annotationBackupTargetRBDImage] = rbdImage
	annotations[annotationRBDPool] = rbdPool
	backup.SetAnnotations(annotations)

	err = r.Update(ctx, backup)
	if err != nil {
		logger.Error(err, "failed to add labels and annotations")
		return ctrl.Result{}, err
	}

	snap, err := r.createSnapshotIfNeeded(rbdPool, rbdImage, snapshotName(backup))
	if err != nil {
		logger.Error(err, "failed to create or get snapshot")
		return ctrl.Result{}, err
	}

	pvcManifest, err := json.Marshal(pvc)
	if err != nil {
		logger.Error(err, "failed to marshal PVC manifest")
		return ctrl.Result{}, err
	}
	updatedBackup := backup.DeepCopy()
	updatedBackup.Status.CreatedAt = metav1.NewTime(snap.Timestamp.Time)
	updatedBackup.Status.SnapID = &snap.ID
	updatedBackup.Status.SnapSize = ptr.To(int64(snap.Size))
	updatedBackup.Status.PVCManifest = string(pvcManifest)
	err = r.Status().Patch(ctx, updatedBackup, client.MergeFrom(backup))
	if err != nil {
		logger.Error(err, "failed to update FinBackup status")
		return ctrl.Result{}, err
	}
	// FIXME: The following "requeue after" is temporary code.
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *FinBackupReconciler) reconcileDelete(ctx context.Context, backup *finv1.FinBackup) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Delete the backup job if it exists
	backupJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupJobName(backup),
			Namespace: r.cephClusterNamespace,
		},
	}
	propagationPolicy := metav1.DeletePropagationBackground
	err := r.Delete(ctx, backupJob,
		&client.DeleteOptions{
			PropagationPolicy: &propagationPolicy,
		})
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to delete backup job")
			return ctrl.Result{}, err
		}
	} else {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// delete the verification job if it exists
	if err := r.deleteVerificationJob(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}

	// If backup job does not exist, proceed to next step
	if backup.Status.SnapID != nil {
		err = r.createOrUpdateCleanupJob(ctx, backup)
		if err != nil {
			logger.Error(err, "failed to create or update cleanup job")
			return ctrl.Result{}, err
		}

		var cleanupJob batchv1.Job
		err = r.Get(ctx, client.ObjectKey{Namespace: r.cephClusterNamespace, Name: cleanupJobName(backup)}, &cleanupJob)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get cleanup job: %w", err)
		}
		done, err := jobCompleted(&cleanupJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return ctrl.Result{RequeueAfter: cleanupJobRequeueAfter}, nil
		}

		err = r.createOrUpdateDeletionJob(ctx, backup)
		if err != nil {
			logger.Error(err, "failed to create or update deletion job")
			return ctrl.Result{}, err
		}

		var deletionJob batchv1.Job
		err = r.Get(ctx, client.ObjectKey{Namespace: r.cephClusterNamespace, Name: deletionJobName(backup)}, &deletionJob)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get deletion job: %w", err)
		}
		done, err = jobCompleted(&deletionJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return ctrl.Result{RequeueAfter: deletionJobRequeueAfter}, nil
		}

		propagationPolicy := metav1.DeletePropagationBackground
		err = r.Delete(ctx, &deletionJob,
			&client.DeleteOptions{
				PropagationPolicy: &propagationPolicy,
			})
		if err != nil && !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to delete deletion job")
			return ctrl.Result{}, err
		}
		err = r.Delete(ctx, &cleanupJob,
			&client.DeleteOptions{
				PropagationPolicy: &propagationPolicy,
			})
		if err != nil && !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to delete cleanup job")
			return ctrl.Result{}, err
		}
	}
	if err := r.removeSnapshot(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(backup, FinBackupFinalizerName)
	if err = r.Update(ctx, backup); err != nil {
		logger.Error(err, "failed to remove finalizer")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *FinBackupReconciler) createSnapshotIfNeeded(rbdPool, rbdImage, snapName string) (*model.RBDSnapshot, error) {
	snap, err := findSnapshot(r.snapRepo, rbdPool, rbdImage, snapName)
	if err != nil {
		if !errors.Is(err, model.ErrNotFound) {
			return nil, fmt.Errorf("failed to get snapshot: %w", err)
		}
		err = r.snapRepo.CreateSnapshot(rbdPool, rbdImage, snapName)
		if err != nil {
			return nil, fmt.Errorf("failed to create snapshot: %w", err)
		}
		snap, err = findSnapshot(r.snapRepo, rbdPool, rbdImage, snapName)
		if err != nil {
			return nil, fmt.Errorf("failed to get snapshot after creation: %w", err)
		}
	}
	return snap, nil
}

// removeSnapshot removes the RBD snapshot for the given FinBackup.
func (r *FinBackupReconciler) removeSnapshot(ctx context.Context, backup *finv1.FinBackup) error {
	logger := log.FromContext(ctx)
	rbdPool, rbdImage, err := r.getRBDPoolAndImage(ctx, backup)
	if err != nil {
		logger.Error(err, "failed to get RBD pool/image from FinBackup")
		return nil
	}

	snapName := snapshotName(backup)
	if _, err = findSnapshot(r.snapRepo, rbdPool, rbdImage, snapName); err != nil {
		if errors.Is(err, model.ErrNotFound) {
			logger.Info("RBD snapshot not found", "pool", rbdPool, "image", rbdImage, "snapName", snapName)
			return nil
		}
		return err
	}
	err = r.snapRepo.RemoveSnapshot(rbdPool, rbdImage, snapName)
	if err != nil {
		return fmt.Errorf("failed to remove snapshot: %w", err)
	}
	return nil
}

func (r *FinBackupReconciler) getRBDPoolAndImageFromPVC(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) (string, string, error) {
	if pvc == nil {
		return "", "", errors.New("pvc is nil")
	}
	var pv corev1.PersistentVolume
	if err := r.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv); err != nil {
		return "", "", err
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeAttributes == nil {
		return "", "", errors.New("PV.Spec.CSI attributes missing")
	}
	pool := pv.Spec.CSI.VolumeAttributes["pool"]
	image := pv.Spec.CSI.VolumeAttributes["imageName"]
	if pool == "" || image == "" {
		return "", "", fmt.Errorf("pool %q or imageName %q is empty", pool, image)
	}
	return pool, image, nil
}

func (r *FinBackupReconciler) getRBDPoolAndImage(ctx context.Context, backup *finv1.FinBackup) (string, string, error) {
	rbdPool := backup.GetAnnotations()[annotationRBDPool]
	rbdImage := backup.GetAnnotations()[annotationBackupTargetRBDImage]
	if rbdPool != "" && rbdImage != "" {
		return rbdPool, rbdImage, nil
	}

	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, client.ObjectKey{Namespace: backup.Spec.PVCNamespace, Name: backup.Spec.PVC}, &pvc); err != nil {
		return "", "", err
	}
	return r.getRBDPoolAndImageFromPVC(ctx, &pvc)
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

func snapIDPreconditionSatisfied(backup *finv1.FinBackup, otherFinBackups []finv1.FinBackup) error {
	smallerIDs := 0
	targetSnapID := *backup.Status.SnapID
	for _, fb := range otherFinBackups {
		if fb.Status.SnapID == nil {
			return fmt.Errorf("found another FinBackup with nil SnapID: %s", fb.Name)
		}
		if fb.Spec.Node != backup.Spec.Node {
			continue
		}

		snapID := *fb.Status.SnapID
		if snapID > targetSnapID {
			continue
		}
		if (!fb.IsStoredToNode() || !fb.IsVerifiedTrue()) && fb.DeletionTimestamp.IsZero() {
			return fmt.Errorf("found FinBackup not yet stored to node or verified: %s/%d", fb.Name, snapID)
		}
		smallerIDs++
		if smallerIDs >= 2 {
			return errors.New("found incremental FinBackup")
		}
	}
	return nil
}

func findDiffSourceSnapID(backup *finv1.FinBackup, otherFinBackups []finv1.FinBackup) (string, bool) {
	diffFromStr := backup.GetAnnotations()[annotationDiffFrom]
	if diffFromStr != "" {
		return diffFromStr, false
	}

	var diffFrom *int
	for _, fb := range otherFinBackups {
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

func verificationJobName(backup *finv1.FinBackup) string {
	return "fin-verify-" + string(backup.GetUID())
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
	job.SetName(backupJobName(backup))
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

		// Up to this point, we modify the mutable fields. From here on, we
		// modify the immutable fields, which cannot be changed after creation.
		if !job.CreationTimestamp.IsZero() {
			return nil
		}

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
					{
						Name:  EnvRawImgExpansionUnitSize,
						Value: strconv.FormatUint(r.rawImgExpansionUnitSize, 10),
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

		// Up to this point, we modify the mutable fields. From here on, we
		// modify the immutable fields, which cannot be changed after creation.
		if !job.CreationTimestamp.IsZero() {
			return nil
		}

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
					{
						Name:  EnvRawImgExpansionUnitSize,
						Value: strconv.FormatUint(r.rawImgExpansionUnitSize, 10),
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

		// Up to this point, we modify the mutable fields. From here on, we
		// modify the immutable fields, which cannot be changed after creation.
		if !job.CreationTimestamp.IsZero() {
			return nil
		}

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

func (r *FinBackupReconciler) reconcileVerification(
	ctx context.Context,
	backup finv1.FinBackup,
	pvc corev1.PersistentVolumeClaim,
) (ctrl.Result, error) {
	if r.checkSkipVerificationCondition(&backup) {
		return r.skipVerification(ctx, &backup)
	}

	if err := r.createOrUpdateVerificationJob(ctx, &backup, &pvc); err != nil {
		return ctrl.Result{}, err
	}

	jobStatus, err := r.checkVerificationJobStatus(ctx, &backup)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check verification job status: %w", err)
	}
	switch jobStatus {
	case verificationJobStatusComplete:
		// do nothing
	case verificationJobStatusInProgress:
		return ctrl.Result{}, nil
	case verificationJobStatusFailedWithExitCode2:
		if err := patchFinBackupCondition(ctx, r.Client, &backup, metav1.Condition{
			Type:    finv1.BackupConditionVerified,
			Status:  metav1.ConditionFalse,
			Reason:  "VerificationFailed",
			Message: "Backup verification failed: fsck error",
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to set FinBackup Verified condition to false: %w", err)
		}
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{}, fmt.Errorf("unknown verification job status: %d", jobStatus)
	}

	if err := patchFinBackupCondition(ctx, r.Client, &backup, metav1.Condition{
		Type:    finv1.BackupConditionVerified,
		Status:  metav1.ConditionTrue,
		Reason:  "VerificationComplete",
		Message: "Backup verification completed successfully: fsck passed",
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to set FinBackup Verified condition to true: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *FinBackupReconciler) checkSkipVerificationCondition(backup *finv1.FinBackup) bool {
	hasSkipVerifyAnnot := false
	if annots := backup.GetAnnotations(); annots != nil {
		hasSkipVerifyAnnot = annots[annotationSkipVerify] == "true"
	}
	return !backup.DoesVerifiedExist() && (hasSkipVerifyAnnot || backup.IsVerificationSkipped())
}

func (r *FinBackupReconciler) skipVerification(
	ctx context.Context,
	backup *finv1.FinBackup,
) (ctrl.Result, error) {
	// set the VerificationSkipped condition to True
	if err := patchFinBackupCondition(ctx, r.Client, backup, metav1.Condition{
		Type:   finv1.BackupConditionVerificationSkipped,
		Status: metav1.ConditionTrue,
		Reason: "AnnotationSet",
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to set FinBackup VerificationSkipped condition: %w", err)
	}

	// delete the verification job if it exists
	if err := r.deleteVerificationJob(ctx, backup); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete verification Job: %w", err)
	}

	// create or update the cleanup job
	if err := r.createOrUpdateCleanupJob(ctx, backup); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create or update cleanup Job: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *FinBackupReconciler) deleteVerificationJob(ctx context.Context, backup *finv1.FinBackup) error {
	// delete the verification job if it exists
	verificationJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      verificationJobName(backup),
			Namespace: r.cephClusterNamespace,
		},
	}
	if err := r.Delete(ctx, verificationJob,
		&client.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		},
	); err != nil {
		if !k8serrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete verification Job: %w", err)
		}
	}
	return nil
}

func (r *FinBackupReconciler) createOrUpdateVerificationJob(
	ctx context.Context,
	backup *finv1.FinBackup,
	pvc *corev1.PersistentVolumeClaim,
) error {
	var job batchv1.Job
	job.SetName(verificationJobName(backup))
	job.SetNamespace(r.cephClusterNamespace)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		annotations := job.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[annotationFinBackupName] = backup.GetName()
		annotations[annotationFinBackupNamespace] = backup.GetNamespace()
		job.SetAnnotations(annotations)

		// Up to this point, we modify the mutable fields. From here on, we
		// modify the immutable fields, which cannot be changed after creation.
		if !job.CreationTimestamp.IsZero() {
			return nil
		}

		job.Spec.BackoffLimit = ptr.To(int32(maxJobBackoffLimit))

		job.Spec.Template.Spec.NodeName = backup.Spec.Node

		job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			FSGroup:      ptr.To(int64(10000)),
			RunAsGroup:   ptr.To(int64(10000)),
			RunAsNonRoot: ptr.To(true),
			RunAsUser:    ptr.To(int64(10000)),
		}

		job.Spec.Template.Spec.ServiceAccountName = "fin-backup-job"

		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever

		job.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{
				{
					Action: batchv1.PodFailurePolicyActionFailJob,
					OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
						ContainerName: ptr.To("verification"),
						Operator:      batchv1.PodFailurePolicyOnExitCodesOpIn,
						Values:        []int32{2},
					},
				},
			},
		}

		job.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "verification",
				Command: []string{"/manager"},
				Args:    []string{"verification"},
				Env: []corev1.EnvVar{
					{
						Name:  "ACTION_UID",
						Value: string(backup.GetUID()),
					},
					{
						Name:  "BACKUP_SNAPSHOT_ID",
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
						Value: string(pvc.GetUID()),
					},
					{
						Name:  EnvRawImgExpansionUnitSize,
						Value: strconv.FormatUint(r.rawImgExpansionUnitSize, 10),
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
	if err != nil {
		return fmt.Errorf("failed to create or update verification Job: %w", err)
	}

	return nil
}

func (r *FinBackupReconciler) checkVerificationJobStatus(
	ctx context.Context,
	backup *finv1.FinBackup,
) (verificationJobStatus, error) {
	var job batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Namespace: r.cephClusterNamespace, Name: verificationJobName(backup)}, &job)
	if err != nil {
		return verificationJobStatusUnknown, fmt.Errorf("failed to get verification Job: %w", err)
	}

	done, err := jobCompleted(&job)
	if done { // Complete=True
		return verificationJobStatusComplete, nil
	} else if err == nil { // not Complete=True nor Failed=True
		return verificationJobStatusInProgress, nil
	}

	// Failed=True, so check the exit code
	pods := corev1.PodList{}
	if err := r.List(ctx, &pods, &client.ListOptions{
		Namespace: r.cephClusterNamespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			"batch.kubernetes.io/job-name": verificationJobName(backup),
		}),
	},
	); err != nil {
		return verificationJobStatusUnknown, fmt.Errorf("failed to list pods of verification Job: %w", err)
	}
	if len(pods.Items) != 1 {
		return verificationJobStatusUnknown, fmt.Errorf("expected 1 pod for verification Job, got %d", len(pods.Items))
	}

	containerStatuses := pods.Items[0].Status.ContainerStatuses
	if len(containerStatuses) != 1 {
		return verificationJobStatusUnknown,
			fmt.Errorf("expected 1 container in verification pod, got %d", len(containerStatuses))
	}

	state := containerStatuses[0].State
	if state.Terminated == nil {
		return verificationJobStatusUnknown, errors.New("verification container is not terminated")
	}

	if state.Terminated.ExitCode == 2 {
		return verificationJobStatusFailedWithExitCode2, nil
	}

	return verificationJobStatusUnknown, fmt.Errorf("verification job failed with exit code %d", state.Terminated.ExitCode)
}

// SetupWithManager sets up the controller with the Manager.
func (r *FinBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&finv1.FinBackup{}).
		Watches(&batchv1.Job{},
			handler.EnqueueRequestsFromMapFunc(enqueueOnJobEvent(
				annotationFinBackupName, annotationFinBackupNamespace)),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: enqueueOnJobCompletionOrFailure,
			})).
		Complete(r)
}

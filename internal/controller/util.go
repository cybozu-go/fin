package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	maxJobBackoffLimit = 65535

	EnvRawImgExpansionUnitSize = "FIN_RAW_IMG_EXPANSION_UNIT_SIZE"
)

func jobCompleted(job *batchv1.Job) (done bool, err error) {
	for _, c := range job.Status.Conditions {
		switch c.Type {
		case batchv1.JobComplete:
			if c.Status == corev1.ConditionTrue {
				return true, nil
			}
		case batchv1.JobFailed:
			if c.Status == corev1.ConditionTrue {
				return false, fmt.Errorf("job %s/%s failed: %s", job.Namespace, job.Name, c.Message)
			}
		}
	}
	return false, nil
}

func enqueueOnJobEvent(resourceName, resourceNamespace string) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		job, ok := obj.(*batchv1.Job)
		if !ok {
			return []reconcile.Request{}
		}
		if name, exist := job.GetAnnotations()[resourceName]; exist {
			if namespace, exist := job.GetAnnotations()[resourceNamespace]; exist {
				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      name,
							Namespace: namespace,
						},
					},
				}
			}
		}
		return []reconcile.Request{}
	}
}

func enqueueOnJobCompletionOrFailure(e event.UpdateEvent) bool {
	newJob, ok := e.ObjectNew.(*batchv1.Job)
	if !ok {
		return false
	}
	newCompleted, newErr := jobCompleted(newJob)
	newJobFinished := newErr != nil /* failed */ || newCompleted

	oldJob, ok := e.ObjectOld.(*batchv1.Job)
	if !ok {
		return false
	}
	oldCompleted, oldErr := jobCompleted(oldJob)
	oldJobFinished := oldErr != nil /* failed */ || oldCompleted

	// Enqueue when the job has just completed or failed.
	return newJobFinished && !oldJobFinished
}

func finVolumePVCName(backup *finv1.FinBackup) string {
	return "fin-" + backup.Spec.Node
}

// findSnapshot searches for a snapshot with the given name by using the
// provided RBDSnapshotListRepository. It returns model.ErrNotFound when not found.
func findSnapshot(
	repo model.RBDSnapshotListRepository,
	poolName, imageName, snapName string,
) (*model.RBDSnapshot, error) {
	snapshots, err := repo.ListSnapshots(poolName, imageName)
	if err != nil {
		return nil, err
	}

	for _, s := range snapshots {
		if s.Name == snapName {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: snapshot=%s pool=%s image=%s", model.ErrNotFound, snapName, poolName, imageName)
}

func checkCephCluster(
	ctx context.Context, reader client.Reader,
	pvc *corev1.PersistentVolumeClaim, cephCluster string,
) (bool, error) {
	scName := storagehelpers.GetPersistentVolumeClaimClass(pvc)
	var storageClass storagev1.StorageClass
	if err := reader.Get(ctx, types.NamespacedName{Name: scName}, &storageClass); err != nil {
		return false, fmt.Errorf("failed to get StorageClass: %q: %w", scName, err)
	}

	if !strings.HasSuffix(storageClass.Provisioner, ".rbd.csi.ceph.com") {
		return false, nil
	}
	clusterID, ok := storageClass.Parameters["clusterID"]
	if !ok {
		return false, nil
	}
	if clusterID != cephCluster {
		return false, nil
	}
	return true, nil
}

func patchFinBackupCondition(
	ctx context.Context,
	r client.Client,
	backup *finv1.FinBackup,
	condition metav1.Condition,
) (*finv1.FinBackup, error) {
	updatedBackup := backup.DeepCopy()
	meta.SetStatusCondition(&updatedBackup.Status.Conditions, condition)
	if err := r.Status().Patch(ctx, updatedBackup, client.MergeFrom(backup)); err != nil {
		return nil, fmt.Errorf("failed to update FinBackup condition: %w", err)
	}
	return updatedBackup, nil
}

func getBackupTargetPVCFromSpecOrStatus(
	ctx context.Context,
	r client.Client,
	backup *finv1.FinBackup,
) (*corev1.PersistentVolumeClaim, bool, error) {
	logger := log.FromContext(ctx)

	var pvc corev1.PersistentVolumeClaim
	var gotFromStatus bool
	err := r.Get(ctx, client.ObjectKey{Namespace: backup.Spec.PVCNamespace, Name: backup.Spec.PVC}, &pvc)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			logger.Error(err, "failed to get backup target PVC")
			return nil, false, err
		}
		pvcYaml := backup.Status.PVCManifest
		err = json.Unmarshal([]byte(pvcYaml), &pvc)
		if err != nil {
			logger.Error(err, "failed to get PVC manifest stored in FinBackup")
			return nil, false, err
		}
		gotFromStatus = true
	}

	return &pvc, gotFromStatus, nil
}

func isFullBackup(backup *finv1.FinBackup) bool {
	diffFrom := backup.GetAnnotations()[annotationDiffFrom]
	return diffFrom == ""
}

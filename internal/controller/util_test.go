package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	cephNamespace  = "rook-ceph" // Ceph, FinController, Jobs, PVCs for job are in this namespace
	cephClusterID  = "rook-ceph"
	workNamespace  = "work" // FinBackup, FinRestore are in this namespace
	userNamespace  = "user" // Users's PVCs are in this namespace
	otherNamespace = "other"
	podImage       = "sample-image"
	rbdPoolName    = "test-pool"
	rbdImageName   = "test-image"
)

func makeJobSucceeded(job *batchv1.Job) {
	job.Status.Conditions = []batchv1.JobCondition{
		{
			Type:   batchv1.JobComplete,
			Status: corev1.ConditionTrue,
		},
		{
			Type:   batchv1.JobSuccessCriteriaMet,
			Status: corev1.ConditionTrue,
		},
	}
	if job.Status.StartTime == nil {
		job.Status.StartTime = &metav1.Time{Time: time.Now()}
	}
	if job.Status.CompletionTime == nil {
		job.Status.CompletionTime = job.Status.StartTime
	}
	job.Status.Succeeded = 1
}

// NewPVCAndPV creates a PV and a PVC that are bound to each other.
func NewPVCAndPV(
	sc *storagev1.StorageClass,
	namespace string,
	pvcName string,
	pvName string,
	imageName string,
) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  pvName,
			VolumeMode:  ptr.To(corev1.PersistentVolumeBlock),
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Mi"),
				},
			},
			StorageClassName: ptr.To(sc.Name),
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       sc.Provisioner,
					VolumeHandle: imageName,
					VolumeAttributes: map[string]string{
						"clusterID": sc.Parameters["clusterID"],
						"pool":      sc.Parameters["pool"],
						"imageName": imageName,
					},
				},
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Mi"),
			},
			VolumeMode:  ptr.To(corev1.PersistentVolumeBlock),
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			ClaimRef: &corev1.ObjectReference{
				Namespace: namespace,
				Name:      pvc.Name,
			},
		},
	}
	return pvc, pv
}

func makeFinBackupMeetConditionAfterJobCompletes(
	ctx context.Context,
	finbackup *finv1.FinBackup,
	jobName string,
	condition func(*finv1.FinBackup) bool,
) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		key := types.NamespacedName{Name: jobName, Namespace: cephNamespace}
		var job batchv1.Job
		g.Expect(k8sClient.Get(ctx, key, &job)).To(Succeed())
		makeJobSucceeded(&job)
		err := k8sClient.Status().Update(ctx, &job)
		g.Expect(err).ShouldNot(HaveOccurred())
	}, "5s", "1s").Should(Succeed())

	Eventually(func(g Gomega) {
		var createdFinBackup finv1.FinBackup
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &createdFinBackup)
		g.Expect(err).ShouldNot(HaveOccurred())
		g.Expect(condition(&createdFinBackup)).Should(BeTrue(), "FinBackup should meet the condition")
	}, "5s", "1s").Should(Succeed())
}

func MakeFinBackupStoredToNode(ctx context.Context, finbackup *finv1.FinBackup) {
	GinkgoHelper()
	makeFinBackupMeetConditionAfterJobCompletes(
		ctx,
		finbackup,
		backupJobName(finbackup),
		func(fb *finv1.FinBackup) bool {
			return fb.IsStoredToNode()
		},
	)
}

func MakeFinBackupVerified(ctx context.Context, finbackup *finv1.FinBackup) {
	GinkgoHelper()
	makeFinBackupMeetConditionAfterJobCompletes(
		ctx,
		finbackup,
		verificationJobName(finbackup),
		func(fb *finv1.FinBackup) bool {
			return fb.IsVerifiedTrue()
		},
	)
}

func WaitForFinBackupRemoved(ctx context.Context, finbackup *finv1.FinBackup) {
	GinkgoHelper()

	var fb finv1.FinBackup
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &fb); k8serrors.IsNotFound(err) {
		return
	}
	for _, jobName := range []string{cleanupJobName(finbackup), deletionJobName(finbackup)} {
		Eventually(func(g Gomega) {
			key := types.NamespacedName{Name: jobName, Namespace: cephNamespace}
			var job batchv1.Job
			g.Expect(k8sClient.Get(ctx, key, &job)).To(Succeed())
			makeJobSucceeded(&job)
			err := k8sClient.Status().Update(ctx, &job)
			g.Expect(err).ShouldNot(HaveOccurred())
		}, "5s", "1s").Should(Succeed())
	}
	Eventually(func(g Gomega) {
		var fb finv1.FinBackup
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &fb)
		g.Expect(k8serrors.IsNotFound(err)).Should(BeTrue())
	}, "5s", "1s").Should(Succeed())
}

// DeletePVCAndPV deletes a PVC and its bound PV.
func DeletePVCAndPV(ctx context.Context, namespace, pvcName string) {
	GinkgoHelper()
	var pvc corev1.PersistentVolumeClaim
	err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: pvcName}, &pvc)
	if !k8serrors.IsNotFound(err) {
		controllerutil.RemoveFinalizer(&pvc, "kubernetes.io/pvc-protection")
		Expect(k8sClient.Status().Update(ctx, &pvc)).ShouldNot(HaveOccurred())
		Expect(k8sClient.Delete(ctx, &pvc)).Should(Succeed())
	}

	if pvc.Spec.VolumeName == "" {
		return
	}
	var pv corev1.PersistentVolume
	err = k8sClient.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv)
	if k8serrors.IsNotFound(err) {
		return
	}
	controllerutil.RemoveFinalizer(&pv, "kubernetes.io/pv-protection")
	Expect(k8sClient.Status().Update(ctx, &pv)).ShouldNot(HaveOccurred())
	Expect(k8sClient.Delete(ctx, &pv)).Should(Succeed())
	Eventually(func(g Gomega) {
		var pv corev1.PersistentVolume
		err = k8sClient.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv)
		g.Expect(k8serrors.IsNotFound(err)).Should(BeTrue())
	}, "5s", "1s").Should(Succeed())
}

func ExpectNoJob(ctx context.Context, k8sClient client.Client, jobName, namespace string) {
	GinkgoHelper()
	jobKey := types.NamespacedName{Name: jobName, Namespace: namespace}
	var job batchv1.Job
	err := k8sClient.Get(ctx, jobKey, &job)
	Expect(err).Should(HaveOccurred())
	Expect(k8serrors.IsNotFound(err)).Should(BeTrue())
}

func NewRBDStorageClass(prefix, cephClusterNamespace, poolName string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: fmt.Sprintf("%s-%s", prefix, cephClusterNamespace)},
		Provisioner: fmt.Sprintf("%s.rbd.csi.ceph.com", cephClusterNamespace),
		Parameters: map[string]string{
			"clusterID": cephClusterNamespace,
			"pool":      poolName,
		},
	}
}

func NewFinBackup(namespace, name, pvc, pvcNS, node string) *finv1.FinBackup {
	return &finv1.FinBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: finv1.FinBackupSpec{
			PVC:          pvc,
			PVCNamespace: pvcNS,
			Node:         node,
		},
	}
}

func NewFinRestore(namespace, name string, fbName, pvc, pvcNamespace string) *finv1.FinRestore {
	return &finv1.FinRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: finv1.FinRestoreSpec{
			PVC:          pvc,
			PVCNamespace: pvcNamespace,
			Backup:       fbName,
		},
	}
}

// createTwoBackupsOrdered creates the two provided FinBackup objects, waits until both
// have SnapIDs, then returns them ordered (smaller, larger) by SnapID.
func createTwoBackupsOrdered(
	ctx context.Context,
	c client.Client,
	fb1, fb2 *finv1.FinBackup,
) (smaller, larger *finv1.FinBackup) {
	GinkgoHelper()
	Expect(c.Create(ctx, fb1)).To(Succeed())
	Expect(c.Create(ctx, fb2)).To(Succeed())

	Eventually(func(g Gomega) {
		var a, b finv1.FinBackup
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(fb1), &a)).To(Succeed())
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(fb2), &b)).To(Succeed())
		g.Expect(a.Status.SnapID).NotTo(BeNil())
		g.Expect(b.Status.SnapID).NotTo(BeNil())
		if *a.Status.SnapID < *b.Status.SnapID {
			smaller, larger = &a, &b
		} else {
			smaller, larger = &b, &a
		}
	}, "5s", "1s").Should(Succeed())
	return smaller, larger
}

func CreateFinBackupStoredAndVerified(
	ctx context.Context,
	c client.Client,
	namespace, name string,
	pvc *corev1.PersistentVolumeClaim,
	snapID int,
	testNode string,
) *finv1.FinBackup {
	GinkgoHelper()

	finbackup := NewFinBackup(namespace, name, pvc.Name, pvc.Namespace, testNode)
	Expect(c.Create(ctx, finbackup)).Should(Succeed())
	pvcManifest, err := json.Marshal(pvc)
	Expect(err).ShouldNot(HaveOccurred())
	finbackup.Status.SnapSize = ptr.To(pvc.Spec.Resources.Requests.Storage().Value())
	finbackup.Status.SnapID = ptr.To(snapID)
	finbackup.Status.PVCManifest = string(pvcManifest)
	meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
		Type:   finv1.BackupConditionStoredToNode,
		Status: metav1.ConditionTrue,
		Reason: "BackupCompleted",
	})
	meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
		Type:   finv1.BackupConditionVerified,
		Status: metav1.ConditionTrue,
		Reason: "Verified",
	})
	Expect(c.Status().Update(ctx, finbackup)).Should(Succeed())
	return finbackup
}

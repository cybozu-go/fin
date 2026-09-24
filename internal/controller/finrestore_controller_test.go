package controller

import (
	"context"
	"encoding/json"
	"testing"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func createAndBindRestorePV(ctx context.Context, finrestore *finv1.FinRestore) {
	GinkgoHelper()

	var restorePVC corev1.PersistentVolumeClaim
	restorePVCKey := client.ObjectKey{Namespace: finrestore.Spec.PVCNamespace, Name: finrestore.Spec.PVC}
	Expect(k8sClient.Get(ctx, restorePVCKey, &restorePVC)).Should(Succeed())

	restorePV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: utils.GetUniqueName("restore-pv")},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: restorePVC.Spec.Resources.Requests[corev1.ResourceStorage],
			},
			ClaimRef: &corev1.ObjectReference{Namespace: restorePVC.Namespace, Name: restorePVC.Name, UID: restorePVC.UID},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "rbd.csi.ceph.com",
					VolumeAttributes: map[string]string{
						"clusterID":     restorePVC.Namespace,
						"pool":          rbdPoolName,
						"imageName":     rbdImageName,
						"imageFeatures": "layering",
						"imageFormat":   "2",
					},
					VolumeHandle: utils.GetUniqueName("restore-volume-handle"),
				},
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              *restorePVC.Spec.StorageClassName,
			VolumeMode:                    restorePVC.Spec.VolumeMode,
		},
	}
	Expect(k8sClient.Create(ctx, restorePV)).Should(Succeed())
	// Bind PVC to PV
	restorePVC.Spec.VolumeName = restorePV.Name
	Expect(k8sClient.Update(ctx, &restorePVC)).Should(Succeed())
	restorePVC.Status.Phase = corev1.ClaimBound
	Expect(k8sClient.Status().Update(ctx, &restorePVC)).Should(Succeed())
}

var _ = Describe("FinRestore Controller", func() {
	var reconciler *FinRestoreReconciler

	BeforeEach(func(ctx SpecContext) {
		reconciler = NewFinRestoreReconciler(
			k8sClient,
			scheme.Scheme,
			cephNamespace,
			podImage,
			ptr.To(resource.MustParse("4096")),
		)
	})

	It("checks createRestoreJobPVIfNotExists", func(ctx SpecContext) {
		var restore *finv1.FinRestore
		var pv *corev1.PersistentVolume

		By("creating dummy FinRestore and PV", func() {
			restore = &finv1.FinRestore{
				ObjectMeta: metav1.ObjectMeta{
					UID: types.UID(utils.GetUniqueName("uid-")),
				},
			}
			_, pv = NewPVCAndPV(normalSC, userNamespace, utils.GetUniqueName("pvc-"), utils.GetUniqueName("pv-"), rbdImageName)
		})

		By("checking result", func() {
			err := reconciler.createRestoreJobPVIfNotExists(ctx, restore, pv)
			Expect(err).ShouldNot(HaveOccurred())
			var jobPV corev1.PersistentVolume
			Expect(k8sClient.Get(ctx, client.ObjectKey{
				Name: restoreJobPVName(restore),
			}, &jobPV)).Should(Succeed())

			Expect(jobPV.UID).NotTo(BeEmpty())
			Expect(jobPV.Annotations).To(BeEmpty())
			Expect(jobPV.Labels).To(Equal(map[string]string{
				"app.kubernetes.io/name":      labelAppNameValue,
				"app.kubernetes.io/component": labelComponentRestoreJob,
			}))
			Expect(jobPV.Spec).To(Equal(corev1.PersistentVolumeSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Capacity:    pv.Spec.Capacity,
				ClaimRef: &corev1.ObjectReference{
					Namespace: cephNamespace,
					Name:      restoreJobPVCName(restore),
				},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver: pv.Spec.CSI.Driver,
						VolumeAttributes: map[string]string{
							"clusterID":     pv.Spec.CSI.VolumeAttributes["clusterID"],
							"imageFeatures": pv.Spec.CSI.VolumeAttributes["imageFeatures"],
							"imageFormat":   pv.Spec.CSI.VolumeAttributes["imageFormat"],
							"pool":          pv.Spec.CSI.VolumeAttributes["pool"],
							"staticVolume":  "true",
						},
						VolumeHandle: pv.Spec.CSI.VolumeAttributes["imageName"],
					},
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				StorageClassName:              "",
				VolumeMode:                    ptr.To(corev1.PersistentVolumeBlock),
			}))
		})

		By("recreate PV should be no-op", func() {
			err := reconciler.createRestoreJobPVIfNotExists(ctx, restore, pv)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	It("checks createRestoreJobPVCIfNotExists", func(ctx SpecContext) {
		var restore *finv1.FinRestore
		var pvc *corev1.PersistentVolumeClaim

		By("creating dummy FinRestore and PVC", func() {
			restore = &finv1.FinRestore{
				ObjectMeta: metav1.ObjectMeta{
					UID: types.UID(utils.GetUniqueName("uid-")),
				},
			}
			pvc, _ = NewPVCAndPV(normalSC, userNamespace, utils.GetUniqueName("pvc-"), utils.GetUniqueName("pv-"), rbdImageName)
		})

		By("checking result", func() {
			err := reconciler.createRestoreJobPVCIfNotExists(ctx, restore, pvc)
			Expect(err).ShouldNot(HaveOccurred())
			var jobPVC corev1.PersistentVolumeClaim
			Expect(k8sClient.Get(ctx, client.ObjectKey{
				Namespace: cephNamespace,
				Name:      restoreJobPVCName(restore),
			}, &jobPVC)).Should(Succeed())

			Expect(jobPVC.UID).NotTo(BeEmpty())
			Expect(jobPVC.Annotations).To(BeEmpty())
			Expect(jobPVC.Labels).To(Equal(map[string]string{
				"app.kubernetes.io/name":      labelAppNameValue,
				"app.kubernetes.io/component": labelComponentRestoreJob,
			}))
			Expect(jobPVC.Spec).To(Equal(corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:        pvc.Spec.Resources,
				StorageClassName: ptr.To(""),
				VolumeName:       restoreJobPVName(restore),
				VolumeMode:       ptr.To(corev1.PersistentVolumeBlock),
			}))
		})

		By("recreate PVC should be no-op", func() {
			err := reconciler.createRestoreJobPVCIfNotExists(ctx, restore, pvc)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})

var _ = Describe("FinRestore Controller Reconcile Test", Ordered, func() {
	var reconciler *FinRestoreReconciler

	BeforeEach(func(ctx SpecContext) {
		reconciler = NewFinRestoreReconciler(
			k8sClient,
			scheme.Scheme,
			cephNamespace,
			podImage,
			ptr.To(resource.MustParse("4096")),
		)
	})

	// CSATEST-1607
	// Description:
	//   Prevent restoring from FinBackup is not managed by the Fin instances.
	//
	// Arrange:
	//   - Create another storage class for another ceph cluster.
	//   - Create a PVC with the StorageClass.
	//   - Create a FinBackup targeting the PVC.
	//
	// Act:
	//   - Create a FinRestore targeting the FinBackup.
	//
	// Assert:
	//   - The reconciler does not return any errors.
	//   - The reconciler does not create a restore job.
	Context("Prevent restoring from FinBackup is not managed by the Fin instances", func() {
		var pvc2 *corev1.PersistentVolumeClaim
		var pv2 *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		var finrestore *finv1.FinRestore

		BeforeEach(func(ctx SpecContext) {
			By("creating PVC with the storage class")
			pvc2, pv2 = NewPVCAndPV(otherSC, otherNamespace, "test-pvc-2", "test-pv-2", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc2)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv2)).Should(Succeed())

			By("creating a FinBackup targeting a PVC in a different CephCluster")
			finbackup = NewFinBackup(workNamespace, "test-fin-backup-1", pvc2.Name, pvc2.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())

			By("creating a FinRestore targeting the FinBackup")
			finrestore = NewFinRestore(workNamespace, "test-restore-1", finbackup.Name, "restore-pvc", pvc2.Namespace)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc2.Namespace, pvc2.Name)
		})

		It("should neither return an error nor create a finrestore job during reconciliation", func(ctx SpecContext) {
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that no restore job is created")
			ExpectNoJob(ctx, k8sClient, restoreJobName(finrestore), cephNamespace)
		})
	})

	// Description:
	//   Stop restoring from a FinBackup whose node no longer exists.
	//
	// Arrange:
	//   - A backup-target PVC exists.
	//   - A restorable FinBackup naming a node that does not exist.
	//   - A restore job already pinned to that node.
	//
	// Act:
	//   - Reconcile the FinRestore.
	//
	// Assert:
	//   - Reconcile() returns no error.
	//   - The restore job is gone and no new one is created.
	Context("Restore from a FinBackup whose node no longer exists", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		var finrestore *finv1.FinRestore

		BeforeEach(func(ctx SpecContext) {
			By("creating PVC and PV")
			pvc, pv = NewPVCAndPV(normalSC, userNamespace,
				utils.GetUniqueName("test-pvc-gone"), utils.GetUniqueName("test-pv-gone"), rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating a restorable FinBackup on a node that does not exist")
			finbackup = CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace,
				utils.GetUniqueName("test-fin-backup-gone"), pvc, 1, "gone-node")

			By("creating a FinRestore targeting the FinBackup")
			finrestore = NewFinRestore(workNamespace, utils.GetUniqueName("test-restore-gone"), finbackup.Name,
				utils.GetUniqueName("restore-pvc-gone"), userNamespace)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())

			By("creating a restore job pinned to that node")
			Expect(k8sClient.Create(ctx, &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Namespace: cephNamespace, Name: restoreJobName(finrestore)},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							NodeName:      "gone-node",
							RestartPolicy: corev1.RestartPolicyNever,
							Containers:    []corev1.Container{{Name: "restore", Image: podImage}},
						},
					},
				},
			})).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should delete the restore job and create no new one", func(ctx SpecContext) {
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that the restore job is gone")
			ExpectNoJob(ctx, k8sClient, restoreJobName(finrestore), cephNamespace)

			By("checking that the FinRestore says why it stopped")
			var got finv1.FinRestore
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finrestore), &got)).To(Succeed())
			condition := meta.FindStatusCondition(got.Status.Conditions, finv1.RestoreConditionReadyToUse)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("BackupUnavailable"))
		})

		It("should keep a restore job that had already finished", func(ctx SpecContext) {
			By("completing the restore job")
			var job batchv1.Job
			jobKey := client.ObjectKey{Namespace: cephNamespace, Name: restoreJobName(finrestore)}
			Expect(k8sClient.Get(ctx, jobKey, &job)).Should(Succeed())
			makeJobSucceeded(&job)
			Expect(k8sClient.Status().Update(ctx, &job)).Should(Succeed())

			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that the restore job is still there")
			Expect(k8sClient.Get(ctx, jobKey, &job)).Should(Succeed())
		})
	})

	// CSATEST-1560
	// Description:
	//   Restore with specifying Restore PVC name and namespace.
	//
	// Arrange:
	//   - A backup-target PVC exists.
	//   - FinBackup referencing the PVC exists and is StoredToNode.
	//
	// Act:
	//   - Create FinRestore referencing the FinBackup.
	//       - The FinRestore specifies spec.pvc and spec.pvcNamespace different from status.pvcManifest.
	//
	// Assert:
	//   - Reconcile() does not return an error.
	//   - Restore PVC exists with the spec.pvcName and spec.pvcNamespace of FinRestore.
	Context("Restore with specifying Restore PVC name and namespace", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		var finrestore *finv1.FinRestore

		BeforeEach(func(ctx SpecContext) {
			By("creating PVC and PV")
			pvc, pv = NewPVCAndPV(normalSC, userNamespace, "test-pvc-1560", "test-pv-1560", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating FinBackup targeting the PVC")
			finbackup = CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, "test-fin-backup-1560", pvc, 1, "test-node")

			By("Creating a FinRestore with a PVC of a different name and namespace.")
			finrestore = NewFinRestore(workNamespace, "test-restore-1560", finbackup.Name, "restore-pvc", otherNamespace)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
			DeletePVCAndPV(ctx, finrestore.Spec.PVCNamespace, finrestore.Spec.PVC)
		})

		It("should complete reconciliation and create restore PVC with specified name", func(ctx SpecContext) {
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that restore PVC is created with specified name")
			var restorePVC corev1.PersistentVolumeClaim
			key := client.ObjectKey{Namespace: finrestore.Spec.PVCNamespace, Name: finrestore.Spec.PVC}
			Expect(k8sClient.Get(ctx, key, &restorePVC)).Should(Succeed())
		})
	})

	// CSATEST-1623
	// Description:
	//   Restore without specifying Restore PVC name and namespace
	//
	// Arrange:
	//   - A backup-target PVC exists.
	//   - FinBackup referencing the PVC exists and is StoredToNode.
	//
	// Act:
	//   - Create FinRestore referencing the FinBackup.
	//   - The FinRestore specifies spec.pvc and spec.pvcNamespace different from status.pvcManifest.
	//
	// Assert:
	//   - Reconcile() does not return an error.
	//   - Restore PVC exists with the same name and namespace as FinRestore.
	Context("Restore without specifying FinRestore PVC name and namespace", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		var finrestore *finv1.FinRestore

		BeforeEach(func(ctx SpecContext) {
			By("creating PVC and PV")
			pvc, pv = NewPVCAndPV(normalSC, userNamespace, "test-pvc-1623", "test-pv-1623", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating FinBackup targeting the PVC")
			finbackup = CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, "test-fin-backup-1623", pvc, 1, "test-node")

			By("creating FinRestore without specifying PVC name and namespace")
			finrestore = NewFinRestore(workNamespace, "test-restore-1623", finbackup.Name, "", "")
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
			DeletePVCAndPV(ctx, finrestore.Namespace, finrestore.Name)
		})

		It("should complete reconciliation and create restore PVC with default name", func(ctx SpecContext) {
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that restore PVC is created with default name")
			var restorePVC corev1.PersistentVolumeClaim
			key := client.ObjectKey{Namespace: finrestore.Namespace, Name: finrestore.Name}
			Expect(k8sClient.Get(ctx, key, &restorePVC)).Should(Succeed())
		})
	})

	// CSATEST-1555
	// Description:
	//   Reconcile error caused by missing FinBackup.
	//
	// Arrange:
	//   - None (no FinBackup exists).
	//
	// Act:
	//   - Create FinRestore referring to a non-existent FinBackup.
	//
	// Assert:
	//   - Reconcile() returns no error.
	//   - The FinRestore records ReadyToUse=False with reason BackupUnavailable.
	//   - No restore job is created.
	Context("Reconcile stopped by missing FinBackup", func() {
		var finrestore *finv1.FinRestore

		BeforeEach(func(ctx SpecContext) {
			By("creating FinRestore referring to non-existent FinBackup")
			finrestore = NewFinRestore(workNamespace, "test-restore-1555", "no-exists-fb", "restore-pvc", userNamespace)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
		})

		It("should report the missing FinBackup and stop", func(ctx SpecContext) {
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).NotTo(HaveOccurred())

			By("checking the FinRestore reports the missing FinBackup")
			var got finv1.FinRestore
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finrestore), &got)).To(Succeed())
			condition := meta.FindStatusCondition(got.Status.Conditions, finv1.RestoreConditionReadyToUse)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("BackupUnavailable"))

			By("checking that no restore job is created")
			ExpectNoJob(ctx, k8sClient, restoreJobName(finrestore), cephNamespace)
		})
	})

	// CSATEST-1557
	// Description:
	//   Block restore until the target FinBackup is stored.
	//
	// Arrange:
	//   - A backup-target PVC exists.
	//   - FinBackup referring to the PVC exists and is not StoredToNode.
	//
	// Act:
	//   - Create a FinRestore referring to the FinBackup.
	//
	// Assert:
	//   - Reconcile() returns an error.
	//   - Restore PVC is not created.
	Context("Block restore until the target FinBackup is stored", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		var finrestore *finv1.FinRestore

		BeforeEach(func(ctx SpecContext) {
			By("creating PVC and PV")
			pvc, pv = NewPVCAndPV(normalSC, userNamespace, "test-pvc-1557", "test-pv-1557", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating FinBackup that is not ready")
			finbackup = NewFinBackup(workNamespace, "test-fin-backup-1557", pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())

			By("creating FinRestore targeting the not-ready FinBackup")
			finrestore = NewFinRestore(workNamespace, "test-restore-1557", finbackup.Name, "restore-pvc", pvc.Namespace)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should not return an error during reconciliation", func(ctx SpecContext) {
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that no restore PVC is created")
			var restorePVC corev1.PersistentVolumeClaim
			key := client.ObjectKey{Namespace: finrestore.Spec.PVCNamespace, Name: finrestore.Spec.PVC}
			Expect(k8sClient.Get(ctx, key, &restorePVC)).To(MatchError(k8serrors.IsNotFound, "restore-pvc should not be found"))
		})
	})

	// CSATEST-1558
	// Description:
	//   Prevent restoring when another FinRestore PVC already exists with the same name.
	//
	// Arrange:
	//   - Two PVCs (PVC1, PVC2) exist. PVC2 has restored_by annotation with invalid UUID value.
	//   - FinBackup referring to PVC1 exists and is StoredToNode.
	//
	// Act:
	//   - Create FinRestore referring to the FinBackup.
	//   - The restore PVC name to be created should be the same as PVC2.
	//
	// Assert:
	//   - Reconcile process returns an error.
	Context("Prevent restoring when another FinRestore PVC already exists with the same name", func() {
		var pvc1, pvc2 *corev1.PersistentVolumeClaim
		var pv1, pv2 *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		var finrestore *finv1.FinRestore

		BeforeEach(func(ctx SpecContext) {
			By("creating PVC1 and PV1")
			pvc1, pv1 = NewPVCAndPV(normalSC, userNamespace, "test-pvc-1558-1", "test-pv-1558-1", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc1)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv1)).Should(Succeed())

			By("creating PVC2 and PV2 with restored_by annotation")
			pvc2, pv2 = NewPVCAndPV(normalSC, userNamespace, "test-pvc-1558-2", "test-pv-1558-2", rbdImageName)
			pvc2.Annotations = map[string]string{"fin.cybozu.io/restored-by": "aaaa"}
			Expect(k8sClient.Create(ctx, pvc2)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv2)).Should(Succeed())

			By("creating FinBackup targeting the PVC1")
			finbackup = CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, "test-fin-backup-1558", pvc1, 1, "test-node")

			By("creating FinRestore targeting the FinBackup with conflicting PVC name")
			finrestore = NewFinRestore(workNamespace, "test-restore-1558-1", finbackup.Name, pvc2.Name, pvc2.Namespace)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc1.Namespace, pvc1.Name)
			DeletePVCAndPV(ctx, pvc2.Namespace, pvc2.Name)
		})

		It("should return an error during reconciliation", func(ctx SpecContext) {
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).Should(HaveOccurred())
			Expect(err).Should(MatchError(ContainSubstring("failed to manage restore pvc due to uid mismatch")))
		})
	})

	// CSATEST-1553
	// Description:
	//   Do nothing when FinRestore is ReadyToUse.
	//
	// Arrange:
	//   - A backup-target PVC exists.
	//   - FinBackup referring to the PVC exists and is StoredToNode.
	//
	// Act:
	//   1. Create a FinRestore and make it ReadyToUse and run reconciliation.
	//   2. Create a restore PV referring to the restore PVC and run reconciliation.
	//   3. Make the FinRestore ready by getting the restore job complete and run reconciliation again.
	//   4. Delete the restore job and run reconciliation.
	//
	// Assert:
	//   - Restore PVC exists.
	//   - Restore job PVC exists.
	//   - Restore job PV exists.
	//   - Restore Job is not created.
	Context("Do nothing when FinRestore is ReadyToUse", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		var finrestore *finv1.FinRestore
		var restorePVC corev1.PersistentVolumeClaim
		var restoreJobPVC corev1.PersistentVolumeClaim
		var restoreJobPV corev1.PersistentVolume

		BeforeEach(func(ctx SpecContext) {
			By("creating backup-target PVC and PV")
			pvc, pv = NewPVCAndPV(normalSC, userNamespace, "test-pvc-1553", "test-pv-1553", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating FinBackup and making it StoredToNode")
			finbackup = CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, "test-fin-backup-1553", pvc, 1, "test-node")

			By("creating FinRestore targeting the FinBackup")
			finrestore = NewFinRestore(workNamespace, "test-restore-1553", finbackup.Name, "restore-pvc-1553", userNamespace)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())

			By("running reconciliation once")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("creating restore PV and binding restore PVC (simulate external-provisioner)")
			createAndBindRestorePV(ctx, finrestore)

			By("running reconciliation after restore PVC is Bound")
			_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("making the restore job complete")
			var restoreJob batchv1.Job
			restoreJobKey := client.ObjectKey{Namespace: cephNamespace, Name: restoreJobName(finrestore)}
			Expect(k8sClient.Get(ctx, restoreJobKey, &restoreJob)).Should(Succeed())
			makeJobSucceeded(&restoreJob)
			Expect(k8sClient.Status().Update(ctx, &restoreJob)).ShouldNot(HaveOccurred())

			By("making FinRestore ready")
			_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finrestore), finrestore)).Should(Succeed())
			Expect(finrestore.IsReady()).Should(BeTrue())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
			DeletePVCAndPV(ctx, finrestore.Spec.PVCNamespace, finrestore.Spec.PVC)
			DeletePVCAndPV(ctx, cephNamespace, restoreJobPVCName(finrestore))
		})

		It("should not recreate the restore job and keep existing resources intact", func(ctx SpecContext) {
			By("deleting the restore job to confirm that it will not be recreated by the reconciler")
			var restoreJob batchv1.Job
			restoreJobKey := client.ObjectKey{Namespace: cephNamespace, Name: restoreJobName(finrestore)}
			Expect(k8sClient.Get(ctx, restoreJobKey, &restoreJob)).Should(Succeed())
			options := &client.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationBackground)}
			Expect(k8sClient.Delete(ctx, &restoreJob, options)).Should(Succeed())

			By("running reconciliation after FinRestore is ReadyToUse")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("verifying restore PVC exists")
			restorePVCKey := client.ObjectKey{Namespace: finrestore.Spec.PVCNamespace, Name: finrestore.Spec.PVC}
			Expect(k8sClient.Get(ctx, restorePVCKey, &restorePVC)).Should(Succeed())

			By("verifying both restore job PVC and PV exist")
			jobPVCKey := client.ObjectKey{Namespace: cephNamespace, Name: restoreJobPVCName(finrestore)}
			Expect(k8sClient.Get(ctx, jobPVCKey, &restoreJobPVC)).Should(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: restoreJobPVName(finrestore)}, &restoreJobPV)).Should(Succeed())

			By("verifying that the restore job is not recreated")
			ExpectNoJob(ctx, k8sClient, restoreJobName(finrestore), cephNamespace)
		})
	})

	Context("checksum verification features", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume

		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV for checksum verification cases")
			pvc, pv = NewPVCAndPV(normalSC, userNamespace, utils.GetUniqueName("pvc-csum-"), utils.GetUniqueName("pv-csum-"), rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			By("cleaning up PVC and PV")
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should create restore PVC when allowChecksumMismatched is true", func(ctx SpecContext) {
			// Description:
			//   Ensure that when a FinBackup has ChecksumMismatched=True and FinRestore has allowChecksumMismatched=true,
			//   the restore PVC is created successfully.
			//
			// Arrange:
			//   - Create a FinBackup with ChecksumMismatched=True.
			//   - Create a FinRestore with allowChecksumMismatched=true.
			//
			// Act:
			//   Run reconciliation.
			//
			// Assert:
			//   - Restore PVC is created.

			// Arrange
			By("creating FinBackup with ChecksumMismatched=True")
			finbackup := CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1,
				CreateNode(ctx, k8sClient, utils.GetUniqueName("test-node")))
			meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
				Type:    finv1.BackupConditionChecksumMismatched,
				Status:  metav1.ConditionTrue,
				Reason:  "ChecksumMismatch",
				Message: "Checksum corruption detected",
			})
			Expect(k8sClient.Status().Update(ctx, finbackup)).Should(Succeed())

			By("creating FinRestore with allowChecksumMismatched=true")
			finrestore := NewFinRestore(
				workNamespace,
				utils.GetUniqueName("test-restore"),
				finbackup.Name,
				utils.GetUniqueName("restore-pvc"),
				userNamespace,
			)
			finrestore.Spec.AllowChecksumMismatched = true
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())

			// Act
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			// Assert
			By("checking that restore PVC is created")
			var restorePVC corev1.PersistentVolumeClaim
			key := client.ObjectKey{Namespace: finrestore.Spec.PVCNamespace, Name: finrestore.Spec.PVC}
			Expect(k8sClient.Get(ctx, key, &restorePVC)).Should(Succeed())
		})

		It("should not create restore PVC when allowChecksumMismatched is false", func(ctx SpecContext) {
			// Description:
			//   Ensure that when a FinBackup has ChecksumMismatched=True and FinRestore has allowChecksumMismatched=false,
			//   the restore PVC is not created.
			//
			// Arrange:
			//   - Create a FinBackup with ChecksumMismatched=True.
			//   - Create a FinRestore with allowChecksumMismatched=false.
			//
			// Act:
			//   Run reconciliation.
			//
			// Assert:
			//   - Restore PVC is not created.

			// Arrange
			By("creating FinBackup with ChecksumMismatched=True")
			finbackup := CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1,
				CreateNode(ctx, k8sClient, utils.GetUniqueName("test-node")))
			meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
				Type:    finv1.BackupConditionChecksumMismatched,
				Status:  metav1.ConditionTrue,
				Reason:  "ChecksumMismatch",
				Message: "Checksum corruption detected",
			})
			Expect(k8sClient.Status().Update(ctx, finbackup)).Should(Succeed())

			By("creating FinRestore with allowChecksumMismatched=false")
			finrestore := NewFinRestore(
				workNamespace,
				utils.GetUniqueName("test-restore"),
				finbackup.Name,
				utils.GetUniqueName("restore-pvc"),
				userNamespace,
			)
			finrestore.Spec.AllowChecksumMismatched = false
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())

			// Act
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			// Assert
			By("checking that FinRestore does not become ReadyToUse")
			Consistently(func(g Gomega) {
				var updated finv1.FinRestore
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finrestore), &updated)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(updated.IsReady()).Should(BeFalse())
			}, "3s", "1s").Should(Succeed())
		})
	})

	Context("fin.sqlite3 corruption (MetadataCorrupted) features", func() {
		var pvc *corev1.PersistentVolumeClaim

		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV")
			var pv *corev1.PersistentVolume
			pvc, pv = NewPVCAndPV(normalSC, userNamespace, utils.GetUniqueName("pvc-mc-"), utils.GetUniqueName("pv-mc-"), rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			By("cleaning up PVC and PV")
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should set MetadataCorrupted=True on FinBackup when restore Job exits with code 4", func(ctx SpecContext) {
			// Description:
			//   Ensure that when restore Job detects fin.sqlite3 corruption (exit code 4),
			//   the FinBackup is set to MetadataCorrupted=True.
			//
			// Arrange:
			//   - Create a FinBackup that is StoredToNode and Verified.
			//   - Create a FinRestore targeting the FinBackup.
			//
			// Act:
			//   - Reconcile to create restore PVC.
			//   - Bind restore PVC to PV.
			//   - Reconcile to create restore Job.
			//   - Make restore Job fail with exit code 4.
			//   - Reconcile after job failure.
			//
			// Assert:
			//   - The FinBackup has MetadataCorrupted=True condition.

			// Arrange
			By("creating a FinBackup that is StoredToNode and Verified")
			finbackup := CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1,
				CreateNode(ctx, k8sClient, utils.GetUniqueName("test-node")))

			By("creating a FinRestore targeting the FinBackup")
			finrestore := NewFinRestore(
				workNamespace,
				utils.GetUniqueName("test-restore"),
				finbackup.Name,
				utils.GetUniqueName("restore-pvc"),
				userNamespace,
			)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())

			// Act
			By("reconciling to create restore PVC")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("binding the restore PVC to a PV")
			createAndBindRestorePV(ctx, finrestore)

			By("reconciling to create restore Job")
			_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("making the restore Job fail with exit code 4")
			makeJobFailWithExitCode(ctx, restoreJobName(finrestore), 4)

			By("reconciling after job failure")
			_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			// Assert
			By("checking the FinBackup has MetadataCorrupted=True condition")
			var updatedBackup finv1.FinBackup
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &updatedBackup)).Should(Succeed())
			Expect(updatedBackup.IsMetadataCorrupted()).Should(BeTrue())
		})

		It("should not create restore Job when FinBackup has MetadataCorrupted=True", func(ctx SpecContext) {
			// Description:
			//   Ensure that when a FinBackup has MetadataCorrupted=True,
			//   the reconciler skips creating a restore Job.
			//
			// Arrange:
			//   - Create a FinBackup with MetadataCorrupted=True.
			//   - Create a FinRestore targeting the FinBackup.
			//
			// Act:
			//   - Reconcile the FinRestore.
			//
			// Assert:
			//   - No restore Job is created.

			// Arrange
			By("creating a FinBackup with MetadataCorrupted=True")
			finbackup := CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1,
				CreateNode(ctx, k8sClient, utils.GetUniqueName("test-node")))
			meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
				Type:    finv1.BackupConditionMetadataCorrupted,
				Status:  metav1.ConditionTrue,
				Reason:  "MetadataCorrupted",
				Message: "fin.sqlite3 corruption detected",
			})
			Expect(k8sClient.Status().Update(ctx, finbackup)).Should(Succeed())

			By("creating a FinRestore targeting the FinBackup")
			finrestore := NewFinRestore(
				workNamespace,
				utils.GetUniqueName("test-restore"),
				finbackup.Name,
				utils.GetUniqueName("restore-pvc"),
				userNamespace,
			)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())

			// Act
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			// Assert
			By("checking no restore Job is created")
			ExpectNoJob(ctx, k8sClient, restoreJobName(finrestore), cephNamespace)
		})
	})

	Context("Behavior of allowUnverified field", func() {
		var pvc *corev1.PersistentVolumeClaim

		BeforeEach(func(ctx SpecContext) {
			By("creating PVC and PV")
			var pv *corev1.PersistentVolume
			pvc, pv = NewPVCAndPV(normalSC, userNamespace, utils.GetUniqueName("test-pvc"), utils.GetUniqueName("test-pv"), rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())
		})

		It("should fail to restore unverified backup if allowUnverified is false", func(ctx SpecContext) {
			// Description:
			//   Ensure that restoration is blocked when FinBackup has
			//   Verified=False condition and FinRestore has
			//   allowUnverified=false.
			//
			// Arrange:
			//   - Create a pair of PVC and PV, which is done in BeforeEach.
			//   - Create an unverified FinBackup targeting the PVC.
			//   - Create a FinRestore referencing the FinBackup with allowUnverified=false.
			//
			// Act:
			//   Run reconciliation of the FinRestore.
			//
			// Assert:
			//   - Reconciliation does not return an error.
			//   - Restore PVC is not created.

			// Arrange
			By("creating unverified FinBackup targeting the PVC")
			finbackup := CreateFinBackupStoredAndVerified(
				ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1,
				CreateNode(ctx, k8sClient, utils.GetUniqueName("test-node")))
			finbackup.Status.Conditions = []metav1.Condition{}
			meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
				Type:   finv1.BackupConditionStoredToNode,
				Status: metav1.ConditionTrue,
				Reason: "BackupCompleted",
			})
			meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
				Type:   finv1.BackupConditionVerified,
				Status: metav1.ConditionFalse,
				Reason: "FsckFailed",
			})
			Expect(k8sClient.Status().Update(ctx, finbackup)).Should(Succeed())

			By("creating FinRestore with allowUnverified false")
			finrestore := NewFinRestore(
				workNamespace,
				utils.GetUniqueName("test-restore"),
				finbackup.Name,
				utils.GetUniqueName("restore-pvc"),
				otherNamespace,
			)
			finrestore.Spec.AllowUnverified = false
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())

			// Act
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})

			// Assert
			Expect(err).NotTo(HaveOccurred())

			By("checking that no restore PVC is created")
			var restorePVC corev1.PersistentVolumeClaim
			key := client.ObjectKey{Namespace: finrestore.Spec.PVCNamespace, Name: finrestore.Spec.PVC}
			Expect(k8sClient.Get(ctx, key, &restorePVC)).To(MatchError(k8serrors.IsNotFound, "restore-pvc should not be found"))
		})

		It("should fail to restore not yet verified backup if allowUnverified is false", func(ctx SpecContext) {
			// Description:
			//   Ensure that restoration is blocked when FinBackup has neither
			//   Verified=True nor Verified=False condition and FinRestore has
			//   allowUnverified=false.
			//
			// Arrange:
			//   - Create a pair of PVC and PV, which is done in BeforeEach.
			//   - Create an unverified FinBackup targeting the PVC.
			//   - Create a FinRestore referencing the FinBackup with allowUnverified=false.
			//
			// Act:
			//   Run reconciliation of the FinRestore.
			//
			// Assert:
			//   - Reconciliation does not return an error.
			//   - Restore PVC is not created.

			// Arrange
			By("creating unverified FinBackup targeting the PVC")
			finbackup := CreateFinBackupStoredAndVerified(
				ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1,
				CreateNode(ctx, k8sClient, utils.GetUniqueName("test-node")))
			finbackup.Status.Conditions = []metav1.Condition{}
			meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
				Type:   finv1.BackupConditionStoredToNode,
				Status: metav1.ConditionTrue,
				Reason: "BackupCompleted",
			})
			Expect(k8sClient.Status().Update(ctx, finbackup)).Should(Succeed())

			By("creating FinRestore with allowUnverified false")
			finrestore := NewFinRestore(
				workNamespace,
				utils.GetUniqueName("test-restore"),
				finbackup.Name,
				utils.GetUniqueName("restore-pvc"),
				otherNamespace,
			)
			finrestore.Spec.AllowUnverified = false
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())

			// Act
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})

			// Assert
			Expect(err).NotTo(HaveOccurred())

			By("checking that no restore PVC is created")
			var restorePVC corev1.PersistentVolumeClaim
			key := client.ObjectKey{Namespace: finrestore.Spec.PVCNamespace, Name: finrestore.Spec.PVC}
			Expect(k8sClient.Get(ctx, key, &restorePVC)).To(MatchError(k8serrors.IsNotFound, "restore-pvc should not be found"))
		})

		It("should restore unverified backup if the verification skipped and allowUnverified is true", func(ctx SpecContext) {
			// Description:
			//   Ensure that restoration is allowed when FinBackup has
			//   VerificationSkipped=True condition and FinRestore has
			//   allowUnverified=true.
			//
			// Arrange:
			//   - Create a pair of PVC and PV, which is done in BeforeEach.
			//   - Create a verification-skipped FinBackup targeting the PVC.
			//   - Create a FinRestore referencing the FinBackup with allowUnverified=true.
			//
			// Act:
			//   Run reconciliation of the FinRestore.
			//
			// Assert:
			//   - Reconciliation does not return an error.
			//   - Restore PVC is created.

			// Arrange
			By("creating unverified FinBackup targeting the PVC")
			finbackup := NewFinBackup(
				workNamespace,
				utils.GetUniqueName("test-fin-backup"),
				pvc.Name,
				pvc.Namespace,
				CreateNode(ctx, k8sClient, utils.GetUniqueName("test-node")),
			)
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())
			pvcManifest, err := json.Marshal(pvc)
			Expect(err).ShouldNot(HaveOccurred())
			finbackup.Status.SnapSize = ptr.To(pvc.Spec.Resources.Requests.Storage().Value())
			finbackup.Status.SnapID = ptr.To(1)
			finbackup.Status.PVCManifest = string(pvcManifest)
			meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
				Type:   finv1.BackupConditionStoredToNode,
				Status: metav1.ConditionTrue,
				Reason: "BackupCompleted",
			})
			meta.SetStatusCondition(&finbackup.Status.Conditions, metav1.Condition{
				Type:   finv1.BackupConditionVerificationSkipped,
				Status: metav1.ConditionTrue,
				Reason: "VerificationSkipped",
			})
			Expect(k8sClient.Status().Update(ctx, finbackup)).Should(Succeed())

			By("creating FinRestore with allowUnverified true")
			finrestore := NewFinRestore(
				workNamespace,
				utils.GetUniqueName("test-restore"),
				finbackup.Name,
				utils.GetUniqueName("restore-pvc"),
				otherNamespace,
			)
			finrestore.Spec.AllowUnverified = true
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())

			// Act
			By("reconciling the FinRestore")
			_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})

			// Assert
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that restore PVC is created with specified name")
			var restorePVC corev1.PersistentVolumeClaim
			key := client.ObjectKey{Namespace: finrestore.Spec.PVCNamespace, Name: finrestore.Spec.PVC}
			Expect(k8sClient.Get(ctx, key, &restorePVC)).Should(Succeed())
		})
	})
})

// Description:
//
//	Map a Node event to the FinRestores that still have work to do.
//
// Arrange:
//   - A FinRestore that is not ready.
//   - A FinRestore that is ready.
//   - A FinRestore that is ready and is being deleted.
//
// Act:
//   - Map a Node event.
//
// Assert:
//   - The unfinished one and the one being deleted are enqueued.
//   - The ready one is not.
var _ = Describe("mapping a Node event to FinRestores", func() {
	var reconciler *FinRestoreReconciler
	var unfinished, ready, readyDeleting *finv1.FinRestore

	makeReady := func(ctx SpecContext, restore *finv1.FinRestore) {
		GinkgoHelper()
		meta.SetStatusCondition(&restore.Status.Conditions, metav1.Condition{
			Type:   finv1.RestoreConditionReadyToUse,
			Status: metav1.ConditionTrue,
			Reason: "Test",
		})
		Expect(k8sClient.Status().Update(ctx, restore)).Should(Succeed())
	}

	BeforeEach(func(ctx SpecContext) {
		reconciler = NewFinRestoreReconciler(
			k8sClient, scheme.Scheme, cephNamespace, podImage, ptr.To(resource.MustParse("4096")))

		newRestore := func(prefix string) *finv1.FinRestore {
			restore := NewFinRestore(workNamespace, utils.GetUniqueName(prefix), "backup", "pvc", userNamespace)
			Expect(k8sClient.Create(ctx, restore)).Should(Succeed())
			return restore
		}
		unfinished = newRestore("enqueue-unfinished")
		ready = newRestore("enqueue-ready")
		makeReady(ctx, ready)

		readyDeleting = newRestore("enqueue-ready-deleting")
		controllerutil.AddFinalizer(readyDeleting, FinRestoreFinalizerName)
		Expect(k8sClient.Update(ctx, readyDeleting)).Should(Succeed())
		makeReady(ctx, readyDeleting)
		Expect(k8sClient.Delete(ctx, readyDeleting)).Should(Succeed())
	})

	AfterEach(func(ctx SpecContext) {
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(readyDeleting), readyDeleting)).Should(Succeed())
		controllerutil.RemoveFinalizer(readyDeleting, FinRestoreFinalizerName)
		Expect(k8sClient.Update(ctx, readyDeleting)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, unfinished)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, ready)).Should(Succeed())
	})

	It("should enqueue the FinRestores that still have work to do", func(ctx SpecContext) {
		requests := reconciler.enqueueUnfinishedFinRestores(ctx,
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gone-node"}})

		Expect(requests).To(ContainElement(ctrl.Request{NamespacedName: client.ObjectKeyFromObject(unfinished)}))
		Expect(requests).To(ContainElement(ctrl.Request{NamespacedName: client.ObjectKeyFromObject(readyDeleting)}))
		Expect(requests).NotTo(ContainElement(ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ready)}))
	})
})

func Test_restoreJobCanProceed(t *testing.T) {
	const liveNodeUID = types.UID("uid-live")

	tests := []struct {
		name        string
		noBackup    bool
		backupNode  string
		recordedUID types.UID
		want        bool
	}{
		{
			name:        "the node that holds the backup",
			backupNode:  "node0",
			recordedUID: liveNodeUID,
			want:        true,
		},
		{
			name:       "a backup whose node UID is not recorded yet",
			backupNode: "node0",
			want:       true,
		},
		{
			name:        "a node that no longer exists",
			backupNode:  "gone-node",
			recordedUID: liveNodeUID,
			want:        false,
		},
		{
			name:        "a node recreated under the same name",
			backupNode:  "node0",
			recordedUID: "uid-replaced",
			want:        false,
		},
		{
			name:       "a FinBackup that is missing",
			noBackup:   true,
			backupNode: "node0",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restore := NewFinRestore(workNamespace, "restore", "backup", "pvc", userNamespace)
			objects := []client.Object{
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node0", UID: liveNodeUID}},
			}
			if !tt.noBackup {
				backup := NewFinBackup(workNamespace, "backup", "pvc", userNamespace, tt.backupNode)
				backup.Status.NodeUID = tt.recordedUID
				objects = append(objects, backup)
			}
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).Build()
			r := &FinRestoreReconciler{Client: c, cephClusterNamespace: cephNamespace}

			got, err := r.restoreJobCanProceed(context.Background(), restore)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_reconcileDelete_whenTheJobCannotFinish(t *testing.T) {
	const liveNodeUID = types.UID("uid-live")

	tests := []struct {
		name        string
		backupNode  string
		recordedUID types.UID
	}{
		{
			name:        "a node that no longer exists",
			backupNode:  "gone-node",
			recordedUID: liveNodeUID,
		},
		{
			name:        "a node recreated under the same name while the job was waited on",
			backupNode:  "node0",
			recordedUID: "uid-replaced",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restore := NewFinRestore(workNamespace, "restore", "backup", "pvc", userNamespace)
			restore.UID = "restore-uid"
			restore.Finalizers = []string{FinRestoreFinalizerName}
			restore.DeletionTimestamp = ptr.To(metav1.Now())

			backup := NewFinBackup(workNamespace, "backup", "pvc", userNamespace, tt.backupNode)
			backup.Status.NodeUID = tt.recordedUID
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Namespace: cephNamespace, Name: restoreJobName(restore)},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{NodeName: tt.backupNode}},
				},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
				restore, backup, job,
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node0", UID: liveNodeUID}},
			).Build()
			r := &FinRestoreReconciler{Client: c, cephClusterNamespace: cephNamespace}

			_, err := r.reconcileDelete(context.Background(), restore)
			require.NoError(t, err)

			err = c.Get(context.Background(), client.ObjectKeyFromObject(job), &batchv1.Job{})
			require.True(t, k8serrors.IsNotFound(err), "the unfinished job should not be waited for, got %v", err)
		})
	}
}

func Test_abortRestoreOnGoneBackupNode(t *testing.T) {
	const liveNodeUID = types.UID("uid-live")

	tests := []struct {
		name        string
		backupNode  string
		recordedUID types.UID
		want        bool
	}{
		{
			name:        "the node that holds the backup",
			backupNode:  "node0",
			recordedUID: liveNodeUID,
			want:        false,
		},
		{
			name:        "a node that no longer exists",
			backupNode:  "gone-node",
			recordedUID: liveNodeUID,
			want:        true,
		},
		{
			name:        "a node recreated under the same name",
			backupNode:  "node0",
			recordedUID: "uid-replaced",
			want:        true,
		},
		{
			name:       "a backup whose node UID is not recorded yet",
			backupNode: "node0",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backup := NewFinBackup(workNamespace, "backup", "pvc", userNamespace, tt.backupNode)
			backup.Status.NodeUID = tt.recordedUID
			restore := &finv1.FinRestore{ObjectMeta: metav1.ObjectMeta{
				Namespace: workNamespace, Name: "restore", UID: "restore-uid",
			}}
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).
				WithObjects(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node0", UID: liveNodeUID}}).Build()
			r := &FinRestoreReconciler{Client: c, cephClusterNamespace: cephNamespace}

			got, err := r.abortRestoreOnGoneBackupNode(context.Background(), restore, backup)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_reconcileMissingBackup(t *testing.T) {
	tests := []struct {
		name       string
		jobState   func(*batchv1.Job)
		wantJob    bool
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "a FinRestore that has no job",
			wantJob:    false,
			wantStatus: metav1.ConditionFalse,
			wantReason: "BackupUnavailable",
		},
		{
			name:       "a job that is still running",
			jobState:   func(*batchv1.Job) {},
			wantJob:    false,
			wantStatus: metav1.ConditionFalse,
			wantReason: "BackupUnavailable",
		},
		{
			name: "a job that failed",
			jobState: func(job *batchv1.Job) {
				job.Status.Conditions = []batchv1.JobCondition{
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
				}
			},
			wantJob:    false,
			wantStatus: metav1.ConditionFalse,
			wantReason: "BackupUnavailable",
		},
		{
			name:       "a job that completed before the FinBackup went away",
			jobState:   makeJobSucceeded,
			wantJob:    true,
			wantStatus: metav1.ConditionTrue,
			wantReason: "RestoreCompleted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restore := NewFinRestore(workNamespace, "restore", "backup", "pvc", userNamespace)
			restore.UID = "restore-uid"
			objects := []client.Object{restore}
			jobKey := client.ObjectKey{Namespace: cephNamespace, Name: restoreJobName(restore)}
			if tt.jobState != nil {
				job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: jobKey.Namespace, Name: jobKey.Name}}
				tt.jobState(job)
				objects = append(objects, job)
			}
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).
				WithObjects(objects...).WithStatusSubresource(&finv1.FinRestore{}).Build()
			r := &FinRestoreReconciler{Client: c, cephClusterNamespace: cephNamespace}

			_, err := r.reconcileMissingBackup(context.Background(), restore)
			require.NoError(t, err)

			err = c.Get(context.Background(), jobKey, &batchv1.Job{})
			if tt.wantJob {
				require.NoError(t, err)
			} else {
				require.True(t, k8serrors.IsNotFound(err), "the job should be gone, got %v", err)
			}

			var got finv1.FinRestore
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(restore), &got))
			condition := meta.FindStatusCondition(got.Status.Conditions, finv1.RestoreConditionReadyToUse)
			require.NotNil(t, condition)
			require.Equal(t, tt.wantStatus, condition.Status)
			require.Equal(t, tt.wantReason, condition.Reason)
		})
	}
}

func Test_reconcileMissingBackup_jobCompletesBeforeDelete(t *testing.T) {
	restore := NewFinRestore(workNamespace, "restore", "backup", "pvc", userNamespace)
	jobKey := client.ObjectKey{Namespace: cephNamespace, Name: restoreJobName(restore)}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: jobKey.Namespace, Name: jobKey.Name}}

	completed := false
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(restore, job).
		WithStatusSubresource(&finv1.FinRestore{}, &batchv1.Job{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*batchv1.Job); ok && !completed {
					var current batchv1.Job
					require.NoError(t, c.Get(ctx, jobKey, &current))
					makeJobSucceeded(&current)
					require.NoError(t, c.Status().Update(ctx, &current))
					completed = true
				}
				return c.Delete(ctx, obj, opts...)
			},
		}).Build()
	r := &FinRestoreReconciler{Client: c, cephClusterNamespace: cephNamespace}

	_, err := r.reconcileMissingBackup(context.Background(), restore)
	require.Error(t, err, "the delete should fail on the job that changed after it was read")
	require.NoError(t, c.Get(context.Background(), jobKey, &batchv1.Job{}))

	_, err = r.reconcileMissingBackup(context.Background(), restore)
	require.NoError(t, err)
	require.NoError(t, c.Get(context.Background(), jobKey, &batchv1.Job{}))

	var got finv1.FinRestore
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(restore), &got))
	condition := meta.FindStatusCondition(got.Status.Conditions, finv1.RestoreConditionReadyToUse)
	require.NotNil(t, condition)
	require.Equal(t, "RestoreCompleted", condition.Reason)
}

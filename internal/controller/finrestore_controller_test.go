package controller

import (
	"context"
	"encoding/json"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
	//   - Reconcile() returns an error.
	Context("Reconcile error caused by missing FinBackup", func() {
		var finrestore *finv1.FinRestore

		BeforeEach(func(ctx SpecContext) {
			By("creating FinRestore referring to non-existent FinBackup")
			finrestore = NewFinRestore(workNamespace, "test-restore-1555", "no-exists-fb", "restore-pvc", userNamespace)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
		})

		It("should return an error during reconciliation", func(ctx SpecContext) {
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).To(MatchError(k8serrors.IsNotFound, "no-exists-fb should not be found"))
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
			finbackup := CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1, utils.GetUniqueName("test-node"))
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
			finbackup := CreateFinBackupStoredAndVerified(ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1, utils.GetUniqueName("test-node"))
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
				ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1, utils.GetUniqueName("test-node"))
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
				ctx, k8sClient, workNamespace, utils.GetUniqueName("test-fin-backup"), pvc, 1, utils.GetUniqueName("test-node"))
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
				utils.GetUniqueName("test-node"),
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

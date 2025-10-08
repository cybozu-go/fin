package controller

import (
	"os"

	finv1 "github.com/cybozu-go/fin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("FinRestore Controller", func() {
	// TODO(user): Add unit tests of controller's reconciliation logic.
})

var _ = Describe("FinRestore Controller Reconcile Test", Ordered, func() {
	var reconciler *FinRestoreReconciler
	var sc *storagev1.StorageClass

	BeforeAll(func(ctx SpecContext) {
		sc = NewRBDStorageClass("unit", namespace, rbdPoolName)
		Expect(k8sClient.Create(ctx, sc)).Should(Succeed())
	})

	AfterAll(func(ctx SpecContext) {
		Expect(k8sClient.Delete(ctx, sc)).Should(Succeed())
	})

	BeforeEach(func(ctx SpecContext) {
		expansionUnitSize, ok := os.LookupEnv("FIN_RAW_IMG_EXPANSION_UNIT_SIZE")
		if !ok {
			expansionUnitSize = "4096" // 4KiB
		}
		reconciler = NewFinRestoreReconciler(
			k8sClient,
			scheme.Scheme,
			namespace,
			podImage,
			ptr.To(resource.MustParse(expansionUnitSize)),
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
		var otherStorageClass *storagev1.StorageClass
		var pvc2 *corev1.PersistentVolumeClaim
		var pv2 *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		var finrestore *finv1.FinRestore

		BeforeEach(func(ctx SpecContext) {
			By("creating another storage class for another ceph cluster")
			otherStorageClass = NewRBDStorageClass("other", otherNamespace.Name, rbdPoolName)
			Expect(k8sClient.Create(ctx, otherStorageClass)).Should(Succeed())

			By("creating PVC with the storage class")
			pvc2, pv2 = NewPVCAndPV(otherStorageClass, otherNamespace.Name, "test-pvc-2", "test-pv-2", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc2)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv2)).Should(Succeed())

			By("creating a FinBackup targeting a PVC in a different CephCluster")
			finbackup = NewFinBackup(namespace, "test-fin-backup-1", pvc2.Name, pvc2.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())

			By("creating a FinRestore targeting the FinBackup")
			finrestore = NewFinRestore(namespace, "test-restore-1", finbackup.Name, "restore-pvc", pvc2.Namespace)
			Expect(k8sClient.Create(ctx, finrestore)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finrestore)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc2.Namespace, pvc2.Name)
			Expect(k8sClient.Delete(ctx, otherStorageClass)).Should(Succeed())
		})

		It("should neither return an error nor create a finrestore job during reconciliation", func(ctx SpecContext) {
			By("reconciling the FinRestore")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finrestore)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that no restore job is created")
			ExpectNoJob(ctx, k8sClient, restoreJobName(finrestore), finrestore.Namespace)
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
			pvc, pv = NewPVCAndPV(sc, namespace, "test-pvc-1560", "test-pv-1560", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating FinBackup targeting the PVC")
			finbackup = CreateFinBackupStored(ctx, k8sClient, namespace, "test-fin-backup-1560", pvc, 1, "test-node")

			By("Creating a FinRestore with a PVC of a different name and namespace.")
			finrestore = NewFinRestore(namespace, "test-restore-1560", finbackup.Name, "restore-pvc", otherNamespace.Name)
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
			pvc, pv = NewPVCAndPV(sc, namespace, "test-pvc-1623", "test-pv-1623", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating FinBackup targeting the PVC")
			finbackup = CreateFinBackupStored(ctx, k8sClient, namespace, "test-fin-backup-1623", pvc, 1, "test-node")

			By("creating FinRestore without specifying PVC name and namespace")
			finrestore = NewFinRestore(namespace, "test-restore-1623", finbackup.Name, "", "")
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
			finrestore = NewFinRestore(namespace, "test-restore-1555", "no-exists-fb", "restore-pvc", namespace)
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
			pvc, pv = NewPVCAndPV(sc, namespace, "test-pvc-1557", "test-pv-1557", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating FinBackup that is not ready")
			finbackup = NewFinBackup(namespace, "test-fin-backup-1557", pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())

			By("creating FinRestore targeting the not-ready FinBackup")
			finrestore = NewFinRestore(namespace, "test-restore-1557", finbackup.Name, "restore-pvc", pvc.Namespace)
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
			pvc1, pv1 = NewPVCAndPV(sc, namespace, "test-pvc-1558-1", "test-pv-1558-1", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc1)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv1)).Should(Succeed())

			By("creating PVC2 and PV2 with restored_by annotation")
			pvc2, pv2 = NewPVCAndPV(sc, namespace, "test-pvc-1558-2", "test-pv-1558-2", rbdImageName)
			pvc2.Annotations = map[string]string{"fin.cybozu.io/restored-by": "aaaa"}
			Expect(k8sClient.Create(ctx, pvc2)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv2)).Should(Succeed())

			By("creating FinBackup targeting the PVC1")
			finbackup = CreateFinBackupStored(ctx, k8sClient, namespace, "test-fin-backup-1558", pvc1, 1, "test-node")

			By("creating FinRestore targeting the FinBackup with conflicting PVC name")
			finrestore = NewFinRestore(namespace, "test-restore-1558-1", finbackup.Name, pvc2.Name, pvc2.Namespace)
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
})

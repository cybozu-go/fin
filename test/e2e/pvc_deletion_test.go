package e2e

import (
	"context"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

func pvcDeletionTestSuite() {
	var ns *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var finbackup *finv1.FinBackup
	var writtenData []byte
	var dataSize int64 = 4 * 1024

	BeforeAll(func(ctx SpecContext) {
		By("creating a namespace")
		ns = NewNamespace(utils.GetUniqueName("test-ns-"))
		Expect(CreateNamespace(ctx, k8sClient, ns)).NotTo(HaveOccurred())
		pvc = CreateBackupTargetPVC(ctx, k8sClient, ns, "Block", rookStorageClass, "ReadWriteOnce", "100Mi")
		pod := CreatePodForBlockPVC(ctx, k8sClient, pvc)
		writtenData = WriteRandomDataToPVC(ctx, pod, devicePathInPodForPVC, dataSize)
		Expect(DeletePod(ctx, k8sClient, pod)).NotTo(HaveOccurred())
		finbackup = CreateBackup(ctx, ctrlClient, rookNamespace, pvc, nodes[0])
	})

	AfterAll(func(ctx SpecContext) {
		By("deleting the remaining resources")
		_ = DeleteFinBackup(ctx, ctrlClient, finbackup)
		Expect(WaitForFinBackupDeletion(ctx, ctrlClient, finbackup, 2*time.Minute)).NotTo(HaveOccurred())
		_ = DeletePVC(ctx, k8sClient, pvc)
		Expect(DeleteNamespace(ctx, k8sClient, ns)).NotTo(HaveOccurred())
	})

	// CSATEST-1625
	// Description:
	//	 Restore successes even when the backup target PVC is deleted.
	//
	// Precondition (set by BeforeAll):
	//  - An RBD PVC is created on the Ceph cluster. The first 4KiB
	//    of the PVC is filled with random data.
	//  - FinBackup1 that references the PVC
	//    and has the "StoredToNode" condition set to "True" is created.
	//
	// Arrange:
	//   - Delete the RBD PVC.
	//
	// Act:
	//   - Restore from the backup.
	//
	// Assert:
	//   - Reconcile successes.
	//   - The contents of the restore PVC is correct.
	It("should restore when the backup target PVC is deleted", func(ctx SpecContext) {
		// Arrange
		Expect(DeletePVC(ctx, k8sClient, pvc)).NotTo(HaveOccurred())
		Expect(WaitForPVCDeletion(ctx, k8sClient, pvc, 2*time.Minute)).NotTo(HaveOccurred())

		// Act
		finrestore := CreateRestore(ctx, ctrlClient, finbackup, ns, utils.GetUniqueName("test-finrestore-"))
		DeferCleanup(func() {
			_ = DeleteFinRestoreAndRestorePVC(context.Background(), ctrlClient, k8sClient, finrestore)
		})

		// Assert
		VerifyDataInRestorePVC(ctx, k8sClient, finrestore, writtenData)
	})
}

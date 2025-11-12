package e2e

import (
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

func fullBackupTestSuite() {
	var ns *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var podForBackupTargetPVC *corev1.Pod
	var finbackup, finbackup2 *finv1.FinBackup
	finrestores := make([]*finv1.FinRestore, 5)
	var err error
	var writtenData []byte
	var dataSize int64 = 4 * 1024

	BeforeAll(func(ctx SpecContext) {
		By("creating a namespace")
		ns = NewNamespace(utils.GetUniqueName("test-ns-"))
		err = CreateNamespace(ctx, k8sClient, ns)
		Expect(err).NotTo(HaveOccurred())

		pvc = CreateBackupTargetPVC(ctx, k8sClient, ns, "Block", rookStorageClass, "ReadWriteOnce", "100Mi")
		podForBackupTargetPVC = CreatePodForBlockPVC(ctx, k8sClient, pvc)
		writtenData = WriteRandomDataToPVC(ctx, podForBackupTargetPVC, devicePathInPodForPVC, dataSize)
	})

	// CSATEST-1551
	// Description:
	//   Create a full backup with no error.
	//
	// Precondition:
	//   - An RBD PVC exists.
	//   - The first 4KiB of the PVC is filled with random data.
	//
	// Arrange:
	//   - Nothing.
	// Act:
	//   Create FinBackup, referring the PVC.
	//
	// Assert:
	//   - FinBackup.conditions["StoredToNode"] is true.
	//   - the first 4KiB of the raw.img in the PVC's directory is filled
	//     with the same data as the first 4KiB of the PVC.
	It("should create full backup", func(ctx SpecContext) {
		finbackup = CreateBackup(ctx, ctrlClient, rookNamespace, pvc, nodes[0])

		VerifyRawImage(pvc, nodes[0], writtenData)
	})

	// CSATEST-1559
	// Description:
	//   Restore from full backup with no error.
	//
	// Precondition:
	//   - An RBD PVC exists. The first 4KiB of this PVC is filled with random data.
	//   - A FinBackup corresponding to the RBD PVC exists and is ready to use.
	//
	// Arrange:
	//   - Nothing.
	//
	// Act:
	//   Create FinRestore, referring the FinBackup.
	//
	// Assert:
	//   - FinRestore becomes ready to use.
	//   - The first 4KiB of the restore PVC is filled with the same data
	//     as the first 4KiB of the PVC.
	//   - The size of the restore PVC is the same as that of the backup target PVC.
	It("should restore from full backup", func(ctx SpecContext) {
		// Act
		By("restoring from the backup")
		finrestores[0] = CreateRestore(ctx, ctrlClient,
			finbackup, ns, utils.GetUniqueName("test-finrestore-"))
		Expect(err).NotTo(HaveOccurred())

		// Assert
		VerifyDataInRestorePVC(ctx, k8sClient, finrestores[0], writtenData)
		VerifySizeOfRestorePVC(ctx, ctrlClient, finrestores[0])
	})

	// CSATEST-1552
	// Description:
	//   Delete a FinRestore with no error.
	//
	// Precondition:
	//   - An RBD PVC exists.
	//   - The FinBackup referring the above PVC is ready to use.
	//   - The FinRestore referring the above FinBackup is ready to use.
	//
	// Arrange:
	// 	 - Nothing
	//
	// Act:
	//   Delete FinRestore.
	//
	// Assert:
	//   - FinRestore doesn't exists.
	//   - Restore PVC still exists.
	//   - Restore job PVC doesn't exist.
	//   - Restore job PV doesn't exist.
	It("should delete restore", func(ctx SpecContext) {
		// Action
		By("deleting FinRestore")
		err = DeleteFinRestore(ctx, ctrlClient, finrestores[0])
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinRestoreDeletion(ctx, ctrlClient, finrestores[0], 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		VerifyDataInRestorePVC(ctx, k8sClient, finrestores[0], writtenData)
		VerifyDeletionOfResourcesForRestore(ctx, k8sClient, finrestores[0])
	})

	// CSATEST-1613
	// Description:
	//   Deletion will succeed if FinRestore is deleted before the restore process completes.
	//
	// Precondition:
	//   - A backup-target PVC exists.
	//   - FinBackup referring to the PVC exists.
	//
	// Arrange:
	//   - Nothing
	//
	// Act:
	//   - Create a FinRestore1 referring to the FinBackup.
	//   - Delete the FinRestore1 before the restore process completes.
	//   - Create another FinRestore2 referring to the same FinBackup.
	//
	// Assert:
	//   - FinRestore1 is deleted successfully.
	//   - FinRestore2 is created successfully (status will be verified).
	It("should delete the FinRestore and create another one successfully", func(ctx SpecContext) {
		// Act
		By("creating the first FinRestore targeting the FinBackup")
		finrestores[1] = CreateRestore(ctx, ctrlClient,
			finbackup, ns, utils.GetUniqueName("test-finrestore-"))

		By("deleting the first FinRestore after starting the restore process")
		_, stderr, err := kubectl("wait",
			"--for=jsonpath={.metadata.finalizers[?(@==\"finrestore.fin.cybozu.io/finalizer\")]}",
			"finrestore", "-n", rookNamespace, finrestores[1].Name, "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), string(stderr))
		err = DeleteFinRestore(ctx, ctrlClient, finrestores[1])
		Expect(err).NotTo(HaveOccurred())

		By("creating the second FinRestore targeting the same FinBackup")
		finrestores[2] = CreateRestore(ctx, ctrlClient,
			finbackup, ns, utils.GetUniqueName("test-finrestore-"))

		// Assert
		By("checking that the first FinRestore is deleted successfully")
		err = WaitForFinRestoreDeletion(ctx, ctrlClient, finrestores[1], 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("checking that the second FinRestore is created successfully")
		err = WaitForFinRestoreReady(ctx, ctrlClient, finrestores[2], 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		// Cleanup
		err = DeleteFinRestore(ctx, ctrlClient, finrestores[2])
		Expect(err).NotTo(HaveOccurred())
	})

	// CSATEST-1604
	// Description:
	//   Delete a full backup with no error.
	//
	// Precondition:
	//   - An RBD PVC exists.
	//   - The FinBackup is ready to use.
	//
	// Arrange:
	//   - Nothing
	//
	// Act:
	//   Delete FinBackup, referring the PVC.
	// Assert:
	//   - Deleted the FinBackup resource.
	//   - Deleted the raw.img file.
	//   - Deleted the cleanup and deletion jobs.
	//   - Deleted the snapshot reference in FinBackup.
	It("should delete full backup", func(ctx SpecContext) {
		By("deleting the backup")
		err = DeleteFinBackup(ctx, ctrlClient, finbackup)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinBackupDeletion(ctx, ctrlClient, finbackup, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		VerifyNonExistenceOfRawImage(pvc, nodes[0])
		VerifyDeletionOfJobsForBackup(ctx, k8sClient, finbackup)
		VerifyDeletionOfSnapshotInFinBackup(ctx, ctrlClient, finbackup)
	})

	// CSATEST-1547
	// Description:
	//   Create two full backups on the different nodes with no error.
	//
	// Precondition:
	//   - An RBD PVC exists.
	//
	// Arrange:
	//   - Create a full backup on a node and is verified.
	//
	// Act:
	//   - Create another full backup on a different node.
	//
	// Assert:
	//   - The newly created backup is verified.
	//   - The data in both raw.img files are the same as the data in the PVC.
	It("should create two full backups on the different nodes", func(ctx SpecContext) {
		/// Arrange
		finbackup = CreateBackup(ctx, ctrlClient, rookNamespace, pvc, nodes[0])

		// Act
		finbackup2 = CreateBackup(ctx, ctrlClient, rookNamespace, pvc, nodes[1])

		// Assert
		VerifyRawImage(pvc, nodes[0], writtenData)
		VerifyRawImage(pvc, nodes[1], writtenData)
	})

	// Description:
	//   Restore from two full backups on the different nodes with no error.
	//
	// Precondition:
	//   - An RBD PVC exists.
	//   - Two backups exist referring to the PVC exist.
	//     - These backups are on the different nodes.
	//
	// Arrange:
	//   - Noting.
	//
	// Act:
	//   - Restore from both backups.
	//
	// Assert:
	//   - The first 4KiB of the restore PVC is filled with the same data
	//     as the first 4KiB of the PVC.
	//   - The size of the restore PVC is the same as the snapshot.
	It("should restore from two full backups on the different nodes", func(ctx SpecContext) {
		// Act
		finrestores[3] = CreateRestore(ctx, ctrlClient,
			finbackup, ns, utils.GetUniqueName("test-finrestore-"))
		finrestores[4] = CreateRestore(ctx, ctrlClient,
			finbackup2, ns, utils.GetUniqueName("test-finrestore-"))

		// Assert
		for _, fr := range []*finv1.FinRestore{finrestores[3], finrestores[4]} {
			VerifyDataInRestorePVC(ctx, k8sClient, fr, writtenData)
			VerifySizeOfRestorePVC(ctx, ctrlClient, fr)
		}
	})

	// Description:
	//   Delete two restores created from backups on the different nodes with no error.
	//
	// Precondition:
	//   - An RBD PVC exists.
	//   - Two backups referring to the PVC exist.
	//     - These backups are on the different nodes.
	//   - Two restores referring to these backups exist.
	//
	// Arrange:
	//   - Noting.
	//
	// Act:
	//   - Delete both restores.
	//
	// Assert:
	//   - For both restores:
	//     - FinRestore doesn't exists.
	//     - Restore PVC still exists.
	//     - Restore job PVC doesn't exist.
	//     - Restore job PV doesn't exist.
	It("should delete two full backups on the different nodes", func(ctx SpecContext) {
		// Act
		Expect(DeleteFinRestore(ctx, ctrlClient, finrestores[3])).NotTo(HaveOccurred())
		Expect(DeleteFinRestore(ctx, ctrlClient, finrestores[4])).NotTo(HaveOccurred())

		// Assert
		By("verifying the restore deletion")
		for _, fr := range []*finv1.FinRestore{finrestores[3], finrestores[4]} {
			err = WaitForFinRestoreDeletion(ctx, ctrlClient, fr, 2*time.Minute)
			Expect(err).NotTo(HaveOccurred())
			VerifyDataInRestorePVC(ctx, k8sClient, fr, writtenData)
			VerifyDeletionOfResourcesForRestore(ctx, k8sClient, fr)
		}
	})

	// Description:
	//   Delete two backups on the different nodes with no error.
	//
	// Precondition:
	//   - An RBD PVC exists.
	//   - Two backups referring to the PVC exist.
	//     - These backups are on the different nodes.
	//
	// Arrange:
	//   - Noting.
	//
	// Act:
	//   - Delete both two backups.
	//
	// Assert:
	//   - For both backups:
	//     - Deleted the FinBackup resource.
	//     - Deleted the raw.img file.
	//     - Deleted the cleanup and deletion jobs.
	//     - Deleted the snapshot reference in FinBackup.
	It("should delete two full backups on the different nodes", func(ctx SpecContext) {
		// Act
		Expect(DeleteFinBackup(ctx, ctrlClient, finbackup)).NotTo(HaveOccurred())
		Expect(DeleteFinBackup(ctx, ctrlClient, finbackup2)).NotTo(HaveOccurred())

		// Assert
		By("deleting the backup")
		for _, fb := range []*finv1.FinBackup{finbackup, finbackup2} {
			err = WaitForFinBackupDeletion(ctx, ctrlClient, fb, 2*time.Minute)
			Expect(err).NotTo(HaveOccurred())
			VerifyDeletionOfJobsForBackup(ctx, k8sClient, fb)
			VerifyDeletionOfSnapshotInFinBackup(ctx, ctrlClient, fb)
		}
		VerifyNonExistenceOfRawImage(pvc, nodes[0])
		VerifyNonExistenceOfRawImage(pvc, nodes[1])
	})

	AfterAll(func(ctx SpecContext) {
		By("deleting the restore PVCs")
		for _, fr := range finrestores {
			err = DeleteRestorePVC(ctx, k8sClient, fr)
			Expect(err).NotTo(HaveOccurred())
		}

		By("deleting the pod to write data to the PVC")
		err = DeletePod(ctx, k8sClient, podForBackupTargetPVC)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the PVC")
		err = DeletePVC(ctx, k8sClient, pvc)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the namespace")
		err = DeleteNamespace(ctx, k8sClient, ns)
		Expect(err).NotTo(HaveOccurred())
	})
}

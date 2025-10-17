package e2e

import (
	"fmt"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

func restoreTestSuite() {
	var ns *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var pod *corev1.Pod
	var restorePod *corev1.Pod
	var finbackup *finv1.FinBackup
	finrestores := make([]*finv1.FinRestore, 3)
	var err error
	var finbackupNamespace = pvcNamespace
	var finrestoreNamespace = pvcNamespace

	BeforeAll(func(ctx SpecContext) {
		By("creating a namespace")
		ns = GetNamespace(pvcNamespace)
		err = CreateNamespace(ctx, k8sClient, ns)
		Expect(err).NotTo(HaveOccurred())

		By("creating a PVC")
		pvc, err = GetPVC(pvcNamespace, "test-pvc", "Block", "rook-ceph-block", "ReadWriteOnce", "100Mi")
		Expect(err).NotTo(HaveOccurred())
		err = CreatePVC(ctx, k8sClient, pvc)
		Expect(err).NotTo(HaveOccurred())

		By("creating a pod")
		pod, err = GetPod(pvcNamespace, "test-pod", "test-pvc", "ghcr.io/cybozu/ubuntu:24.04", "/data")
		Expect(err).NotTo(HaveOccurred())

		err = CreatePod(ctx, k8sClient, pod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, pvcNamespace, "test-pod", 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("writing data to the pvc")
		_, stderr, err := kubectl("exec", "-n", pvcNamespace, pod.Name, "--",
			"dd", "if=/dev/urandom", "of=/data", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("exec", "-n", pvcNamespace, pod.Name, "--", "sync")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
	})

	// CSATEST-1559
	// Description:
	//   Restore from full backup with no error.
	//
	// Arrange:
	//   - An RBD PVC exists. The head of this PVC is filled with random data.
	//   - A FinBackup corresponding to the RBD PVC exists and is ready to use.
	//
	// Act:
	//   Create FinRestore, referring the FinBackup.
	//
	// Assert:
	//   - FinRestore becomes ready to use.
	//   - The head of the restore PVC is filled with the same data
	//     as the head of the PVC.
	It("should restore from full backup", func(ctx SpecContext) {
		// Arrange
		By("reading the data from the pvc")
		expectedWrittenData, stderr, err := kubectl("exec", "-n", pvcNamespace, pod.Name, "--",
			"dd", "if=/data", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("creating a backup")
		finbackup, err = GetFinBackup(finbackupNamespace, "finbackup-test", pvcNamespace, pvc.Name, "minikube-worker")
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinBackup(ctx, ctrlClient, finbackup)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinBackupStoredToNodeAndVerified(ctx, ctrlClient, finbackupNamespace, finbackup.Name, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		// Act
		By("restoring from the backup")
		finrestores[0], err = GetFinRestore(
			finrestoreNamespace, "finrestore-test", finbackup.Name, "finrestore-test", rookNamespace)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, finrestores[0])
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinRestoreReady(ctx, ctrlClient, finrestoreNamespace, finrestores[0].Name, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the existence of the restore PVC")
		_, stderr, err = kubectl("wait", "pvc", "-n", rookNamespace, "finrestore-test",
			"--for=jsonpath={.status.phase}=Bound", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("creating a pod to verify the contents in the restore PVC")
		restorePod, err = GetPod(
			finrestores[0].Spec.PVCNamespace,
			"test-restore-pod",
			finrestores[0].Spec.PVC,
			"ghcr.io/cybozu/ubuntu:24.04",
			"/restore",
		)
		Expect(err).NotTo(HaveOccurred())
		err = CreatePod(ctx, k8sClient, restorePod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, rookNamespace, restorePod.Name, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the data in the restore PVC")
		restoredData, stderr, err := kubectl("exec", "-n", rookNamespace, restorePod.Name, "--",
			"dd", "if=/restore", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(restoredData).To(Equal(expectedWrittenData), "Data in restore PVC does not match the expected data")
	})

	// CSATEST-1552
	// Description:
	//   Delete a FinRestore with no error.
	//
	// Arrange:
	//   - An RBD PVC exists.
	//   - The FinBackup referring the above PVC is ready to use.
	//   - The FinRestore referring the above FinBackup is ready to use.
	// Act:
	//   Delete FinRestore.
	// Assert:
	//   - FinRestore doesn't exists.
	//   - Restore PVC exists.
	//   - Restore job PVC doesn't exist.
	//   - Restore job PV doesn't exist.
	It("should delete restore", func(ctx SpecContext) {
		// Action
		By("deleting FinRestore")
		err = DeleteFinRestore(ctx, ctrlClient, finrestoreNamespace, "finrestore-test")
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinRestoreDeletion(ctx, ctrlClient, finrestoreNamespace, "finrestore-test", 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		// Assert
		By("verifying the deletion of the restore job")
		restoreJobName := fmt.Sprintf("fin-restore-%s", finrestores[0].UID)
		err = WaitForJobDeletion(ctx, k8sClient, rookNamespace, restoreJobName, 10*time.Second)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the deletion of the restore job PVC")
		_, stderr, err := kubectl("wait", "pvc", "-n", rookNamespace, restoreJobName, "--for=delete", "--timeout=3m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("verifying the deletion of the restore job PV")
		_, stderr, err = kubectl("wait", "pv", restoreJobName, "--for=delete", "--timeout=3m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
	})

	// CSATEST-1613
	// Description:
	//   Deletion will succeed if delete FinRestore before the restore process completes.
	//
	// Arrange:
	//   - A backup-target PVC exists.
	//   - FinBackup referring to the PVC exists.
	//
	// Act:
	//   - Create a FinRestore1 referring to the FinBackup.
	//   - Delete the FinRestore1 before the restore process completes.
	//   - Create another FinRestore2 referring to the same FinBackup.
	//
	// Assert:
	//   - FinRestore1 is deleted successfully.
	//   - FinRestore2 is created successfully(status will be verified).
	It("should delete the a FinRestore and create another FinRestore successfully", func(ctx SpecContext) {
		finrestore1Name := "finbackup-test-1"
		finrestore2Name := "finbackup-test-2"

		// Arrange
		// Already arranged: A backup-target PVC exists.

		// Act
		By("creating the first FinRestore targeting the FinBackup")
		finrestores[1], err = GetFinRestore(
			finrestoreNamespace, finrestore1Name, finbackup.Name, "finrestore-test-r1", pvcNamespace)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, finrestores[1])
		Expect(err).NotTo(HaveOccurred())

		By("deleting the first FinRestore after starting the restore process")
		_, stderr, err := kubectl("wait",
			"--for=jsonpath={.metadata.finalizers[?(@==\"finrestore.fin.cybozu.io/finalizer\")]}",
			"finrestore", "-n", finrestoreNamespace, finrestore1Name, "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), string(stderr))
		err = DeleteFinRestore(ctx, ctrlClient, finrestoreNamespace, finrestore1Name)
		Expect(err).NotTo(HaveOccurred())

		By("creating the second FinRestore targeting the same FinBackup")
		finrestores[2], err = GetFinRestore(
			finrestoreNamespace, finrestore2Name, finbackup.Name, "finrestore-test-r2", pvcNamespace)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, finrestores[2])
		Expect(err).NotTo(HaveOccurred())

		// Assert
		By("checking that the first FinRestore is deleted successfully")
		err = WaitForFinRestoreDeletion(ctx, ctrlClient, finrestoreNamespace, finrestore1Name, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("checking that the second FinRestore is created successfully")
		err = WaitForFinRestoreReady(ctx, ctrlClient, finrestoreNamespace, finrestore2Name, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		// Cleanup for finrestore[2]
		err = DeleteFinRestore(ctx, ctrlClient, finrestoreNamespace, finrestore2Name)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func(ctx SpecContext) {
		By("deleting the pod to verify the contents in the restore PVC")
		err := DeletePod(ctx, k8sClient, rookNamespace, restorePod.Name)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the restore PVC")
		for _, fr := range finrestores {
			err = DeletePVC(ctx, k8sClient, fr.Spec.PVCNamespace, fr.Spec.PVC)
			Expect(err).NotTo(HaveOccurred())
		}

		By("deleting the backup")
		err = DeleteFinBackup(ctx, ctrlClient, finbackupNamespace, finbackup.Name)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinBackupDeletion(ctx, ctrlClient, finbackupNamespace, finbackup.Name, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the pod to write data to the PVC")
		err = DeletePod(ctx, k8sClient, pvcNamespace, pod.Name)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the PVC")
		err = DeletePVC(ctx, k8sClient, pvcNamespace, pvc.Name)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the namespace")
		err = DeleteNamespace(ctx, k8sClient, pvcNamespace)
		Expect(err).NotTo(HaveOccurred())
	})
}

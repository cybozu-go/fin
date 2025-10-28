package e2e

import (
	"fmt"
	"path/filepath"
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
	var pod *corev1.Pod
	var restorePod *corev1.Pod
	var finbackup *finv1.FinBackup
	finrestores := make([]*finv1.FinRestore, 3)
	var err error

	BeforeAll(func(ctx SpecContext) {
		By("creating a namespace")
		ns = NewNamespace(utils.GetUniqueName("test-ns-"))
		err = CreateNamespace(ctx, k8sClient, ns)
		Expect(err).NotTo(HaveOccurred())

		By("creating a PVC")
		pvc, err = NewPVC(ns.Name, utils.GetUniqueName("test-pvc-"), "Block", rookStorageClass, "ReadWriteOnce", "100Mi")
		Expect(err).NotTo(HaveOccurred())
		err = CreatePVC(ctx, k8sClient, pvc)
		Expect(err).NotTo(HaveOccurred())

		By("creating a pod")
		pod, err = NewPod(ns.Name, utils.GetUniqueName("test-pod-"), pvc.Name, "ghcr.io/cybozu/ubuntu:24.04", "/data")
		Expect(err).NotTo(HaveOccurred())
		err = CreatePod(ctx, k8sClient, pod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, pod, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("writing data to the pvc")
		_, stderr, err := kubectl("exec", "-n", ns.Name, pod.Name, "--",
			"dd", "if=/dev/urandom", "of=/data", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("exec", "-n", ns.Name, pod.Name, "--", "sync")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
	})

	// CSATEST-1551
	// Description:
	//   Create a full backup with no error.
	//
	// Arrange:
	//   - An RBD PVC exists.
	//   - The head of the PVC is filled with random data.
	// Act:
	//   Create FinBackup, referring the PVC.
	//
	// Assert:
	//   - FinBackup.conditions["StoredToNode"] is true.
	//   - the head of the raw.img in the PVC's directory is filled
	//     with the same data as the head of the PVC.
	It("should create full backup", func(ctx SpecContext) {
		By("reading the data from the pvc")
		expectedWrittenData, stderr, err := kubectl("exec", "-n", ns.Name, pod.Name, "--",
			"dd", "if=/data", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("creating a backup")
		finbackup, err = NewFinBackup(rookNamespace, utils.GetUniqueName("test-finbackup-"),
			pvc, "minikube-worker")
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinBackup(ctx, ctrlClient, finbackup)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinBackupStoredToNodeAndVerified(ctx, ctrlClient, finbackup, 1*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the data in raw.img")
		// `--native-ssh=false` is used to avoid issues of conversion from LF to CRLF.
		actualWrittenData, stderr, err := execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"dd", fmt.Sprintf("if=/fin/%s/%s/raw.img", ns.Name, pvc.Name), "bs=1K", "count=1", "status=none")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(actualWrittenData).To(Equal(expectedWrittenData), "Data in raw.img does not match the expected data")
	})

	// CSATEST-1559
	// Description:
	//   Restore from full backup with no error.
	//
	// Precondition:
	//   - An RBD PVC exists. The head of this PVC is filled with random data.
	//   - A FinBackup corresponding to the RBD PVC exists and is ready to use.
	//
	// Arrange:
	//   - Reading the data from the PVC.
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
		expectedWrittenData, stderr, err := kubectl("exec", "-n", ns.Name, pod.Name, "--",
			"dd", "if=/data", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		// Act
		By("restoring from the backup")
		finRestoreName0 := utils.GetUniqueName("test-finrestore-")
		finrestores[0], err = NewFinRestore(
			finRestoreName0, finbackup, ns.Name, finRestoreName0)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, finrestores[0])
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinRestoreReady(ctx, ctrlClient, finrestores[0], 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the existence of the restore PVC")
		_, stderr, err = kubectl("wait", "pvc", "-n", ns.Name, finrestores[0].Name,
			"--for=jsonpath={.status.phase}=Bound", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("creating a pod to verify the contents in the restore PVC")
		restorePod, err = NewPod(
			finrestores[0].Spec.PVCNamespace,
			"test-restore-pod",
			finrestores[0].Spec.PVC,
			"ghcr.io/cybozu/ubuntu:24.04",
			"/restore",
		)
		Expect(err).NotTo(HaveOccurred())
		err = CreatePod(ctx, k8sClient, restorePod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, restorePod, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the data in the restore PVC")
		restoredData, stderr, err := kubectl("exec", "-n", ns.Name, restorePod.Name, "--",
			"dd", "if=/restore", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(restoredData).To(Equal(expectedWrittenData), "Data in restore PVC does not match the expected data")
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
	//   - Restore PVC exists.
	//   - Restore job PVC doesn't exist.
	//   - Restore job PV doesn't exist.
	It("should delete restore", func(ctx SpecContext) {
		// Action
		By("deleting FinRestore")
		err = DeleteFinRestore(ctx, ctrlClient, finrestores[0])
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinRestoreDeletion(ctx, ctrlClient, finrestores[0], 2*time.Minute)
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
		finRestoreName1 := utils.GetUniqueName("test-finrestore-")
		finrestores[1], err = NewFinRestore(
			finRestoreName1, finbackup, ns.Name, finRestoreName1)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, finrestores[1])
		Expect(err).NotTo(HaveOccurred())

		By("deleting the first FinRestore after starting the restore process")
		_, stderr, err := kubectl("wait",
			"--for=jsonpath={.metadata.finalizers[?(@==\"finrestore.fin.cybozu.io/finalizer\")]}",
			"finrestore", "-n", rookNamespace, finRestoreName1, "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), string(stderr))
		err = DeleteFinRestore(ctx, ctrlClient, finrestores[1])
		Expect(err).NotTo(HaveOccurred())

		By("creating the second FinRestore targeting the same FinBackup")
		finRestoreName2 := utils.GetUniqueName("test-finrestore-")
		finrestores[2], err = NewFinRestore(
			finRestoreName2, finbackup, ns.Name, finRestoreName2)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, finrestores[2])
		Expect(err).NotTo(HaveOccurred())

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

		By("verifying the deletion of raw.img")
		rawImgPath := filepath.Join("/fin", ns.Name, pvc.Name, "raw.img")
		stdout, stderr, err := execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--", "test", "!", "-e", rawImgPath)
		Expect(err).NotTo(HaveOccurred(), "raw.img file should be deleted. stdout: %s, stderr: %s", stdout, stderr)

		By("verifying the deletion of jobs")
		err = WaitForJobDeletion(ctx, k8sClient, rookNamespace, fmt.Sprintf("fin-cleanup-%s", finbackup.UID), 10*time.Second)
		Expect(err).NotTo(HaveOccurred(), "Cleanup job should be deleted.")
		err = WaitForJobDeletion(ctx, k8sClient, rookNamespace, fmt.Sprintf("fin-deletion-%s", finbackup.UID), 10*time.Second)
		Expect(err).NotTo(HaveOccurred(), "Deletion job should be deleted.")

		By("verifying the deletion of snapshot reference in FinBackup")
		rbdImage := finbackup.Annotations["fin.cybozu.io/backup-target-rbd-image"]
		stdout, stderr, err = kubectl("exec", "-n", rookNamespace, "deploy/rook-ceph-tools", "--",
			"rbd", "info", fmt.Sprintf("%s/%s@fin-backup-%s", poolName, rbdImage, finbackup.UID))
		Expect(err).To(HaveOccurred(), "Snapshot should be deleted. stdout: %s, stderr: %s", stdout, stderr)
	})

	AfterAll(func(ctx SpecContext) {
		By("deleting the pod to verify the contents in the restore PVC")
		err := DeletePod(ctx, k8sClient, restorePod)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the restore PVCs")
		for _, fr := range finrestores {
			err = DeleteRestorePVC(ctx, k8sClient, fr)
			Expect(err).NotTo(HaveOccurred())
		}

		By("deleting the pod to write data to the PVC")
		err = DeletePod(ctx, k8sClient, pod)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the PVC")
		err = DeletePVC(ctx, k8sClient, pvc)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the namespace")
		err = DeleteNamespace(ctx, k8sClient, ns)
		Expect(err).NotTo(HaveOccurred())
	})
}

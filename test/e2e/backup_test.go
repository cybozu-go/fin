package e2e

import (
	"fmt"
	"path/filepath"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

func backupTestSuite() {
	var pvc *corev1.PersistentVolumeClaim
	var pod *corev1.Pod
	var finbackup *finv1.FinBackup
	var err error

	BeforeAll(func(ctx SpecContext) {
		By("creating a namespace")
		ns := GetNamespace(pvcNamespace)
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
		_, stderr, err := kubectl("exec", "-n", pvcNamespace, "test-pod", "--",
			"dd", "if=/dev/urandom", "of=/data", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("exec", "-n", pvcNamespace, "test-pod", "--", "sync")
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
		expectedWrittenData, stderr, err := kubectl("exec", "-n", pvcNamespace, pod.Name, "--",
			"dd", "if=/data", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("creating a backup")
		finbackup, err = GetFinBackup(rookNamespace, "finbackup-test", pvcNamespace, pvc.Name, "minikube-worker")
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinBackup(ctx, ctrlClient, finbackup)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinBackupStoredToNode(ctx, ctrlClient, rookNamespace, finbackup.GetName(), 1*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the data in raw.img")
		// `--native-ssh=false` is used to avoid issues of conversion from LF to CRLF.
		actualWrittenData, stderr, err := execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"dd", fmt.Sprintf("if=/fin/%s/%s/raw.img", pvcNamespace, pvcName), "bs=1K", "count=1", "status=none")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(actualWrittenData).To(Equal(expectedWrittenData), "Data in raw.img does not match the expected data")
	})

	// CSATEST-1604
	// Description:
	//   Delete a full backup with no error.
	//
	// Arrange:
	//   - An RBD PVC exists.
	//   - The FinBackup is ready to use.
	// Act:
	//   Delete FinBackup, referring the PVC.
	// Assert:
	//   - Deleted the FinBackup resource.
	//   - Deleted the raw.img file.
	//   - Deleted the cleanup and deletion jobs.
	//   - Deleted the snapshot reference in FinBackup.
	It("should delete full backup", func(ctx SpecContext) {
		By("deleting the backup")
		err = DeleteFinBackup(ctx, ctrlClient, rookNamespace, finbackup.Name)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForFinBackupDeletion(ctx, ctrlClient, rookNamespace, finbackup.Name, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the deletion of raw.img")
		rawImgPath := filepath.Join("/fin", pvcNamespace, pvcName, "raw.img")
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
		By("deleting the pod")
		err := DeletePod(ctx, k8sClient, pod.Namespace, pod.Name)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the PVC")
		err = DeletePVC(ctx, k8sClient, pvc.Namespace, pvc.Name)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the namespace")
		err = DeleteNamespace(ctx, k8sClient, pvcNamespace)
		Expect(err).NotTo(HaveOccurred())
	})
}

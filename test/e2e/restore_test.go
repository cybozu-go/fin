package e2e

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func restoreTestSuite() {
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
	It("should restore from full backup", func() {
		// Arrange
		By("creating a namespace")
		_, stderr, err := kubectl("apply", "-f", "testdata/namespace.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("creating a PVC")
		_, stderr, err = kubectl("apply", "-f", "testdata/backup-target-pvc.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("wait", "pvc", "-n", pvcNamespace, "test-pvc",
			"--for=jsonpath={.status.phase}=Bound", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("creating a pod")
		_, stderr, err = kubectl("apply", "-f", "testdata/test-pod.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("wait", "pod", "-n", pvcNamespace, "test-pod",
			"--for=condition=Ready", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("writing data to the pvc")
		_, stderr, err = kubectl("exec", "-n", pvcNamespace, "test-pod", "--",
			"dd", "if=/dev/urandom", "of=/data", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("exec", "-n", pvcNamespace, "test-pod", "--", "sync")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		expectedWrittenData, stderr, err := kubectl("exec", "-n", pvcNamespace, "test-pod", "--",
			"dd", "if=/data", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("creating a backup")
		_, stderr, err = kubectl("apply", "-f", "testdata/finbackup.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("wait", "finbackup", "-n", rookNamespace, "finbackup-test",
			"--for=condition=ReadyToUse", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		// Act
		By("restoring from the backup")
		_, stderr, err = kubectl("apply", "-f", "testdata/finrestore.yaml")

		// Assert
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("wait", "finrestore", "-n", rookNamespace, "finrestore-test",
			"--for=condition=ReadyToUse", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		By("verifying the existence of the restore PVC")
		_, stderr, err = kubectl("wait", "pvc", "-n", rookNamespace, "finrestore-test",
			"--for=jsonpath={.status.phase}=Bound", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		By("verifying the data in the restore PVC")
		actualWrittenData, stderr, err := execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"dd", fmt.Sprintf("if=/fin/%s/%s/raw.img", pvcNamespace, pvcName), "bs=1K", "count=1", "status=none")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(actualWrittenData).To(Equal(expectedWrittenData), "Data in restore PVC does not match the expected data")
	})

	AfterAll(func() {
		By("deleting the backup")
		_, stderr, err := kubectl("delete", "-f", "testdata/finbackup.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		By("deleting the pod")
		_, stderr, err = kubectl("delete", "-f", "testdata/test-pod.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		By("deleting the PVC")
		_, stderr, err = kubectl("delete", "-f", "testdata/backup-target-pvc.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		By("deleting the namespace")
		_, stderr, err = kubectl("delete", "-f", "testdata/namespace.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
	})
}

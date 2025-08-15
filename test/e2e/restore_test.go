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

		By("creating a pod to write data to the PVC")
		_, stderr, err = kubectl("apply", "-f", "testdata/test-pod.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("wait", "pod", "-n", pvcNamespace, "test-pod",
			"--for=condition=Ready", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("writing data to the PVC")
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

		By("creating a pod to verify the contents in the restore PVC")
		_, stderr, err = kubectl("apply", "-f", "testdata/test-restore-pod.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("wait", "pod", "-n", rookNamespace, "test-restore-pod",
			"--for=condition=Ready", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("verifying the data in the restore PVC")
		restoredData, stderr, err := kubectl("exec", "-n", rookNamespace, "test-restore-pod", "--",
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
	It("should delete restore", func() {
		// Arrange
		By("getting FinRestore's UID")
		finrestoreUID, stderr, err := kubectl("get", "finrestore", "-n", rookNamespace,
			"finrestore-test", "-ojsonpath={.metadata.uid}")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		// Action
		By("deleting FinRestore")
		_, stderr, err = kubectl("delete", "-f", "testdata/finrestore.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		// Assert
		_, stderr, err = kubectl("wait", "finrestore", "-n", rookNamespace,
			"finrestore-test", "--for=delete", "--timeout=3m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("verifying the deletion of the restore job")
		_, stderr, err = kubectl("wait", "job", "-n", rookNamespace,
			fmt.Sprintf("fin-restore-%s", finrestoreUID), "--for=delete", "--timeout=3m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("verifying the deletion of the restore job PVC")
		_, stderr, err = kubectl("wait", "pvc", "-n", rookNamespace,
			fmt.Sprintf("fin-restore-%s", finrestoreUID), "--for=delete", "--timeout=3m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("verifying the deletion of the restore job PV")
		_, stderr, err = kubectl("wait", "pvc",
			fmt.Sprintf("fin-restore-%s", finrestoreUID), "--for=delete", "--timeout=3m")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
	})

	AfterAll(func() {
		By("deleting the pod to verify the contents in the restore PVC")
		_, stderr, err := kubectl("delete", "-f", "testdata/test-restore-pod.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		By("deleting the restore PVC")
		_, stderr, err = kubectl("delete", "-n", "rook-ceph", "pvc", "finrestore-test")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		By("deleting the backup")
		_, stderr, err = kubectl("delete", "-f", "testdata/finbackup.yaml")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		By("deleting the pod to write data to the PVC")
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

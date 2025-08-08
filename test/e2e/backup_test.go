package e2e

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func backupTestSuite() {
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
	//   - FinBackup.conditions["ReadyToUse"] is true.
	//   - the head of the raw.img in the PVC's directory is filled
	//     with the same data as the head of the PVC.
	It("should create full backup", func() {
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

		By("verifying the data in raw.img")
		// `--native-ssh=false` is used to avoid issues of conversion from LF to CRLF.
		actualWrittenData, stderr, err := execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"dd", fmt.Sprintf("if=/fin/%s/%s/raw.img", pvcNamespace, pvcName), "bs=1K", "count=1", "status=none")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(actualWrittenData).To(Equal(expectedWrittenData), "Data in raw.img does not match the expected data")
	})
}

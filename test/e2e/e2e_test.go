package e2e

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cybozu-go/fin/test/utils"
)

const (
	rookNamespace = "rook-ceph"
	pvcNamespace  = "test-ns"
	pvcName       = "test-pvc"
	poolName      = "rook-ceph-block-pool"
)

var (
	minikube = "minikube"
)

func init() {
	if m := os.Getenv("MINIKUBE"); m != "" {
		minikube = os.Getenv("MINIKUBE")
	}
}

func execWrapper(cmd string, input []byte, args ...string) ([]byte, []byte, error) {
	var stdout, stderr bytes.Buffer
	command := exec.Command(cmd, args...)
	command.Stdout = &stdout
	command.Stderr = &stderr

	if len(input) != 0 {
		command.Stdin = bytes.NewReader(input)
	}

	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func kubectl(args ...string) ([]byte, []byte, error) {
	return execWrapper("kubectl", nil, args...)
}

var _ = Describe("controller", Ordered, func() {

	Context("Operator", func() {
		It("should run successfully", func() {
			var controllerPodName string

			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func() error {
				// Get the controller pod name
				stdout, stderr, err := kubectl("get", "-n", rookNamespace,
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
				)
				Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
				podNames := utils.GetNonEmptyLines(string(stdout))
				if len(podNames) != 1 {
					return fmt.Errorf("expect 1 controller pods running, but got %d", len(podNames))
				}
				controllerPodName = podNames[0]
				ExpectWithOffset(2, controllerPodName).Should(ContainSubstring("controller-manager"))

				// Validate pod status
				stdout, stderr, err = kubectl("get", "-n", rookNamespace,
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
				)
				Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
				if string(stdout) != "Running" {
					return fmt.Errorf("controller pod in %s status", stdout)
				}
				return nil
			}
			EventuallyWithOffset(1, verifyControllerUp, time.Minute, time.Second).Should(Succeed())

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
	})
})

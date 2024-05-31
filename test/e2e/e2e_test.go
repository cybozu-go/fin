package e2e

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cybozu-go/fin/test/utils"
)

const namespace = "fin-system"

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
				stdout, stderr, err := kubectl("get", "-n", namespace,
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
				stdout, stderr, err = kubectl("get", "-n", namespace,
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
				)
				Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
				if string(stdout) != "Running" {
					return fmt.Errorf("controller pod in %s status", stdout)
				}
				return nil
			}
			EventuallyWithOffset(1, verifyControllerUp, time.Minute, time.Second).Should(Succeed())

			By("creating a PVC")
			_, stderr, err := kubectl("apply", "-f", "testdata/backup-target-pvc.yaml")
			Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

			By("creating a backup")
			_, stderr, err = kubectl("apply", "-f", "testdata/finbackup.yaml")
			Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		})
	})
})

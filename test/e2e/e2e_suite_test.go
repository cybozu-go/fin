package e2e

import (
	"context"
	"os"
	"regexp"
	"testing"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/controller"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var k8sClient kubernetes.Interface
var ctrlClient client.Client
var nodes []string
var minikubeProfile string

// Multi-node minikube has the following nodes.
// - <profile-name>
// - <profile-name>-m[0-9]+
var regexpForExtraNode = regexp.MustCompile(`.*-m[0-9]+$`)

// Run e2e tests using the Ginkgo runner.
func TestE2E(t *testing.T) {
	if os.Getenv("E2ETEST") == "" {
		t.Skip("Run under e2e/")
	}
	RegisterFailHandler(Fail)
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	Expect(err).NotTo(HaveOccurred())

	k8sClient, err = kubernetes.NewForConfig(kubeConfig)
	Expect(err).NotTo(HaveOccurred())

	err = finv1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())
	ctrlClient, err = client.New(kubeConfig, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	Expect(os.Setenv(controller.EnvRawImgExpansionUnitSize, "4096")).NotTo(HaveOccurred())

	nodes, err = GetNodeNames(context.Background(), k8sClient)
	Expect(err).NotTo(HaveOccurred())
	var found bool
	for _, node := range nodes {
		if !regexpForExtraNode.MatchString(node) {
			minikubeProfile = node
			found = true
			break
		}
	}
	Expect(found).To(BeTrue(), "failed to find minikube profile")
})

// Labelling policies for test scenarios(suites):
//   - One label for each scenario.
//   - Run short scenarios together in one GitHub runner
//     by specifying extra "misc" label. It's to avoid
//     rate limiting of GitHub Actions API caused
//     by too many runners.
//
// NOTE: This policy would be revised later when e2e tests are more established.
var _ = Describe("Fin", func() {
	Context("wait environment", waitEnvironment)
	Context("full backup", Label("full-backup"), Ordered, fullBackupTestSuite)
	Context("incremental backup", Label("incremental-backup"), Ordered, incrementalBackupTestSuite)
	Context("lock", Label("lock"), Label("misc"), Ordered, lockTestSuite)
	Context("verification", Label("verification"), Label("misc"), Ordered, verificationTestSuite)
	Context("delete incremental backup", Label("delete-incremental-backup"), Label("misc"), Ordered,
		deleteIncrementalBackupTestSuite)
	Context("pvc", Label("pvc-deletion"), Label("misc"), Ordered, pvcDeletionTestSuite)
	Context("checksum mismatch", Label("checksum"), Label("misc"), Ordered, checksumMismatchTestSuite)
})

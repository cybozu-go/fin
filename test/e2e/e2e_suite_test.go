package e2e

import (
	"os"
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
})

var _ = Describe("Fin", func() {
	Context("wait environment", waitEnvironment)
	Context("full backup", Label("full-backup"), Ordered, fullBackupTestSuite)
	Context("incremental backup", Label("incremental-backup"), Ordered, incrementalBackupTestSuite)
	Context("verification", Label("verification"), Ordered, verificationTestSuite)
})

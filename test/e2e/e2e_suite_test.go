package e2e

import (
	"fmt"
	"os"
	"testing"

	finv1 "github.com/cybozu-go/fin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
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

	var err error
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	if err != nil {
		panic(err)
	}
	k8sClient, err = kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		panic(err)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(finv1.AddToScheme(scheme))
	ctrlClient, err = client.New(kubeConfig, client.Options{Scheme: scheme})
	if err != nil {
		panic(err)
	}

	RegisterFailHandler(Fail)
	_, err = fmt.Fprintf(GinkgoWriter, "Starting fin suite\n")
	if err != nil {
		panic(err)
	}
	RunSpecs(t, "e2e suite")
}

var _ = Describe("Fin", func() {
	Context("wait environment", waitEnvironment)
	Context("backup", Ordered, backupTestSuite)
	Context("incremental backup", Ordered, incrementalBackupTestSuite)
	Context("restore", Ordered, restoreTestSuite)
})

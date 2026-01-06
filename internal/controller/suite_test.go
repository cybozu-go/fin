package controller

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	finv1 "github.com/cybozu-go/fin/api/v1"
	//+kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var cfg *rest.Config
var k8sClient client.Client
var testEnv *envtest.Environment

var normalSC *storagev1.StorageClass
var otherSC *storagev1.StorageClass

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func(ctx SpecContext) {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,

		DownloadBinaryAssets:  true,
		BinaryAssetsDirectory: os.Getenv("ENVTEST_BIN_DIR"),
	}

	// If the environment variable is not set, the field is left empty,
	// which causes envtest to use the default version.
	if os.Getenv("ENVTEST_KUBERNETES_VERSION") != "" {
		testEnv.DownloadBinaryAssetsVersion = "v" + os.Getenv("ENVTEST_KUBERNETES_VERSION")
	}

	var err error
	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = finv1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	//+kubebuilder:scaffold:scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Create namespaces.
	for _, ns := range []string{
		cephNamespace,
		workNamespace,
		userNamespace,
		otherNamespace,
	} {
		Expect(k8sClient.Create(ctx,
			&corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			},
		)).Should(Succeed())
	}

	// Create StorageClass.
	normalSC = NewRBDStorageClass("normal", cephClusterID, rbdPoolName)
	Expect(k8sClient.Create(ctx, normalSC)).Should(Succeed())
	otherSC = NewRBDStorageClass("other", "other-ceph", rbdPoolName)
	Expect(k8sClient.Create(ctx, otherSC)).Should(Succeed())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

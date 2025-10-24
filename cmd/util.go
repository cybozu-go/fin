package cmd

import (
	"fmt"
	"os"
	"strconv"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/controller"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getClientSet() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes clientset: %w", err)
	}
	return clientSet, nil
}

func getControllerClient() (client.Client, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add client-go scheme: %w", err)
	}
	if err := finv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add fin v1 scheme: %w", err)
	}

	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create controller-runtime client: %w", err)
	}
	return k8sClient, nil
}

func getExpansionUnitSize() (uint64, error) {
	expansionUnitSizeStr := os.Getenv(controller.EnvRawImgExpansionUnitSize)
	if expansionUnitSizeStr == "" {
		return 0, fmt.Errorf("%s environment variable is not set", controller.EnvRawImgExpansionUnitSize)
	}
	expansionUnitSize, err := strconv.ParseUint(expansionUnitSizeStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", controller.EnvRawImgExpansionUnitSize, err)
	}
	return expansionUnitSize, nil
}

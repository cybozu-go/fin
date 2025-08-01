package kubernetes

import (
	"context"
	"fmt"

	"github.com/cybozu-go/fin/internal/model"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes"
)

type KubernetesRepository struct {
	clientSet *kubernetes.Clientset
}

var _ model.KubernetesRepository = &KubernetesRepository{}

func NewKubernetesRepository(clientSet *kubernetes.Clientset) *KubernetesRepository {
	return &KubernetesRepository{
		clientSet: clientSet,
	}
}

func (r *KubernetesRepository) GetPVC(name, namespace string) (*corev1.PersistentVolumeClaim, error) {
	ctx := context.Background()
	pvc, err := r.clientSet.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC %s/%s: %w", namespace, name, err)
	}
	return pvc, nil
}

func (r *KubernetesRepository) GetPV(name string) (*corev1.PersistentVolume, error) {
	ctx := context.Background()
	pv, err := r.clientSet.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get PV %s: %w", name, err)
	}
	return pv, nil
}

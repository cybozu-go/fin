package kubernetes

import (
	"context"
	"fmt"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/model"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubernetesRepository struct {
	clientSet *kubernetes.Clientset
	client    client.Client
}

var _ model.KubernetesRepository = &KubernetesRepository{}

func NewKubernetesRepository(clientSet *kubernetes.Clientset, client client.Client) *KubernetesRepository {
	return &KubernetesRepository{
		clientSet: clientSet,
		client:    client,
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

func (r *KubernetesRepository) GetFinBackup(name, namespace string) (*finv1.FinBackup, error) {
	if r.client == nil {
		return nil, fmt.Errorf("runtime client is not available")
	}
	ctx := context.Background()
	var fb finv1.FinBackup
	if err := r.client.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, &fb); err != nil {
		return nil, fmt.Errorf("failed to get FinBackup %s/%s: %w", namespace, name, err)
	}
	return &fb, nil
}

func (r *KubernetesRepository) UpdateFinBackup(fb *finv1.FinBackup) error {
	if r.client == nil {
		return fmt.Errorf("runtime client is not available")
	}

	ctx := context.Background()
	if err := r.client.Update(ctx, fb); err != nil {
		return fmt.Errorf("failed to update FinBackup: %w", err)
	}
	return nil
}

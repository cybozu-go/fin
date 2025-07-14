package kubernetes

import (
	"errors"

	"github.com/cybozu-go/fin/internal/model"

	corev1 "k8s.io/api/core/v1"
)

type KubernetesRepository struct{}

var _ model.KubernetesRepository = &KubernetesRepository{}

func NewKubernetesRepository() *KubernetesRepository {
	return &KubernetesRepository{}
}

func (r *KubernetesRepository) GetPVC(name, namespace string) (*corev1.PersistentVolumeClaim, error) {
	return nil, errors.New("not implemented")
}

func (r *KubernetesRepository) GetPV(name string) (*corev1.PersistentVolume, error) {
	return nil, errors.New("not implemented")
}

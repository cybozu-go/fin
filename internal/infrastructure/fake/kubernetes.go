package fake

import (
	"github.com/cybozu-go/fin/internal/model"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type KubernetesRepository struct {
	pvcMap map[types.NamespacedName]*corev1.PersistentVolumeClaim
	pvMap  map[string]*corev1.PersistentVolume
}

var _ model.KubernetesRepository = &KubernetesRepository{}

func NewKubernetesRepository(
	pvcMap map[types.NamespacedName]*corev1.PersistentVolumeClaim,
	pvMap map[string]*corev1.PersistentVolume,
) *KubernetesRepository {
	return &KubernetesRepository{
		pvcMap: pvcMap,
		pvMap:  pvMap,
	}
}

func (r *KubernetesRepository) GetPVC(name, namespace string) (*corev1.PersistentVolumeClaim, error) {
	if pvc, ok := r.pvcMap[types.NamespacedName{Name: name, Namespace: namespace}]; ok {
		return pvc, nil
	}
	return nil, model.ErrNotFound
}

func (r *KubernetesRepository) GetPV(name string) (*corev1.PersistentVolume, error) {
	if pv, ok := r.pvMap[name]; ok {
		return pv, nil
	}
	return nil, model.ErrNotFound
}

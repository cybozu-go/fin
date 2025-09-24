package fake

import (
	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/model"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type KubernetesRepository struct {
	pvcMap       map[types.NamespacedName]*corev1.PersistentVolumeClaim
	pvMap        map[string]*corev1.PersistentVolume
	finBackupMap map[string]*finv1.FinBackup
}

var _ model.KubernetesRepository = &KubernetesRepository{}

func NewKubernetesRepository(
	pvcMap map[types.NamespacedName]*corev1.PersistentVolumeClaim,
	pvMap map[string]*corev1.PersistentVolume,
) *KubernetesRepository {
	return &KubernetesRepository{
		pvcMap:       pvcMap,
		pvMap:        pvMap,
		finBackupMap: make(map[string]*finv1.FinBackup),
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

func (r *KubernetesRepository) SetPVC(name, namespace string, newPVC *corev1.PersistentVolumeClaim) {
	r.pvcMap[types.NamespacedName{Name: name, Namespace: namespace}] = newPVC
}

func (r *KubernetesRepository) SetPV(name string, newPV *corev1.PersistentVolume) {
	r.pvMap[name] = newPV
}

func (r *KubernetesRepository) DeletePVC(name, namespace string) {
	delete(r.pvcMap, types.NamespacedName{Name: name, Namespace: namespace})
}

func (r *KubernetesRepository) DeletePV(name string) {
	delete(r.pvMap, name)
}

func (r *KubernetesRepository) GetFinBackup(name, namespace string) (*finv1.FinBackup, error) {
	for _, fb := range r.finBackupMap {
		if fb.GetName() == name && fb.GetNamespace() == namespace {
			return fb, nil
		}
	}
	return nil, model.ErrNotFound
}

func (r *KubernetesRepository) UpdateFinBackup(fb *finv1.FinBackup) error {
	r.finBackupMap[string(fb.GetUID())] = fb
	return nil
}

package fake

import (
	"github.com/cybozu-go/fin/internal/model"
	"github.com/cybozu-go/fin/test/utils"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type VolumeInfo struct {
	Namespace string
	PVCName   string
	PVName    string
	PoolName  string
	ImageName string
}

// NewStorage creates a fake Kubernetes and RBD storage repository.
// It creates a PVC, a PV, and a RBD volume with unique names.
// The PVC is associated with a PV and a corresponding RBD volume.
func NewStorage() (*KubernetesRepository, *RBDRepository, *VolumeInfo) {
	k8sRepo := &KubernetesRepository{
		pvcMap: make(map[types.NamespacedName]*corev1.PersistentVolumeClaim, 1),
		pvMap:  make(map[string]*corev1.PersistentVolume, 1),
	}

	volumeInfo := &VolumeInfo{
		Namespace: utils.GetUniqueName("ns-"),
		PVCName:   utils.GetUniqueName("pvc-"),
		PVName:    utils.GetUniqueName("pv-"),
		PoolName:  utils.GetUniqueName("pool-"),
		ImageName: utils.GetUniqueName("image-"),
	}
	pvcUID := types.UID(uuid.New().String())

	// PVC
	k8sRepo.pvcMap[types.NamespacedName{
		Namespace: volumeInfo.Namespace,
		Name:      volumeInfo.PVCName,
	}] = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volumeInfo.PVCName,
			Namespace: volumeInfo.Namespace,
			UID:       pvcUID,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: volumeInfo.PVName,
		},
	}

	// PV
	k8sRepo.pvMap[volumeInfo.PVName] = &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeInfo.PVName,
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeAttributes: map[string]string{
						"imageName": volumeInfo.ImageName,
						"pool":      volumeInfo.PoolName,
					},
				},
			},
		},
	}

	// RBD volume without snapshots
	rbdRepo := &RBDRepository{
		snapshots: make([]*model.RBDSnapshot, 0),
		poolName:  volumeInfo.PoolName,
		imageName: volumeInfo.ImageName,
	}

	return k8sRepo, rbdRepo, volumeInfo
}

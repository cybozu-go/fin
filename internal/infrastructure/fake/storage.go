package fake

import (
	"github.com/cybozu-go/fin/test/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"
)

type VolumeInfo struct {
	Namespace string
	PVCName   string
	PVName    string
	PoolName  string
	ImageName string
}

// NewStorage creates a fake Kubernetes and VolumeInfo for RBD repository.
// It creates a PVC, a PV, and a RBD volume with unique names.
// The PVC is associated with a PV and a corresponding RBD volume.
func NewStorage() (*fake.Clientset, *VolumeInfo) {
	volumeInfo := &VolumeInfo{
		Namespace: utils.GetUniqueName("ns-"),
		PVCName:   utils.GetUniqueName("pvc-"),
		PVName:    utils.GetUniqueName("pv-"),
		PoolName:  utils.GetUniqueName("pool-"),
		ImageName: utils.GetUniqueName("image-"),
	}

	// PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volumeInfo.PVCName,
			Namespace: volumeInfo.Namespace,
			UID:       uuid.NewUUID(),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: volumeInfo.PVName,
		},
	}
	// PV
	pv := &corev1.PersistentVolume{
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
	k8sClient := fake.NewClientset(pvc, pv)

	return k8sClient, volumeInfo
}

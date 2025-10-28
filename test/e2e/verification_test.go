package e2e

import (
	"time"

	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

func verificationTestSuite() {
	var ns *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var expectedWrittenData []byte
	var err error

	BeforeEach(func(ctx SpecContext) {
		ns = NewNamespace(utils.GetUniqueName("test-ns-"))
		By("creating a namespace: " + ns.Name)
		err = CreateNamespace(ctx, k8sClient, ns)
		Expect(err).NotTo(HaveOccurred())

		By("creating a PVC")
		pvc, err = NewPVC(
			ns.Name,
			utils.GetUniqueName("test-pvc-"),
			"Filesystem",
			rookStorageClass,
			"ReadWriteOnce",
			"100Mi",
		)
		Expect(err).NotTo(HaveOccurred())
		err = CreatePVC(ctx, k8sClient, pvc)
		Expect(err).NotTo(HaveOccurred())

		By("creating a pod")
		pod := NewPodMountingFilesystem(ns.Name, utils.GetUniqueName("test-pod-"),
			pvc.Name, "ghcr.io/cybozu/ubuntu:24.04", "/data")
		err = CreatePod(ctx, k8sClient, pod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, pod, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("writing data to the pvc")
		_, _, err = kubectl("exec", "-n", ns.Name, pod.Name, "--",
			"dd", "if=/dev/urandom", "of=/data/test", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred())
		_, _, err = kubectl("exec", "-n", ns.Name, pod.Name, "--", "sync")
		Expect(err).NotTo(HaveOccurred())

		By("reading the data from the pvc")
		expectedWrittenData, _, err = kubectl("exec", "-n", pod.Namespace, pod.Name, "--", "cat", "/data/test")
		Expect(err).NotTo(HaveOccurred())
	})

	// Description:
	//   Ensure that verification and restoration of backup data works correctly
	//   when the backup target PVC is a filesystem.
	//
	// Arrange (1) (in BeforeEach):
	//   - Create a filesystem PVC.
	//   - Create a pod mounting the PVC and write some data to it.
	//
	// Act (1):
	//   Create a FinBackup resource referring the target filesystem PVC.
	//
	// Assert (1):
	//   The FinBackup becomes StoredToNode=True and Verified=True.
	//
	// Arrange (2):
	//   (No action required)
	//
	// Act (2):
	//   Create a FinRestore resource referring the FinBackup.
	//
	// Assert (2):
	//   - The FinRestore becomes ReadyToUse=True.
	//   - A restored PVC is created.
	//   - The data in the restored PVC is identical to the data written in the Arrange (1).
	It("should verify and restore backup data", func(ctx SpecContext) {
		// Arrange (1)
		// nothing to do.

		// Act (1)
		By("creating a backup")
		finbackup, err := NewFinBackup(ns.Name, utils.GetUniqueName("test-finbackup-"),
			pvc, "minikube-worker")
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinBackup(ctx, ctrlClient, finbackup)
		Expect(err).NotTo(HaveOccurred())

		// Assert (1)
		err = WaitForFinBackupStoredToNodeAndVerified(
			ctx, ctrlClient, finbackup, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		// Arrange (2)
		// nothing to do.

		// Act (2)
		By("restoring from the backup")
		finRestoreName := utils.GetUniqueName("test-finrestore-")
		finrestore, err := NewFinRestore(
			finRestoreName, finbackup, pvc.Namespace, finRestoreName)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, finrestore)
		Expect(err).NotTo(HaveOccurred())

		// Assert (2)
		err = WaitForFinRestoreReady(ctx, ctrlClient, finrestore, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the existence of the restore PVC")
		_, _, err = kubectl("wait", "pvc", "-n", finrestore.Spec.PVCNamespace, finrestore.Spec.PVC,
			"--for=jsonpath={.status.phase}=Bound", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())

		By("creating a pod to verify the contents in the restored PVC")
		restorePod := NewPodMountingFilesystem(
			finrestore.Spec.PVCNamespace,
			utils.GetUniqueName("test-restore-pod-"),
			finrestore.Spec.PVC,
			"ghcr.io/cybozu/ubuntu:24.04",
			"/restored",
		)
		err = CreatePod(ctx, k8sClient, restorePod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, restorePod, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the data in the restored PVC")
		restoredData, _, err := kubectl(
			"exec", "-n", restorePod.Namespace, restorePod.Name, "--", "cat", "/restored/test")
		Expect(err).NotTo(HaveOccurred())
		Expect(restoredData).To(Equal(expectedWrittenData))
	})

	// Description:
	//   Ensure that FinBackup sets Verified=False when the backup data is corrupted
	//
	// Arrange:
	//   - In BeforeEach:
	//     - Create a filesystem PVC.
	//     - Create a pod mounting the PVC and write some data to it.
	//   - Corrupt the data in the PVC by accessing its RBD image as a block device through a static PV and PVC.
	//
	// Act:
	//   Create a FinBackup resource referring the target filesystem PVC.
	//
	// Assert:
	//   The FinBackup becomes StoredToNode=True and Verified=False.
	It("should set Verified=False condition to FinBackup when backup is corrupted", func(ctx SpecContext) {
		// Arrange
		By("creating a static PV and PVC to access the RBD image as a block device")
		stdout, _, err := kubectl("get", "pvc", "-n", pvc.Namespace, pvc.Name, "-o", "jsonpath={.spec.volumeName}")
		Expect(err).NotTo(HaveOccurred())
		volumeName := string(stdout)
		stdout, _, err = kubectl("get", "pv", volumeName, "-o", "jsonpath={.spec.csi.volumeAttributes.imageName}")
		Expect(err).NotTo(HaveOccurred())
		imageName := string(stdout)

		staticPVName := utils.GetUniqueName("test-static-pv-")
		staticPVCName := utils.GetUniqueName("test-static-pvc-")
		staticPodName := utils.GetUniqueName("test-static-pod-")

		staticPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: staticPVName,
			},
			Spec: corev1.PersistentVolumeSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("100Mi"),
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				StorageClassName:              "",
				VolumeMode:                    ptr.To(corev1.PersistentVolumeBlock),
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       "rook-ceph.rbd.csi.ceph.com",
						VolumeHandle: imageName,
						FSType:       "ext4",
						VolumeAttributes: map[string]string{
							"clusterID":     "rook-ceph",
							"pool":          poolName,
							"staticVolume":  "true",
							"imageFeatures": "layering",
						},
						NodeStageSecretRef: &corev1.SecretReference{
							Name:      "rook-csi-rbd-node",
							Namespace: rookNamespace,
						},
					},
				},
			},
		}
		err = ctrlClient.Create(ctx, staticPV)
		Expect(err).NotTo(HaveOccurred())

		staticPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      staticPVCName,
				Namespace: rookNamespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Mi"),
					},
				},
				StorageClassName: ptr.To(""),
				VolumeMode:       ptr.To(corev1.PersistentVolumeBlock),
				VolumeName:       staticPVName,
			},
		}
		err = ctrlClient.Create(ctx, staticPVC)
		Expect(err).NotTo(HaveOccurred())

		By("creating a pod to access the static PVC")
		staticPod, err := NewPod(rookNamespace, staticPodName, staticPVCName, "ghcr.io/cybozu/ubuntu:24.04", "/data")
		Expect(err).NotTo(HaveOccurred())
		err = CreatePod(ctx, k8sClient, staticPod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, staticPod, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("corrupting the data in the RBD image")
		_, _, err = kubectl("exec", "-n", rookNamespace, staticPodName, "--",
			"dd", "if=/dev/urandom", "of=/data", "bs=1M", "count=1")
		Expect(err).NotTo(HaveOccurred())
		_, _, err = kubectl("exec", "-n", rookNamespace, staticPodName, "--", "sync")
		Expect(err).NotTo(HaveOccurred())

		By("removing the pod, PV, and PVC to release the RBD image")
		err = DeletePod(ctx, k8sClient, staticPod)
		Expect(err).NotTo(HaveOccurred())
		err = DeletePVC(ctx, k8sClient, staticPVC)
		Expect(err).NotTo(HaveOccurred())
		err = ctrlClient.Delete(ctx, &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: staticPVName,
			},
		})
		Expect(err).NotTo(HaveOccurred())

		// Act
		By("creating a FinBackup resource")
		finbackup, err := NewFinBackup(ns.Name, utils.GetUniqueName("test-finbackup-"),
			pvc, "minikube-worker")
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinBackup(ctx, ctrlClient, finbackup)
		Expect(err).NotTo(HaveOccurred())

		// Assert
		By("waiting for the FinBackup to be stored to node and Verified=False")
		Eventually(func(g Gomega, ctx SpecContext) {
			err := ctrlClient.Get(
				ctx,
				types.NamespacedName{
					Namespace: finbackup.Namespace,
					Name:      finbackup.Name,
				},
				finbackup,
			)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(finbackup.IsStoredToNode()).To(BeTrue())
			g.Expect(finbackup.IsVerifiedFalse()).To(BeTrue())
		}, "60s", "1s").WithContext(ctx).Should(Succeed())
	})

	// Description:
	//   Ensure that FinBackup skips verification when it has skip-verify annotation
	//   and that FinRestore restores the backup when allowUnverified is true.
	//
	// Arrange (1) (in BeforeEach):
	//   - Create a filesystem PVC.
	//   - Create a pod mounting the PVC and write some data to it.
	//
	// Act (1):
	//   Create a FinBackup resource referring the target filesystem PVC with
	//   the skip-verify annotation.
	//
	// Assert (1):
	//   The FinBackup becomes VerificationSkipped=True.
	//
	// Arrange (2):
	//   (No action required)
	//
	// Act (2):
	//   Create a FinRestore resource referring the FinBackup with .spec.allowUnverified=True.
	//
	// Assert (2):
	//   - The FinRestore becomes ReadyToUse=True.
	It("should skip verification when backup has skip-verify annotation and "+
		"should restore it when allowUnverified is true",
		func(ctx SpecContext) {
			// Arrange (1)
			// nothing to do.

			// Act (1)
			By("creating a backup with annotation skip-verify")
			finbackup, err := NewFinBackup(ns.Name, utils.GetUniqueName("test-finbackup-"),
				pvc, "minikube-worker")
			Expect(err).NotTo(HaveOccurred())
			finbackup.Annotations = map[string]string{
				"fin.cybozu.io/skip-verify": "true",
			}
			err = CreateFinBackup(ctx, ctrlClient, finbackup)
			Expect(err).NotTo(HaveOccurred())

			// Assert (1)
			By("waiting for the FinBackup to be stored to node and VerificationSkipped=True")
			Eventually(func(g Gomega, ctx SpecContext) {
				err := ctrlClient.Get(
					ctx,
					types.NamespacedName{Namespace: finbackup.Namespace, Name: finbackup.Name},
					finbackup,
				)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(finbackup.IsStoredToNode()).To(BeTrue())
				g.Expect(finbackup.IsVerificationSkipped()).To(BeTrue())
			}, "60s", "1s").WithContext(ctx).Should(Succeed())

			// Arrange (2)
			// nothing to do.

			// Act (2)
			By("restoring from the backup")
			finRestoreName := utils.GetUniqueName("test-finrestore-")
			finrestore, err := NewFinRestore(
				finRestoreName, finbackup, pvc.Namespace, finRestoreName)
			Expect(err).NotTo(HaveOccurred())
			finrestore.Spec.AllowUnverified = true
			err = CreateFinRestore(ctx, ctrlClient, finrestore)
			Expect(err).NotTo(HaveOccurred())

			// Assert (2)
			err = WaitForFinRestoreReady(ctx, ctrlClient, finrestore, 2*time.Minute)
			Expect(err).NotTo(HaveOccurred())
		},
	)
}

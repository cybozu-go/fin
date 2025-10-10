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
		ns = GetNamespace(utils.GetUniqueName("test-ns-"))
		By("creating a namespace: " + ns.GetName())
		err = CreateNamespace(ctx, k8sClient, ns)
		Expect(err).NotTo(HaveOccurred())

		By("creating a PVC")
		pvc, err = GetPVC(
			ns.GetName(),
			"test-pvc",
			"Filesystem",
			"rook-ceph-block",
			"ReadWriteOnce",
			"100Mi",
		)
		Expect(err).NotTo(HaveOccurred())
		err = CreatePVC(ctx, k8sClient, pvc)
		Expect(err).NotTo(HaveOccurred())

		By("creating a pod")
		pod := GetPodMoutingFilesystem(ns.GetName(), "test-pod", pvc.GetName(), "ghcr.io/cybozu/ubuntu:24.04", "/data")
		err = CreatePod(ctx, k8sClient, pod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, pod.GetNamespace(), pod.GetName(), 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("writing data to the pvc")
		_, _, err = kubectl("exec", "-n", ns.GetName(), pod.GetName(), "--",
			"dd", "if=/dev/urandom", "of=/data/test", "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred())
		_, _, err = kubectl("exec", "-n", ns.GetName(), pod.GetName(), "--", "sync")
		Expect(err).NotTo(HaveOccurred())

		By("reading the data from the pvc")
		expectedWrittenData, _, err = kubectl("exec", "-n", pod.GetNamespace(), pod.GetName(), "--", "cat", "/data/test")
		Expect(err).NotTo(HaveOccurred())
	})

	It("should verify and restore backup data", func(ctx SpecContext) {
		// Arrange (1)
		By("creating a backup")
		finbackup, err := GetFinBackup(ns.GetName(), "finbackup-test", pvc.GetNamespace(), pvc.GetName(), "minikube-worker")
		Expect(err).NotTo(HaveOccurred())

		// Act (1)
		err = CreateFinBackup(ctx, ctrlClient, finbackup)
		Expect(err).NotTo(HaveOccurred())

		// Assert (1)
		err = WaitForFinBackupStoredToNodeAndVerified(
			ctx, ctrlClient, finbackup.GetNamespace(), finbackup.GetName(), 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		// Arrange (2)
		By("restoring from the backup")
		finrestore, err := GetFinRestore(
			ns.GetName(), "finrestore-test", finbackup.GetName(), "finrestore-test", pvc.GetNamespace())
		Expect(err).NotTo(HaveOccurred())

		// Act (2)
		err = CreateFinRestore(ctx, ctrlClient, finrestore)
		Expect(err).NotTo(HaveOccurred())

		// Assert (2)
		err = WaitForFinRestoreReady(ctx, ctrlClient, finrestore.GetNamespace(), finrestore.GetName(), 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the existence of the restore PVC")
		_, _, err = kubectl("wait", "pvc", "-n", finrestore.Spec.PVCNamespace, finrestore.Spec.PVC,
			"--for=jsonpath={.status.phase}=Bound", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())

		By("creating a pod to verify the contents in the restore PVC")
		restorePod := GetPodMoutingFilesystem(
			finrestore.Spec.PVCNamespace,
			"test-restore-pod",
			finrestore.Spec.PVC,
			"ghcr.io/cybozu/ubuntu:24.04",
			"/restore",
		)
		err = CreatePod(ctx, k8sClient, restorePod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, restorePod.GetNamespace(), restorePod.GetName(), 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the data in the restore PVC")
		restoredData, _, err := kubectl(
			"exec", "-n", restorePod.GetNamespace(), restorePod.GetName(), "--", "cat", "/restore/test")
		Expect(err).NotTo(HaveOccurred())
		Expect(restoredData).To(Equal(expectedWrittenData))
	})

	It("should set Verified=False condition to FinBackup when backup is corrupted", func(ctx SpecContext) {
		By("corrupt the PVC data to make the backup verification fail")
		//By("deleting the pod that uses the PVC")
		//err = DeletePod(ctx, k8sClient, pod.GetNamespace(), pod.GetName())
		//Expect(err).NotTo(HaveOccurred())

		By("creating a static PV and PVC to access the RBD image as a block device")
		stdout, _, err := kubectl("get", "pvc", "-n", pvc.GetNamespace(), pvc.GetName(), "-o", "jsonpath={.spec.volumeName}")
		Expect(err).NotTo(HaveOccurred())
		volumeName := string(stdout)
		stdout, _, err = kubectl("get", "pv", volumeName, "-o", "jsonpath={.spec.csi.volumeAttributes.imageName}")
		Expect(err).NotTo(HaveOccurred())
		imageName := string(stdout)

		staticPVName := "test-static-pv"
		staticPVCName := "test-static-pvc"
		staticPodName := "test-static-pod"

		err = ctrlClient.Create(ctx, &corev1.PersistentVolume{
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
							"pool":          "rook-ceph-block-pool",
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
		})
		Expect(err).NotTo(HaveOccurred())

		err = ctrlClient.Create(ctx, &corev1.PersistentVolumeClaim{
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
		})
		Expect(err).NotTo(HaveOccurred())

		By("creating a pod to access the static PVC")
		staticPod, err := GetPod(rookNamespace, staticPodName, staticPVCName, "ghcr.io/cybozu/ubuntu:24.04", "/data")
		Expect(err).NotTo(HaveOccurred())
		err = CreatePod(ctx, k8sClient, staticPod)
		Expect(err).NotTo(HaveOccurred())
		err = WaitForPodReady(ctx, k8sClient, rookNamespace, staticPodName, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("corrupting the data in the RBD image")
		_, _, err = kubectl("exec", "-n", rookNamespace, staticPodName, "--",
			"dd", "if=/dev/urandom", "of=/data", "bs=1M", "count=1")
		Expect(err).NotTo(HaveOccurred())
		_, _, err = kubectl("exec", "-n", rookNamespace, staticPodName, "--", "sync")
		Expect(err).NotTo(HaveOccurred())

		By("removing the pod, PV, and PVC to release the RBD image")
		err = DeletePod(ctx, k8sClient, rookNamespace, staticPodName)
		Expect(err).NotTo(HaveOccurred())
		err = DeletePVC(ctx, k8sClient, rookNamespace, staticPVCName)
		Expect(err).NotTo(HaveOccurred())
		err = ctrlClient.Delete(ctx, &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: staticPVName,
			},
		})
		Expect(err).NotTo(HaveOccurred())

		By("creating a FinBackup resource")
		finbackup, err := GetFinBackup(ns.GetName(), "finbackup-test", pvc.GetNamespace(), pvc.GetName(), "minikube-worker")
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinBackup(ctx, ctrlClient, finbackup)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the FinBackup to be stored to node and Verified=False")
		Eventually(func(g Gomega, ctx SpecContext) {
			err := ctrlClient.Get(
				ctx,
				types.NamespacedName{
					Namespace: finbackup.GetNamespace(),
					Name:      finbackup.GetName(),
				},
				finbackup,
			)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(finbackup.IsStoredToNode()).To(BeTrue())
			g.Expect(finbackup.IsVerifiedFalse()).To(BeTrue())
		}, "60s", "1s").WithContext(ctx).Should(Succeed())
	})

	It("should skip verification when backup has skip-verify annotation and "+
		"should restore it when allowUnverified is true",
		func(ctx SpecContext) {
			// Arrange (1)
			finbackup, err := GetFinBackup(ns.GetName(), "finbackup-test", pvc.GetNamespace(), pvc.GetName(), "minikube-worker")
			Expect(err).NotTo(HaveOccurred())
			finbackup.Annotations = map[string]string{
				"fin.cybozu.io/skip-verify": "true",
			}

			// Act (1)
			By("creating a backup with annotation skip-verify")
			err = CreateFinBackup(ctx, ctrlClient, finbackup)
			Expect(err).NotTo(HaveOccurred())

			// Assert (1)
			By("waiting for the FinBackup to be stored to node and VerificationSkipped=True")
			Eventually(func(g Gomega, ctx SpecContext) {
				err := ctrlClient.Get(
					ctx,
					types.NamespacedName{Namespace: finbackup.GetNamespace(), Name: finbackup.GetName()},
					finbackup,
				)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(finbackup.IsStoredToNode()).To(BeTrue())
				g.Expect(finbackup.IsVerificationSkipped()).To(BeTrue())
			}, "60s", "1s").WithContext(ctx).Should(Succeed())

			// Arrange (2)
			By("restoring from the backup")
			finrestore, err := GetFinRestore(
				ns.GetName(), "finrestore-test", finbackup.GetName(), "finrestore-test", pvc.GetNamespace())
			Expect(err).NotTo(HaveOccurred())
			finrestore.Spec.AllowUnverified = true

			// Act (2)
			err = CreateFinRestore(ctx, ctrlClient, finrestore)
			Expect(err).NotTo(HaveOccurred())

			// Assert (2)
			err = WaitForFinRestoreReady(ctx, ctrlClient, finrestore.GetNamespace(), finrestore.GetName(), 2*time.Minute)
			Expect(err).NotTo(HaveOccurred())
		},
	)
}

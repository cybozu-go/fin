package controller

import (
	"context"
	"slices"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/model"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	namespace    = "default"
	podImage     = "sample-image"
	rbdPoolName  = "test-pool"
	rbdImageName = "test-image"
)

var (
	defaultMaxPartSize = resource.MustParse("100Mi")
)

func init() {
	cleanupJobRequeueAfter = 1 * time.Second
	deletionJobRequeueAfter = 1 * time.Second
}

var _ = Describe("FinBackup Controller", Ordered, func() {
	var reconciler *FinBackupReconciler
	var rbdRepo *fake.RBDRepository
	var stopFunc context.CancelFunc

	BeforeAll(func() {
		rbdRepo = fake.NewRBDRepository(map[fake.PoolImageName][]*model.RBDSnapshot{
			{PoolName: rbdPoolName, ImageName: rbdImageName}: {
				{ID: 1, Name: "init-snap", Size: uint64(defaultMaxPartSize.Value())},
			},
		})
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme.Scheme})
		Expect(err).ToNot(HaveOccurred())
		reconciler = &FinBackupReconciler{
			Client:                  mgr.GetClient(),
			Scheme:                  mgr.GetScheme(),
			cephClusterNamespace:    namespace,
			podImage:                podImage,
			maxPartSize:             &defaultMaxPartSize,
			snapRepo:                rbdRepo,
			rawImgExpansionUnitSize: 100 * 1 << 20,
		}
		err = reconciler.SetupWithManager(mgr)
		Expect(err).ToNot(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		stopFunc = cancel
		go func() {
			defer GinkgoRecover()
			err := mgr.Start(ctx)
			Expect(err).ToNot(HaveOccurred(), "failed to run controller")
		}()
	})

	AfterAll(func() {
		stopFunc()
	})

	var pvc1 *corev1.PersistentVolumeClaim
	var pv1 *corev1.PersistentVolume
	var finbackup1 *finv1.FinBackup
	BeforeEach(func(ctx SpecContext) {
		pvc1, pv1 = NewPVCAndPV(namespace, "test-pvc-1", "test-pv-1")
		Expect(k8sClient.Create(ctx, pvc1)).Should(Succeed())
		Expect(k8sClient.Create(ctx, pv1)).Should(Succeed())
		finbackup1 = NewFinBackup(namespace, "test-backup-1", pvc1.Name, pvc1.Namespace, "test-node")
		Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
		WaitForFinBackupIsReady(ctx, finbackup1)
	})

	AfterEach(func(ctx SpecContext) {
		DeletePVCAndPV(ctx, pvc1.Namespace, pvc1.Name)
		if err := k8sClient.Delete(ctx, finbackup1); err == nil {
			WaitForFinBackupRemoved(ctx, finbackup1)
		}
	})

	// This is the first example test.
	// Description:
	//    Deleting a full backup when both full and incremental backups exist.
	//
	// Arrange:
	//    - Create both full and incremental backups.
	//
	// Act:
	//    - Delete the full backup.
	//
	// Assert:
	//    - The FinBackup for the full backup is deleted.
	//    - The FinBackup for the incremental backup is not deleted.
	//    - The snapshot for the full backup is deleted
	//    - The snapshot for the incremental backup is not deleted
	Describe("deleting a full backup when both full and incremental backups exist", func() {
		var finbackup2 *finv1.FinBackup
		BeforeAll(func(ctx SpecContext) {
			By("creating a incremental FinBackup")
			finbackup2 = NewFinBackup(namespace, "test-backup-2", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())
			WaitForFinBackupIsReady(ctx, finbackup2)

			By("deleting the full FinBackup")
			Expect(k8sClient.Delete(ctx, finbackup1)).Should(Succeed())
		})

		AfterAll(func(ctx SpecContext) {
			if err := k8sClient.Delete(ctx, finbackup2); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup2)
			}
		})

		It("should delete the FinBackup for the full backup", func(ctx SpecContext) {
			WaitForFinBackupRemoved(ctx, finbackup1)
		})
		It("should delete the snapshot for the original FinBackup", func(ctx SpecContext) {
			Eventually(func(g Gomega) {
				snapshots, err := rbdRepo.ListSnapshots(rbdPoolName, rbdImageName)
				g.Expect(err).ShouldNot(HaveOccurred())
				snapshotIndex := slices.IndexFunc(snapshots, func(snapshot *model.RBDSnapshot) bool {
					return snapshot.Name == backupJobName(finbackup1)
				})
				Expect(snapshotIndex).ShouldNot(Equal(-1))
			}, "5s", "1s").Should(Succeed())
		})
		It("should keep the FinBackup for the incremental backup", func(ctx SpecContext) {
			Consistently(func(g Gomega) {
				var fb finv1.FinBackup
				err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: finbackup2.Name}, &fb)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(fb.DeletionTimestamp).Should(BeNil())
			}, "5s", "1s").Should(Succeed())
		})
	})
})

func NewFinBackup(namespace, name, pvc, pvcNS, node string) *finv1.FinBackup {
	return &finv1.FinBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: finv1.FinBackupSpec{
			PVC:          pvc,
			PVCNamespace: pvcNS,
			Node:         node,
		},
	}
}

// NewPVCAndPV creates a PV and a PVC that are bound to each other.
func NewPVCAndPV(namespace, pvcName, pvName string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  pvName,
			VolumeMode:  ptr.To(corev1.PersistentVolumeBlock),
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Mi"),
				},
			},
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "rbd.csi.ceph.com",
					VolumeHandle: pvName,
					VolumeAttributes: map[string]string{
						"imageName": rbdImageName,
						"pool":      rbdPoolName,
					},
				},
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Mi"),
			},
			VolumeMode:  ptr.To(corev1.PersistentVolumeBlock),
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			ClaimRef: &corev1.ObjectReference{
				Namespace: namespace,
				Name:      pvc.Name,
			},
		},
	}
	return pvc, pv
}

func WaitForFinBackupIsReady(ctx context.Context, finbackup *finv1.FinBackup) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		key := types.NamespacedName{Name: backupJobName(finbackup), Namespace: namespace}
		var job batchv1.Job
		g.Expect(k8sClient.Get(ctx, key, &job)).To(Succeed())
		job.Status.Conditions = []batchv1.JobCondition{{
			Type:   batchv1.JobComplete,
			Status: corev1.ConditionTrue,
		}}
		job.Status.Succeeded = 1
		err := k8sClient.Status().Update(ctx, &job)
		g.Expect(err).ShouldNot(HaveOccurred())
	}, "5s", "1s").Should(Succeed())

	Eventually(func(g Gomega) {
		var createdFinBackup finv1.FinBackup
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &createdFinBackup)
		g.Expect(err).ShouldNot(HaveOccurred())
		g.Expect(createdFinBackup.IsReady()).Should(BeTrue(), "FinBackup should be ready")
	}, "5s", "1s").Should(Succeed())
}

func WaitForFinBackupRemoved(ctx context.Context, finbackup *finv1.FinBackup) {
	GinkgoHelper()

	for _, jobName := range []string{cleanupJobName(finbackup), deletionJobName(finbackup)} {
		Eventually(func(g Gomega) {
			key := types.NamespacedName{Name: jobName, Namespace: namespace}
			var job batchv1.Job
			g.Expect(k8sClient.Get(ctx, key, &job)).To(Succeed())
			job.Status.Conditions = []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}}
			job.Status.Succeeded = 1
			err := k8sClient.Status().Update(ctx, &job)
			g.Expect(err).ShouldNot(HaveOccurred())
		}, "5s", "1s").Should(Succeed())
	}
	Eventually(func(g Gomega) {
		var fb finv1.FinBackup
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &fb)
		g.Expect(k8serrors.IsNotFound(err)).Should(BeTrue())
	}, "5s", "1s").Should(Succeed())
}

// DeletePVCAndPV deletes a PVC and its bound PV.
func DeletePVCAndPV(ctx context.Context, namespace, pvcName string) {

	GinkgoHelper()
	var pvc corev1.PersistentVolumeClaim
	err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: pvcName}, &pvc)
	if !k8serrors.IsNotFound(err) {
		controllerutil.RemoveFinalizer(&pvc, "kubernetes.io/pvc-protection")
		Expect(k8sClient.Status().Update(ctx, &pvc)).ShouldNot(HaveOccurred())
		Expect(k8sClient.Delete(ctx, &pvc)).Should(Succeed())
	}

	var pv corev1.PersistentVolume
	err = k8sClient.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv)
	if k8serrors.IsNotFound(err) {
		return
	}
	controllerutil.RemoveFinalizer(&pv, "kubernetes.io/pv-protection")
	Expect(k8sClient.Status().Update(ctx, &pv)).ShouldNot(HaveOccurred())
	Expect(k8sClient.Delete(ctx, &pv)).Should(Succeed())
	Eventually(func(g Gomega) {
		var pv corev1.PersistentVolume
		err = k8sClient.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv)
		g.Expect(k8serrors.IsNotFound(err)).Should(BeTrue())
	}, "5s", "1s").Should(Succeed())
}

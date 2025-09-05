package controller

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
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

var _ = Describe("FinBackup Controller integration test", Ordered, func() {
	var reconciler *FinBackupReconciler
	var rbdRepo *fake.RBDRepository
	var sc1 *storagev1.StorageClass
	var stopFunc context.CancelFunc

	BeforeAll(func() {
		sc1 = NewRBDStorageClass("integration", namespace, rbdPoolName)
		Expect(k8sClient.Create(context.TODO(), sc1)).Should(Succeed())

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
		pvc1, pv1 = NewPVCAndPV(sc1, namespace, "test-pvc-1", "test-pv-1", rbdImageName)
		Expect(k8sClient.Create(ctx, pvc1)).Should(Succeed())
		Expect(k8sClient.Create(ctx, pv1)).Should(Succeed())
		finbackup1 = NewFinBackup(namespace, "test-backup-1", pvc1.Name, pvc1.Namespace, "test-node")
		Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
		WaitForFinBackupIsReady(ctx, finbackup1)
	})

	AfterEach(func(ctx SpecContext) {
		if err := k8sClient.Delete(ctx, finbackup1); err == nil {
			WaitForFinBackupRemoved(ctx, finbackup1)
		}
		DeletePVCAndPV(ctx, pvc1.Namespace, pvc1.Name)
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
		BeforeEach(func(ctx SpecContext) {
			By("creating a incremental FinBackup")
			finbackup2 = NewFinBackup(namespace, "test-backup-2", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())
			WaitForFinBackupIsReady(ctx, finbackup2)

			By("deleting the full FinBackup")
			Expect(k8sClient.Delete(ctx, finbackup1)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			if err := k8sClient.Delete(ctx, finbackup2); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup2)
			}
		})

		It("should delete the FinBackup for the full backup", func(ctx SpecContext) {
			By("waiting for the FinBackup to be removed")
			WaitForFinBackupRemoved(ctx, finbackup1)

			By("waiting for the snapshot to be removed")
			Eventually(func(g Gomega) {
				snapshots, err := rbdRepo.ListSnapshots(rbdPoolName, rbdImageName)
				g.Expect(err).ShouldNot(HaveOccurred())
				snapshotIndex := slices.IndexFunc(snapshots, func(snapshot *model.RBDSnapshot) bool {
					return snapshot.Name == backupJobName(finbackup1)
				})
				Expect(snapshotIndex).Should(Equal(-1))
			}, "5s", "1s").Should(Succeed())
			By("checking another FinBackup is not deleted")
			Consistently(func(g Gomega) {
				var fb finv1.FinBackup
				err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: finbackup2.Name}, &fb)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(fb.DeletionTimestamp).Should(BeNil())
			}, "5s", "1s").Should(Succeed())
		})
	})

	// CSATEST-1542
	// Description:
	//   FinBackup becomes ReadyToUse when a backup-target PVC is recreated.
	//
	// Arrange:
	//   - Create a backup-target PVC.
	//   - Create a FinBackup and make it ReadyToUse.
	//   - Recreate the backup-target PVC.
	//
	// Act:
	//   - Create a new FinBackup.
	//
	// Assert:
	//   - The new FinBackup becomes ReadyToUse.
	Describe("FinBackup becomes ReadyToUse when a backup-target PVC is recreated", func() {
		var finbackup2 *finv1.FinBackup
		BeforeEach(func(ctx SpecContext) {
			By("recreating the backup-target PVC")
			DeletePVCAndPV(ctx, pvc1.Namespace, pvc1.Name)
			pvc1, pv1 = NewPVCAndPV(sc1, namespace, pvc1.Name, pv1.Name, rbdImageName)
			Expect(k8sClient.Create(ctx, pvc1)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv1)).Should(Succeed())

			By("reconciling a new FinBackup")
			finbackup2 = NewFinBackup(namespace, "test-backup-2", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())
		})
		AfterEach(func(ctx SpecContext) {
			if err := k8sClient.Delete(ctx, finbackup2); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup2)
			}
		})

		It("should make the new FinBackup ReadyToUse", func(ctx SpecContext) {
			By("waiting for the FinBackup to be ready")
			WaitForFinBackupIsReady(ctx, finbackup2)
		})
	})

	// CSATEST-1621
	// Description:
	//   Ensure that appropriate labels and annotations are added when reconciling
	//   an incremental FinBackup.
	//
	// Arrange:
	//   - An RBD PVC exists.
	//   - A FinBackup as a full backup exists and is ReadyToUse.
	//
	// Act:
	//   - Create a FinBackup (FB2) as an incremental backup.
	//
	// Assert:
	//   - The FinBackup (FB2) has the correct labels and annotations.
	Describe("creating an incremental backup after a full backup exists", Label("new"), func() {
		var finbackup2 *finv1.FinBackup
		BeforeEach(func(ctx SpecContext) {
			By("creating an incremental FinBackup")
			finbackup2 = NewFinBackup(namespace, "test-incr-backup", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())
			WaitForFinBackupIsReady(ctx, finbackup2)
		})

		AfterEach(func(ctx SpecContext) {
			if err := k8sClient.Delete(ctx, finbackup2); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup2)
			}
		})

		It("should add appropriate labels and annotations for the incremental FinBackup", func(ctx SpecContext) {
			By("verifying labels and annotations on the incremental FinBackup")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup2), finbackup2)).Should(Succeed())
			Expect(finbackup2.GetLabels()).To(HaveKeyWithValue(labelBackupTargetPVCUID, string(pvc1.GetUID())))
			annotations := finbackup2.GetAnnotations()
			Expect(annotations).To(HaveKeyWithValue(annotationBackupTargetRBDImage, rbdImageName))
			Expect(annotations).To(HaveKeyWithValue(annotationRBDPool, rbdPoolName))

			// Incremental backup specific: the diff-from annotation should exist and point to the SnapID of the full backup.
			var fb1 finv1.FinBackup
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup1), &fb1)).Should(Succeed())
			Expect(annotations).To(HaveKeyWithValue(annotationDiffFrom, strconv.Itoa(*fb1.Status.SnapID)))
		})
	})
})

var _ = Describe("FinBackup Controller Reconcile Test", Ordered, func() {
	var reconciler *FinBackupReconciler
	var rbdRepo *fake.RBDRepository
	var sc *storagev1.StorageClass

	BeforeAll(func(ctx SpecContext) {
		sc = NewRBDStorageClass("unit", namespace, rbdPoolName)
		Expect(k8sClient.Create(ctx, sc)).Should(Succeed())
	})

	BeforeEach(func(ctx SpecContext) {
		rbdRepo = fake.NewRBDRepository(map[fake.PoolImageName][]*model.RBDSnapshot{
			{PoolName: rbdPoolName, ImageName: rbdImageName}: {{}},
		})
		reconciler = &FinBackupReconciler{
			Client:                  k8sClient,
			Scheme:                  scheme.Scheme,
			cephClusterNamespace:    namespace,
			podImage:                podImage,
			maxPartSize:             &defaultMaxPartSize,
			snapRepo:                rbdRepo,
			rawImgExpansionUnitSize: 100 * 1 << 20,
		}
	})

	// CSATEST-1620
	// Description:
	//   Confirm that the FinBackup is created with the correct labels and annotations.
	//
	// Arrange:
	//   - Create a pair of PVC and PV.
	//   - Create a FinBackup resource.
	//
	// Act:
	//   - Run FinBackup Controller's Reconcile().
	//
	// Assert:
	//   - Reconcile() does not return an error.
	//   - The FinBackup has the correct labels and annotations.
	Context("when reconciling a full backup", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var finbackup *finv1.FinBackup

		BeforeAll(func(ctx SpecContext) {
			pvc, pv = NewPVCAndPV(sc, namespace, "test-pvc-labels", "test-pv-labels", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			finbackup = NewFinBackup(namespace, "test-full-backup-labels", pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())
		})
		AfterEach(func(ctx SpecContext) {
			controllerutil.RemoveFinalizer(finbackup, "finbackup.fin.cybozu.io/finalizer")
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should add spec-defined labels and annotations for a full backup", func(ctx SpecContext) {
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
			Expect(err).ShouldNot(HaveOccurred())

			var updated finv1.FinBackup
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &updated)).Should(Succeed())
			Expect(updated.GetLabels()).To(HaveKeyWithValue(labelBackupTargetPVCUID, string(pvc.GetUID())))
			Expect(updated.GetAnnotations()).To(HaveKeyWithValue(annotationBackupTargetRBDImage, rbdImageName))
			Expect(updated.GetAnnotations()).To(HaveKeyWithValue(annotationRBDPool, rbdPoolName))
		})
	})

	// CSATEST-1607
	// Description:
	//   Prevent backups from being created for PVCs by wrong Fin instances.
	//
	// Arrange:
	//   - Create another storage class for another ceph cluster.
	//   - Create a PVC with the other StorageClass.
	//
	// Act:
	//   - Create a FinBackup targeting a PVC in a different CephCluster
	//
	// Assert:
	//   - The reconciler does not return any errors.
	//   - The reconciler does not create a backup job.
	Context("Prevent backups from being created for PVCs by wrong Fin instances", func() {
		var pvc2 *corev1.PersistentVolumeClaim
		var pv2 *corev1.PersistentVolume
		var finbackup *finv1.FinBackup

		BeforeEach(func(ctx SpecContext) {
			By("creating another storage class for another ceph cluster")
			otherNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test-namespace-2"}}
			Expect(k8sClient.Create(ctx, otherNamespace)).Should(Succeed())
			otherStorageClass := NewRBDStorageClass("other", otherNamespace.Name, rbdPoolName)
			Expect(k8sClient.Create(ctx, otherStorageClass)).Should(Succeed())

			By("creating PVC in the other storage class")
			pvc2, pv2 = NewPVCAndPV(otherStorageClass, otherNamespace.Name, "test-pvc-2", "test-pv-2", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc2)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv2)).Should(Succeed())

			By("creating a FinBackup targeting a PVC in a different CephCluster")
			finbackup = NewFinBackup(namespace, "test-fin-backup-1", pvc2.Name, pvc2.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc2.Namespace, pvc2.Name)
		})

		It("should not return an error in the reconcile process", func(ctx SpecContext) {
			By("reconciling the FinBackup")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
			Expect(err).ShouldNot(HaveOccurred())

			ExpectNoBackupJob(ctx, k8sClient, finbackup)
		})
	})

	// CSATEST-1629
	// Description:
	//	Prevent backups from being created for non-Ceph PVCs.
	//
	// Arrange:
	//   - Create a StorageClass with a non-Ceph provisioner.
	//   - Create a PVC using that non-Ceph StorageClass.
	//
	// Act:
	//   - Create a FinBackup targeting the non-Ceph PVC.
	//
	// Assert:
	//   - The reconciler does not return any errors.
	//   - The reconciler does not create a backup job.
	Context("Prevent backups from being created for non-Ceph PVCs", func() {
		var pvc2 *corev1.PersistentVolumeClaim
		var pv2 *corev1.PersistentVolume
		var finbackup *finv1.FinBackup

		BeforeEach(func(ctx SpecContext) {
			By("creating another namespace for non-Ceph tests")
			nonCephNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test-namespace-non-ceph"}}
			Expect(k8sClient.Create(ctx, nonCephNamespace)).Should(Succeed())

			By("creating a non-Ceph StorageClass")
			nonCephStorageClass := &storagev1.StorageClass{
				ObjectMeta:  metav1.ObjectMeta{Name: "non-ceph"},
				Provisioner: "non-ceph-provisioner.csi.com",
			}
			Expect(k8sClient.Create(ctx, nonCephStorageClass)).Should(Succeed())

			By("creating a PVC in the non-Ceph StorageClass")
			pvc2, pv2 = NewPVCAndPV(nonCephStorageClass, nonCephNamespace.Name, "test-pvc-2", "test-pv-2", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc2)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv2)).Should(Succeed())

			By("creating a FinBackup targeting the non-Ceph PVC")
			finbackup = NewFinBackup(namespace, "test-fin-backup-1", pvc2.Name, pvc2.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			DeletePVCAndPV(ctx, pvc2.Namespace, pvc2.Name)
		})

		It("should neither return an error nor create a backup job during reconciliation", func(ctx SpecContext) {
			By("reconciling the FinBackup")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
			Expect(err).ShouldNot(HaveOccurred())

			ExpectNoBackupJob(ctx, k8sClient, finbackup)
		})
	})

	// CSATEST-1628
	// Description:
	//  Do not create backup job when a FinBackup with a SnapID is created.
	//
	// Arrange:
	//   - Create a pair of PVC and PV
	//   - Create a FinBackup with a SnapID.
	//
	// Act:
	//   - Run FinBackup Controller's Reconcile().
	//
	// Assert:
	//   - Reconcile() does not return an error.
	//   - No backup job is created.
	Context("when reconciling a deleted FinBackup without SnapID", Ordered, func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		BeforeAll(func(ctx SpecContext) {
			By("creating a pair of PVC and PV")
			pvc, pv = NewPVCAndPV(sc, namespace, "test-pvc-snapid", "test-pv-snapid", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating a FinBackup with a SnapID")
			finbackup = NewFinBackup(namespace, "test-finbackup-snapid", pvc.Name, pvc.Namespace, "test-node")
			finbackup.Status.SnapID = ptr.To(1)
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())
		})
		AfterAll(func(ctx SpecContext) {
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should not create any cleanup or deletion jobs", func(ctx SpecContext) {
			By("reconciling the FinBackup")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that no backup job is created")
			backupJobKey := types.NamespacedName{Name: backupJobName(finbackup), Namespace: namespace}
			var backupJob batchv1.Job
			err = k8sClient.Get(ctx, backupJobKey, &backupJob)
			Expect(err).Should(HaveOccurred())
			Expect(k8serrors.IsNotFound(err)).Should(BeTrue())
		})

		// CSATEST-1626
		// Description:
		//  Do not create any cleanup or deletion jobs when a FinBackup without SnapID is deleted
		//
		// Arrange:
		//   - Create a PVC and PV.
		//   - Create a FinBackup resource and set a deletion timestamp.
		//   - Remove the SnapID from the FinBackup.
		//
		// Act:
		//   - Run FinBackup Controller's Reconcile().
		//
		// Assert:
		//   - Reconcile() does not return an error.
		//   - No cleanup or deletion jobs are created.
		When("the FinBackup is deleted without SnapID", func() {
			BeforeAll(func(ctx SpecContext) {
				// This FinBackup has a SnapID in the BeforeAll of the parent Context.
				// To simulate the condition where a FinBackup without SnapID is deleted,
				// we remove the SnapID here and add a deletion timestamp.
				By("removing SnapID from the FinBackup")
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), finbackup)).Should(Succeed())
				finbackup.Status.SnapID = nil
				Expect(k8sClient.Status().Update(ctx, finbackup)).Should(Succeed())
				By("adding a deletion timestamp")
				Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			})

			It("should not create any cleanup or deletion jobs", func(ctx SpecContext) {
				By("reconciling the FinBackup after removing SnapID and adding the deletion timestamp")
				_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
				Expect(err).ShouldNot(HaveOccurred())

				By("checking that the FinBackup is deleted")
				err = k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), finbackup)
				Expect(err).Should(HaveOccurred())
				Expect(k8serrors.IsNotFound(err)).Should(BeTrue())

				By("checking that no cleanup or deletion jobs are created")
				cleanupJobKey := types.NamespacedName{Name: cleanupJobName(finbackup), Namespace: namespace}
				var cleanupJob batchv1.Job
				err = k8sClient.Get(ctx, cleanupJobKey, &cleanupJob)
				Expect(err).Should(HaveOccurred())
				Expect(k8serrors.IsNotFound(err)).Should(BeTrue())

				deletionJobKey := types.NamespacedName{Name: deletionJobName(finbackup), Namespace: namespace}
				var deletionJob batchv1.Job
				err = k8sClient.Get(ctx, deletionJobKey, &deletionJob)
				Expect(err).Should(HaveOccurred())
				Expect(k8serrors.IsNotFound(err)).Should(BeTrue())
			})
		})
	})

	// CSATEST-1612
	// Description:
	//   Do not create a backup when the PVC UID recorded by FinBackup differs
	//   from the UID of the actual backup-target PVC.
	//
	// Arrange:
	//   - A backup-target PVC.
	//   - A FinBackup corresponding to the backup-target PVC.
	//
	// Act:
	//   1. Change the FinBackup label "fin.cybozu.io/backup-target-pvc-uid"
	//      to a different UID and run Reconcile().
	//   2. Change the PVC UID embedded in FinBackup.status.pvcManifest
	//      and run Reconcile().
	//
	// Assert:
	//   - Reconcile() returns an error.
	//   - No backup job is created.
	Context("Prevent backup when PVC UID differs", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume

		BeforeAll(func(ctx SpecContext) {
			pvc, pv = NewPVCAndPV(sc, namespace, "test-pvc-uid", "test-pv-uid", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			var ppvc corev1.PersistentVolumeClaim
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &ppvc)).Should(Succeed())
		})
		AfterAll(func(ctx SpecContext) {
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		When("The label in FinBackup differs the PVC UID", func() {
			var finbackup *finv1.FinBackup
			BeforeEach(func(ctx SpecContext) {
				finbackup = NewFinBackup(namespace, "test-fin-backup-uid-1", pvc.Name, pvc.Namespace, "test-node")
				Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())
				finbackup.Status.SnapID = ptr.To(1)
				finbackup.Labels = map[string]string{labelBackupTargetPVCUID: "invalid-uid"}
				Expect(k8sClient.Status().Update(ctx, finbackup)).Should(Succeed())
			})
			AfterEach(func(ctx SpecContext) {
				controllerutil.RemoveFinalizer(finbackup, "finbackup.fin.cybozu.io/finalizer")
				Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			})

			It("should neither return an error nor create a backup job during reconciliation", func(ctx SpecContext) {
				By("reconciling the FinBackup")
				_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
				Expect(err).Should(HaveOccurred())
				Expect(err).Should(MatchError(ContainSubstring("backup target PVC UID does not match (inLabel=")))

				ExpectNoBackupJob(ctx, k8sClient, finbackup)
			})
		})

		When("The UID in status.pvcManifest differs from the PVC UID", func() {
			var finbackup *finv1.FinBackup
			BeforeEach(func(ctx SpecContext) {
				finbackup = NewFinBackup(namespace, "test-fin-backup-uid-2", pvc.Name, pvc.Namespace, "test-node")
				finbackup.Labels = map[string]string{labelBackupTargetPVCUID: string(pvc.GetUID())}
				Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())
				finbackup.Status.SnapID = ptr.To(1)
				finbackup.Status.PVCManifest = `{"metadata":{"uid":"invalid-uid"}}`
				Expect(k8sClient.Status().Update(ctx, finbackup)).Should(Succeed())

			})
			AfterEach(func(ctx SpecContext) {
				controllerutil.RemoveFinalizer(finbackup, "finbackup.fin.cybozu.io/finalizer")
				Expect(k8sClient.Delete(ctx, finbackup)).Should(Succeed())
			})

			It("should neither return an error nor create a backup job during reconciliation", func(ctx SpecContext) {
				By("reconciling the FinBackup")
				_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
				Expect(err).Should(HaveOccurred())
				Expect(err).Should(MatchError(ContainSubstring("backup target PVC UID does not match (inStatus=")))

				ExpectNoBackupJob(ctx, k8sClient, finbackup)
			})
		})
	})
})

func NewRBDStorageClass(prefix, cephClusterNamespace, poolName string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: fmt.Sprintf("%s-%s", prefix, cephClusterNamespace)},
		Provisioner: fmt.Sprintf("%s.rbd.csi.ceph.com", cephClusterNamespace),
		Parameters: map[string]string{
			"clusterID": cephClusterNamespace,
			"pool":      poolName,
		},
	}
}

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
func NewPVCAndPV(
	sc *storagev1.StorageClass,
	namespace string,
	pvcName string,
	pvName string,
	imageName string,
) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
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
			StorageClassName: ptr.To(sc.Name),
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       sc.Provisioner,
					VolumeHandle: imageName,
					VolumeAttributes: map[string]string{
						"clusterID": sc.Parameters["clusterID"],
						"pool":      sc.Parameters["pool"],
						"imageName": imageName,
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

	var fb finv1.FinBackup
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &fb); k8serrors.IsNotFound(err) {
		return
	}
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

func ExpectNoBackupJob(ctx context.Context, k8sClient client.Client, finbackup *finv1.FinBackup) {
	GinkgoHelper()
	By("checking if the backup job is not created")
	jobKey := types.NamespacedName{Name: backupJobName(finbackup), Namespace: namespace}
	var job batchv1.Job
	err := k8sClient.Get(ctx, jobKey, &job)
	Expect(err).Should(HaveOccurred())
	Expect(k8serrors.IsNotFound(err)).Should(BeTrue())
}

// CSATEST-1627
// Description:
//
//	Do not proceed reconciliation until all FinBackups
//	for the backup target PVC satisfy the preconditions.
//
// Arrange
//   - The FinBackup resource that is the subject of reconciliation
//   - For each test case, prepare an appropriate FinBackupList
//
// Act
//   - Run snapIDPreconditionSatisfied
//
// Assert
//   - If only self is present in the list, returns true.
//   - If another backup has a nil SnapID, returns false (nil SnapID blocks reconciliation).
//   - For Finbackups with a smaller SnapID:
//     . On a different node, returns true.
//     . On the same node but is being deleted, returns true.
//     . On the same node, not ready and is not being deleted.
//   - If a larger SnapID exists, returns true.
func Test_snapIDPreconditionSatisfied(t *testing.T) {
	logger := logr.Discard()
	deletionTimestamp := metav1.Now()
	createFinBackup := func(
		uid string, snapID int, node string, ready bool, deletionTimestamp *metav1.Time,
	) *finv1.FinBackup {
		fb := &finv1.FinBackup{
			ObjectMeta: metav1.ObjectMeta{
				UID:               types.UID(uid),
				DeletionTimestamp: deletionTimestamp,
			},
			Spec: finv1.FinBackupSpec{
				Node: node,
			},
		}
		if snapID > 0 {
			fb.Status.SnapID = ptr.To(snapID)
		}
		if ready {
			fb.Status.Conditions = []metav1.Condition{
				{
					Type:   finv1.BackupConditionReadyToUse,
					Status: metav1.ConditionTrue,
				},
			}
		}
		return fb
	}

	type args struct {
		backup        *finv1.FinBackup
		finBackupList *finv1.FinBackupList
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "only self in list",
			args: args{
				backup: createFinBackup("backup-uid", 20, "node1", true, nil),
				finBackupList: &finv1.FinBackupList{
					Items: []finv1.FinBackup{
						*createFinBackup("backup-uid", 20, "node1", true, nil),
					},
				},
			},
			want: true,
		},
		{
			name: "another FinBackup with nil SnapID",
			args: args{
				backup: createFinBackup("backup-uid", 20, "node1", true, nil),
				finBackupList: &finv1.FinBackupList{
					Items: []finv1.FinBackup{
						*createFinBackup("backup-uid", 20, "node1", true, nil),
						*createFinBackup("other-uid", 0, "node1", true, nil),
					},
				},
			},
			want: false,
		},
		{
			name: "anothoer FinBackup in different node",
			args: args{
				backup: createFinBackup("backup-uid", 20, "node1", true, nil),
				finBackupList: &finv1.FinBackupList{
					Items: []finv1.FinBackup{
						*createFinBackup("backup-uid", 20, "node1", true, nil),
						*createFinBackup("other-uid", 10, "node2", false, nil),
					},
				},
			},
			want: true,
		},
		{
			name: "another FinBackup is deleted",
			args: args{
				backup: createFinBackup("backup-uid", 20, "node1", true, nil),
				finBackupList: &finv1.FinBackupList{
					Items: []finv1.FinBackup{
						*createFinBackup("backup-uid", 20, "node1", true, nil),
						*createFinBackup("other-uid", 10, "node1", false, &deletionTimestamp),
					},
				},
			},
			want: true,
		},
		{
			name: "anothoer FinBackup is not ready",
			args: args{
				backup: createFinBackup("backup-uid", 20, "node1", true, nil),
				finBackupList: &finv1.FinBackupList{
					Items: []finv1.FinBackup{
						*createFinBackup("backup-uid", 20, "node1", true, nil),
						*createFinBackup("other-uid", 10, "node1", false, nil),
					},
				},
			},
			want: false,
		},
		{
			name: "another FinBackup has a larger SnapID",
			args: args{
				backup: createFinBackup("backup-uid", 20, "node1", true, nil),
				finBackupList: &finv1.FinBackupList{
					Items: []finv1.FinBackup{
						*createFinBackup("backup-uid", 20, "node1", true, nil),
						*createFinBackup("other-uid", 30, "node1", false, nil),
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := snapIDPreconditionSatisfied(&logger, tt.args.backup, tt.args.finBackupList); got != tt.want {
				t.Errorf("snapIDPreconditionSatisfied() = %v, want %v", got, tt.want)
			}
		})
	}
}

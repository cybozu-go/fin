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
	"github.com/cybozu-go/fin/test/utils"
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

var (
	defaultMaxPartSize = resource.MustParse("100Mi")
)

func makeJobFailWithExitCode(ctx SpecContext, jobName string, exitCode int32) {
	GinkgoHelper()
	Eventually(func(g Gomega, ctx SpecContext) {
		var pod corev1.Pod
		pod.SetName(fmt.Sprintf("%s-pod", jobName))
		pod.SetNamespace(namespace)
		_, err := ctrl.CreateOrUpdate(ctx, k8sClient, &pod, func() error {
			pod.Labels = map[string]string{
				"batch.kubernetes.io/job-name": jobName,
			}
			pod.Spec = corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "container-name",
						Image: "container-image",
					},
				},
			}
			return nil
		})
		g.Expect(err).NotTo(HaveOccurred())
		pod.Status = corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: exitCode,
						},
					},
				},
			},
		}
		err = k8sClient.Status().Update(ctx, &pod)
		g.Expect(err).NotTo(HaveOccurred())

		jobKey := types.NamespacedName{Name: jobName, Namespace: namespace}
		var job batchv1.Job
		g.Expect(k8sClient.Get(ctx, jobKey, &job)).To(Succeed())
		job.Status.Conditions = []batchv1.JobCondition{
			{
				Type:   batchv1.JobFailed,
				Status: corev1.ConditionTrue,
				Reason: "PodFailurePolicy",
			},
			{
				Type:   batchv1.JobFailureTarget,
				Status: corev1.ConditionTrue,
				Reason: "PodFailurePolicy",
			},
		}
		if job.Status.StartTime == nil {
			job.Status.StartTime = &metav1.Time{Time: time.Now()}
		}
		job.Status.Failed = 1
		err = k8sClient.Status().Update(ctx, &job)
		g.Expect(err).ShouldNot(HaveOccurred())
	}, "5s", "1s").WithContext(ctx).Should(Succeed())
}

func createFinBackupWithSkipChecksumVerifyAnnotation(ctx context.Context, pvc *corev1.PersistentVolumeClaim) *finv1.FinBackup {
	GinkgoHelper()
	finbackup := NewFinBackup(namespace, utils.GetUniqueName("fb-full-"), pvc.Name, pvc.Namespace, "test-node")
	finbackup.SetAnnotations(map[string]string{
		annotationSkipChecksumVerify: annotationValueTrue,
	})
	Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())
	return finbackup
}

func expectEnableChecksumVerifyIsFalse(ctx SpecContext, jobName string) {
	GinkgoHelper()
	jobKey := client.ObjectKey{Namespace: namespace, Name: jobName}
	Eventually(func(g Gomega, ctx SpecContext) {
		var job batchv1.Job
		g.Expect(k8sClient.Get(ctx, jobKey, &job)).To(Succeed())
		g.Expect(job.Spec.Template.Spec.Containers).ToNot(BeEmpty())
		var envVar *corev1.EnvVar
		for i := range job.Spec.Template.Spec.Containers[0].Env {
			if job.Spec.Template.Spec.Containers[0].Env[i].Name == EnvEnableChecksumVerify {
				envVar = &job.Spec.Template.Spec.Containers[0].Env[i]
				break
			}
		}
		g.Expect(envVar).ToNot(BeNil())
		g.Expect(envVar.ValueFrom).ToNot(BeNil())
		g.Expect(envVar.ValueFrom.ConfigMapKeyRef).ToNot(BeNil())
		g.Expect(envVar.ValueFrom.ConfigMapKeyRef.Key).To(Equal(EnvEnableChecksumVerify))

		var cm corev1.ConfigMap
		cmKey := client.ObjectKey{Namespace: namespace, Name: envVar.ValueFrom.ConfigMapKeyRef.Name}
		g.Expect(k8sClient.Get(ctx, cmKey, &cm)).To(Succeed())
		g.Expect(cm.Data).To(HaveKeyWithValue(EnvEnableChecksumVerify, "false"))
	}, "5s", "1s").WithContext(ctx).Should(Succeed())
}

var _ = Describe("FinBackup Controller integration test", Ordered, func() {
	var reconciler *FinBackupReconciler
	var rbdRepo *fake.RBDRepository2
	var sc1 *storagev1.StorageClass
	var stopFunc context.CancelFunc

	BeforeAll(func() {
		sc1 = NewRBDStorageClass("integration", namespace, rbdPoolName)
		Expect(k8sClient.Create(context.TODO(), sc1)).Should(Succeed())

		volumeInfo := &fake.VolumeInfo{
			Namespace: utils.GetUniqueName("ns-"),
			PVCName:   utils.GetUniqueName("pvc-"),
			PVName:    utils.GetUniqueName("pv-"),
			PoolName:  rbdPoolName,
			ImageName: rbdImageName,
		}
		rbdRepo = fake.NewRBDRepository2(volumeInfo.PoolName, volumeInfo.ImageName)

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
		Expect(k8sClient.Delete(context.Background(), sc1)).Should(Succeed())
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
		var pvc1 *corev1.PersistentVolumeClaim
		var pv1 *corev1.PersistentVolume
		var finbackup1 *finv1.FinBackup
		var finbackup2 *finv1.FinBackup
		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV")
			pvc1, pv1 = NewPVCAndPV(sc1, namespace, "pvc-sample", "pv-sample", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc1)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv1)).Should(Succeed())

			By("creating a full FinBackup")
			finbackup1 = NewFinBackup(namespace, "fb1-sample", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup1)
			MakeFinBackupVerified(ctx, finbackup1)

			By("creating a incremental FinBackup")
			finbackup2 = NewFinBackup(namespace, "fb2-sample", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup2)
			MakeFinBackupVerified(ctx, finbackup2)

			By("deleting the full FinBackup")
			Expect(k8sClient.Delete(ctx, finbackup1)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			if err := k8sClient.Delete(ctx, finbackup2); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup2)
			}
			if err := k8sClient.Delete(ctx, finbackup1); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup1)
			}
			DeletePVCAndPV(ctx, pvc1.Namespace, pvc1.Name)
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
	//   FinBackup becomes StoredToNode when a backup-target PVC is recreated.
	//
	// Arrange:
	//   - Create a backup-target PVC.
	//   - Create a FinBackup and make it StoredToNode.
	//   - Recreate the backup-target PVC.
	//
	// Act:
	//   - Create a new FinBackup.
	//
	// Assert:
	//   - The new FinBackup becomes StoredToNode.
	Describe("FinBackup becomes StoredToNode when a backup-target PVC is recreated", func() {
		var pvc1 *corev1.PersistentVolumeClaim
		var pv1 *corev1.PersistentVolume
		var finbackup1 *finv1.FinBackup
		var finbackup2 *finv1.FinBackup
		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV")
			pvc1, pv1 = NewPVCAndPV(sc1, namespace, "pvc-1542", "pv-1542", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc1)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv1)).Should(Succeed())

			By("creating a full FinBackup")
			finbackup1 = NewFinBackup(namespace, "fb1-1542", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup1)
			MakeFinBackupVerified(ctx, finbackup1)

			By("recreating the backup-target PVC")
			DeletePVCAndPV(ctx, pvc1.Namespace, pvc1.Name)
			pvc1, pv1 = NewPVCAndPV(sc1, namespace, pvc1.Name, pv1.Name, rbdImageName)
			Expect(k8sClient.Create(ctx, pvc1)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv1)).Should(Succeed())

			By("reconciling a new FinBackup")
			finbackup2 = NewFinBackup(namespace, "fb2-1542", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())
		})
		AfterEach(func(ctx SpecContext) {
			if err := k8sClient.Delete(ctx, finbackup2); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup2)
			}
			if err := k8sClient.Delete(ctx, finbackup1); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup1)
			}
			DeletePVCAndPV(ctx, pvc1.Namespace, pvc1.Name)
		})

		It("should make the new FinBackup StoredToNode", func(ctx SpecContext) {
			By("waiting for the FinBackup to be ready")
			MakeFinBackupStoredToNode(ctx, finbackup2)
			MakeFinBackupVerified(ctx, finbackup2)
		})
	})

	// CSATEST-1621
	// Description:
	//   Ensure that appropriate labels and annotations are added when reconciling
	//   an incremental FinBackup.
	//
	// Arrange:
	//   - An RBD PVC exists.
	//   - A FinBackup as a full backup exists and is StoredToNode.
	//
	// Act:
	//   - Create a FinBackup (FB2) as an incremental backup.
	//
	// Assert:
	//   - The FinBackup (FB2) has the correct labels and annotations.
	Describe("creating an incremental backup after a full backup exists", func() {
		var pvc1 *corev1.PersistentVolumeClaim
		var pv1 *corev1.PersistentVolume
		var finbackup1 *finv1.FinBackup
		var finbackup2 *finv1.FinBackup
		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV")
			pvc1, pv1 = NewPVCAndPV(sc1, namespace, "pvc-1621", "pv-1621", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc1)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv1)).Should(Succeed())

			By("creating a full FinBackup")
			finbackup1 = NewFinBackup(namespace, "fb1-1621", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup1)
			MakeFinBackupVerified(ctx, finbackup1)

			By("creating an incremental FinBackup")
			finbackup2 = NewFinBackup(namespace, "fb2-1621", pvc1.Name, pvc1.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup2)
			MakeFinBackupVerified(ctx, finbackup2)
		})

		AfterEach(func(ctx SpecContext) {
			if err := k8sClient.Delete(ctx, finbackup2); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup2)
			}
			if err := k8sClient.Delete(ctx, finbackup1); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup1)
			}
			DeletePVCAndPV(ctx, pvc1.Namespace, pvc1.Name)
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

	// CSATEST-1505
	// Description:
	//   Prioritize the FinBackup with the smallest SnapID as the full backup.
	//
	// Arrange:
	//   - A backup-target PVC exists.
	//
	// Act:
	//   - Two FinBackups are created to target the same PVC.
	//
	// Assert:
	//   - A backup job is created only for the FinBackup with the smallest SnapID.
	Describe("prioritize the FinBackup with the smallest snapID for initial backup", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var smallestFB *finv1.FinBackup
		var largerFB *finv1.FinBackup

		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV")
			pvc, pv = NewPVCAndPV(sc1, namespace, "pvc-1505", "pv-1505", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating two FinBackups and ordering them by SnapID")
			fb1 := NewFinBackup(namespace, "fb1-1505", pvc.Name, pvc.Namespace, "test-node")
			fb2 := NewFinBackup(namespace, "fb2-1505", pvc.Name, pvc.Namespace, "test-node")
			smallestFB, largerFB = createTwoBackupsOrdered(ctx, k8sClient, fb1, fb2)
		})

		AfterEach(func(ctx SpecContext) {
			for _, fb := range []*finv1.FinBackup{smallestFB, largerFB} {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(fb), fb)).To(Succeed())
				if err := k8sClient.Delete(ctx, fb); err == nil {
					WaitForFinBackupRemoved(ctx, fb)
				}
			}
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should back up the FinBackup with the smallest SnapID", func(ctx SpecContext) {
			By("waiting for a backup job for the FinBackup with the smallest SnapID")
			Eventually(func(g Gomega) {
				var job batchv1.Job
				key := client.ObjectKey{Namespace: namespace, Name: backupJobName(smallestFB)}
				g.Expect(k8sClient.Get(ctx, key, &job)).To(Succeed())
			}, "5s", "1s").Should(Succeed())

			By("verifying no backup job exists for the FinBackup with the larger SnapID")
			Consistently(func(g Gomega) {
				ExpectNoJob(ctx, k8sClient, backupJobName(largerFB), namespace)
			}, "5s", "1s").Should(Succeed())
		})
	})

	// CSATEST-1506
	// Description:
	//   Prioritize the FinBackup with the smallest SnapID as the next incremental backup.
	//
	// Arrange:
	//   - A backup-target PVC exists.
	//   - FinBackup referring this PVC and is ready to use.
	//   - Create two new incremental FinBackups for the same PVC.
	//
	// Assert:
	//   - A backup job is created only for the FinBackup with the smallest SnapID.
	Describe("prioritize the FinBackup with the smallest snapID among new incrementals", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var finbackup *finv1.FinBackup
		var smallestFB *finv1.FinBackup
		var largerFB *finv1.FinBackup

		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV")
			pvc, pv = NewPVCAndPV(sc1, namespace, "pvc-1506", "pv-1506", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			By("creating FinBackup as a full backup and waiting until it becomes StoredToNode")
			finbackup = NewFinBackup(namespace, "fb1-1506", pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup)
			MakeFinBackupVerified(ctx, finbackup)

			By("creating two FinBackups and ordering them by SnapID")
			fb2 := NewFinBackup(namespace, "fb2-1506", pvc.Name, pvc.Namespace, "test-node")
			fb3 := NewFinBackup(namespace, "fb3-1506", pvc.Name, pvc.Namespace, "test-node")
			smallestFB, largerFB = createTwoBackupsOrdered(ctx, k8sClient, fb2, fb3)
		})

		AfterEach(func(ctx SpecContext) {
			for _, fb := range []*finv1.FinBackup{finbackup, smallestFB, largerFB} {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(fb), fb)).To(Succeed())
				if err := k8sClient.Delete(ctx, fb); err == nil {
					WaitForFinBackupRemoved(ctx, fb)
				}
			}
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should back up the FinBackup with the smallest SnapID", func(ctx SpecContext) {
			By("waiting for a backup job for the FinBackup with the smallest SnapID")
			Eventually(func(g Gomega) {
				var job batchv1.Job
				key := client.ObjectKey{Namespace: namespace, Name: backupJobName(smallestFB)}
				g.Expect(k8sClient.Get(ctx, key, &job)).To(Succeed())
			}, "5s", "1s").Should(Succeed())

			By("verifying no backup job exists for the FinBackup with the larger SnapID")
			Consistently(func(g Gomega) {
				ExpectNoJob(ctx, k8sClient, backupJobName(largerFB), namespace)
			}, "5s", "1s").Should(Succeed())
		})
	})

	Describe("verification process", func() {
		var pvc *corev1.PersistentVolumeClaim

		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV")
			var pv *corev1.PersistentVolume
			pvc, pv = NewPVCAndPV(sc1, namespace, "pvc-sample", "pv-sample", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			By("cleaning up PVC and PV")
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should set the condition Verified to False when fsck failed", func(ctx SpecContext) {
			// Description:
			//   Ensure that the condition Verified is set to False when fsck fails.
			//   In addition, when an incremental FinBackup is created after that,
			//   a backup Job is not created for the incremental FinBackup.
			//
			// Arrange:
			//   - Create a pair of PVC and PV, which is done in BeforeEach.
			//   - Create a FinBackup resource and make it StoredToNode.
			//
			// Act (1):
			//   Make the verification Job fail.
			//
			// Assert (1):
			//   The FinBackup has the condition Verified=False.
			//
			// Act (2):
			//   Create another FinBackup as an incremental backup.
			//
			// Assert (2):
			//   A backup Job is not created for the incremental FinBackup.

			// Arrange
			By("creating a full FinBackup")
			finbackup1 := NewFinBackup(namespace, "fb1-sample", pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup1)

			// Act (1)
			By("making the verification Job fail with exit code 3")
			makeJobFailWithExitCode(ctx, verificationJobName(finbackup1), 3)

			// Assert (1)
			By("checking the FinBackup has the condition Verified=False")
			Eventually(func(g Gomega) {
				var createdFinBackup finv1.FinBackup
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup1), &createdFinBackup)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(createdFinBackup.IsVerifiedFalse()).Should(BeTrue(), "FinBackup should meet the condition")
			}, "5s", "1s").Should(Succeed())

			// Act (2)
			By("creating an incremental FinBackup")
			finbackup2 := NewFinBackup(namespace, "fb2-sample", pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())

			// Assert (2)
			By("checking a new backup Job is not created")
			Consistently(func(g Gomega) {
				key := types.NamespacedName{Name: backupJobName(finbackup2), Namespace: namespace}
				var job batchv1.Job
				err := k8sClient.Get(ctx, key, &job)
				Expect(err).To(MatchError(k8serrors.IsNotFound, "should not find the Job"))
			}, "3s", "1s").Should(Succeed())

			By("cleaning up")
			if err := k8sClient.Delete(ctx, finbackup1); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup1)
			}
			if err := k8sClient.Delete(ctx, finbackup2); err == nil {
				WaitForFinBackupRemoved(ctx, finbackup2)
			}
		})

		It("should skip verification when skip-verify annotation is set to true", func(ctx SpecContext) {
			// Description:
			//   Ensure that verification is skipped when the skip-verify annotation is set to true.
			//   In addition, when the annotation is removed, a verification Job is not created.
			//
			// Arrange:
			//   Create a pair of PVC and PV, which is done in BeforeEach.
			//
			// Act (1):
			//   - Create a FinBackup resource with the skip-verify annotation set to true.
			//   - Make the FinBackup StoredToNode.
			//
			// Assert (1):
			//   - The FinBackup has the condition VerificationSkipped=True.
			//   - A verification Job is not created.
			//   - A cleanup Job is created.
			//
			// Act (2):
			//   - Make the cleanup Job succeed.
			//   - Remove the skip-verify annotation.
			//
			// Assert (2):
			//   - A verification Job is not created.

			// Act (1)
			By("creating a full FinBackup")
			finbackup1 := NewFinBackup(namespace, "fb1-sample", pvc.Name, pvc.Namespace, "test-node")
			finbackup1.SetAnnotations(map[string]string{
				AnnotationSkipVerify: "true", // Set skip-verify annotation to true
			})
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup1)

			// Assert (1)
			By("checking the FinBackup has the condition VerificationSkipped=True")
			Eventually(func(g Gomega) {
				var createdFinBackup finv1.FinBackup
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup1), &createdFinBackup)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(createdFinBackup.IsVerificationSkipped()).Should(BeTrue(), "FinBackup should meet the condition")
			}, "5s", "1s").Should(Succeed())

			By("checking the verification Job is not found")
			ExpectNoJob(ctx, k8sClient, verificationJobName(finbackup1), namespace)

			By("checking the cleanup Job is created")
			cleanupJobKey := client.ObjectKey{Namespace: namespace, Name: cleanupJobName(finbackup1)}
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, cleanupJobKey, &job)).To(Succeed())
			}, "5s", "1s").WithContext(ctx).Should(Succeed())

			// Act (2)
			By("making the cleanup Job succeed", func() {
				var job batchv1.Job
				Expect(k8sClient.Get(ctx, cleanupJobKey, &job)).To(Succeed())
				makeJobSucceeded(&job)
				err := k8sClient.Status().Update(ctx, &job)
				Expect(err).ShouldNot(HaveOccurred())
			})

			By("removing the skip-verify annotation")
			Eventually(func(g Gomega, ctx SpecContext) {
				var fb finv1.FinBackup
				fb.SetName(finbackup1.GetName())
				fb.SetNamespace(finbackup1.GetNamespace())
				_, err := ctrl.CreateOrUpdate(ctx, k8sClient, &fb, func() error {
					fb.SetAnnotations(map[string]string{})
					return nil
				})
				g.Expect(err).NotTo(HaveOccurred())
			}, "5s", "1s").WithContext(ctx).Should(Succeed())

			// Assert (2)
			By("checking a verification Job is not created")
			Consistently(func(g Gomega, ctx SpecContext) {
				verifJobKey := client.ObjectKey{Namespace: namespace, Name: verificationJobName(finbackup1)}
				var job batchv1.Job
				err := k8sClient.Get(ctx, verifJobKey, &job)
				g.Expect(err).To(MatchError(k8serrors.IsNotFound, "should not find the Job"))
			}, "3s", "1s").WithContext(ctx).Should(Succeed())

			By("deleting the FinBackup")
			Expect(k8sClient.Delete(ctx, finbackup1)).Should(Succeed())
			WaitForFinBackupRemoved(ctx, finbackup1)

			By("checking the cleanup Job is deleted")
			ExpectNoJob(ctx, k8sClient, cleanupJobName(finbackup1), namespace)
		})

		It("should not create verification Job before the FinBackup becomes StoredToNode", func(ctx SpecContext) {
			// Description:
			//   Ensure that a verification Job is not created before the FinBackup becomes StoredToNode.
			//
			// Arrange:
			//   Create a pair of PVC and PV, which is done in BeforeEach.
			//
			// Act:
			//   Create a FinBackup resource, but do not make it StoredToNode.
			//
			// Assert:
			//   A verification Job is not created.

			// Act
			By("creating a full FinBackup")
			finbackup1 := NewFinBackup(namespace, "fb1-sample", pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())

			// Assert
			Consistently(func(g Gomega, ctx SpecContext) {
				verifJobKey := client.ObjectKey{Namespace: namespace, Name: verificationJobName(finbackup1)}
				var job batchv1.Job
				err := k8sClient.Get(ctx, verifJobKey, &job)
				g.Expect(err).To(MatchError(k8serrors.IsNotFound, "should not find the Job"))
			}, "3s", "1s").WithContext(ctx).Should(Succeed())

			By("cleaning up")
			Expect(k8sClient.Delete(ctx, finbackup1)).Should(Succeed())
			WaitForFinBackupRemoved(ctx, finbackup1)
		})

		It("should delete verification Job when the FinBackup is deleted", func(ctx SpecContext) {
			// Description:
			//   Ensure that a verification Job is deleted when the FinBackup is deleted.
			//
			// Arrange:
			//   - Create a pair of PVC and PV, which is done in BeforeEach.
			//   - Create a FinBackup resource and make it StoredToNode.
			//
			// Act:
			//   Delete the FinBackup.
			//
			// Assert:
			//   The verification Job is deleted.

			// Arrange
			By("creating a full FinBackup")
			finbackup1 := NewFinBackup(namespace, "fb1-sample", pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup1)

			By("waiting for the verification Job to be created")
			verifJobKey := client.ObjectKey{Namespace: namespace, Name: verificationJobName(finbackup1)}
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, verifJobKey, &job)).To(Succeed())
			}, "5s", "1s").WithContext(ctx).Should(Succeed())

			// Act
			By("deleting the FinBackup")
			Expect(k8sClient.Delete(ctx, finbackup1)).Should(Succeed())
			WaitForFinBackupRemoved(ctx, finbackup1)

			// Assert
			By("checking the verification Job is deleted")
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				err := k8sClient.Get(ctx, verifJobKey, &job)
				g.Expect(err).To(MatchError(k8serrors.IsNotFound, "should not find the Job"))
			}, "5s", "1s").WithContext(ctx).Should(Succeed())
		})
	})

	Describe("checksum verification features", func() {
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume

		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV for checksum verification cases")
			pvc, pv = NewPVCAndPV(sc1, namespace, utils.GetUniqueName("pvc-csum-"), utils.GetUniqueName("pv-csum-"), rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			By("cleaning up PVC and PV")
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should set ENABLE_CHECKSUM_VERIFY=false in verification Job when skip-checksum-verify annotation is set", func(ctx SpecContext) {
			// Description:
			//   Ensure that when a FinBackup has the skip-checksum-verify annotation,
			//   the verification Job gets ENABLE_CHECKSUM_VERIFY=false environment variable
			//   to disable checksum verification during the verification process.
			//
			// Arrange:
			//   - Create a FinBackup with skip-checksum-verify annotation set to true.
			//
			// Act:
			//   - Make the FinBackup StoredToNode to trigger verification Job creation.
			//
			// Assert:
			//   - The verification Job has ENABLE_CHECKSUM_VERIFY environment variable set to false.

			// Arrange & Act
			By("creating a full FinBackup with skip-checksum-verify annotation")
			finbackup1 := createFinBackupWithSkipChecksumVerifyAnnotation(ctx, pvc)
			MakeFinBackupStoredToNode(ctx, finbackup1)

			// Assert
			By("checking ENABLE_CHECKSUM_VERIFY is false in the verification Job")
			expectEnableChecksumVerifyIsFalse(ctx, verificationJobName(finbackup1))
		})

		It("should set ENABLE_CHECKSUM_VERIFY=false in backup Job when skip-checksum-verify annotation is set", func(ctx SpecContext) {
			// Description:
			//   Ensure that when a FinBackup has the skip-checksum-verify annotation,
			//   the backup Job gets ENABLE_CHECKSUM_VERIFY=false environment variable
			//   to disable checksum verification during the backup process.
			//
			// Arrange:
			//   - Create a FinBackup with skip-checksum-verify annotation set to true.
			//
			// Act:
			//   - Make the FinBackup StoredToNode to trigger backup Job creation.
			//
			// Assert:
			//   - The backup Job has ENABLE_CHECKSUM_VERIFY environment variable set to false.

			// Arrange & Act
			By("creating a full FinBackup with skip-checksum-verify annotation")
			finbackup1 := createFinBackupWithSkipChecksumVerifyAnnotation(ctx, pvc)
			MakeFinBackupStoredToNode(ctx, finbackup1)

			// Assert
			By("checking ENABLE_CHECKSUM_VERIFY is false in the backup Job")
			expectEnableChecksumVerifyIsFalse(ctx, backupJobName(finbackup1))
		})

		It("should set ENABLE_CHECKSUM_VERIFY=false in deletion Job when skip-checksum-verify annotation is set", func(ctx SpecContext) {
			// Description:
			//   Ensure that when a FinBackup has the skip-checksum-verify annotation,
			//   the deletion Job gets ENABLE_CHECKSUM_VERIFY=false environment variable
			//   to disable checksum verification during the deletion process.
			//
			// Arrange:
			//   - Create a FinBackup with skip-checksum-verify annotation set to true.
			//   - Make the FinBackup StoredToNode and Verified.
			//
			// Act:
			//   - Delete the FinBackup to trigger deletion reconciliation.
			//   - Wait for cleanup Job to be created and make it succeed.
			//   - Wait for deletion Job to be created.
			//
			// Assert:
			//   - The deletion Job has ENABLE_CHECKSUM_VERIFY environment variable set to false.

			// Arrange
			By("creating a full FinBackup with skip-checksum-verify annotation")
			finbackup1 := createFinBackupWithSkipChecksumVerifyAnnotation(ctx, pvc)
			MakeFinBackupStoredToNode(ctx, finbackup1)
			MakeFinBackupVerified(ctx, finbackup1)

			// Act
			By("deleting the FinBackup to trigger deletion reconciliation")
			Expect(k8sClient.Delete(ctx, finbackup1)).Should(Succeed())

			By("waiting for the cleanup Job to be created")
			cleanupJobKey := client.ObjectKey{Namespace: namespace, Name: cleanupJobName(finbackup1)}
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, cleanupJobKey, &job)).To(Succeed())
			}, "30s", "1s").WithContext(ctx).Should(Succeed())

			By("making the cleanup Job succeed")
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, cleanupJobKey, &job)).To(Succeed())
				makeJobSucceeded(&job)
				g.Expect(k8sClient.Status().Update(ctx, &job)).Should(Succeed())
			}, "5s", "1s").WithContext(ctx).Should(Succeed())

			By("waiting for the deletion Job to be created")
			delJobKey := client.ObjectKey{Namespace: namespace, Name: deletionJobName(finbackup1)}
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, delJobKey, &job)).To(Succeed())
			}, "30s", "1s").WithContext(ctx).Should(Succeed())

			// Assert
			By("checking ENABLE_CHECKSUM_VERIFY is false in the deletion Job")
			expectEnableChecksumVerifyIsFalse(ctx, deletionJobName(finbackup1))
		})

		It("should set ChecksumMismatched=True when backup Job exits with code 2", func(ctx SpecContext) {
			// Description:
			//   Ensure that when backup Job detects a checksum mismatch (exit code 2),
			//   the FinBackup is set to ChecksumMismatched=True and subsequent
			//   incremental backups are blocked from being created.
			//
			// Arrange:
			//   - Create a full FinBackup to trigger backup Job creation.
			//
			// Act (1):
			//   - Wait for the backup Job to be created.
			//   - Make the backup Job fail with exit code 2 (checksum mismatch).
			//
			// Assert (1):
			//   - The first FinBackup has ChecksumMismatched=True condition.
			//
			// Act (2):
			//   - Create a new incremental FinBackup.
			//
			// Assert (2):
			//   - The incremental backup Job is not created due to checksum mismatch.

			// Arrange
			By("creating a full FinBackup")
			finbackup1 := NewFinBackup(namespace, utils.GetUniqueName("fb-full-"), pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())

			// Act (1)
			By("waiting for the backup Job to be created")
			backupJobKey := client.ObjectKey{Namespace: namespace, Name: backupJobName(finbackup1)}
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, backupJobKey, &job)).To(Succeed())
			}, "5s", "1s").WithContext(ctx).Should(Succeed())

			By("making the backup Job fail with exit code 2")
			makeJobFailWithExitCode(ctx, backupJobKey.Name, 2)

			// Assert (1)
			By("checking the FinBackup has the condition ChecksumMismatched=True")
			Eventually(func(g Gomega) {
				var updated finv1.FinBackup
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup1), &updated)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(updated.IsChecksumMismatched()).Should(BeTrue())
			}, "5s", "1s").Should(Succeed())

			// Act (2)
			By("creating an incremental FinBackup")
			finbackup2 := NewFinBackup(namespace, utils.GetUniqueName("fb-incremental-"), pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())

			// Assert (2)
			By("confirming backup Job is not created")
			Consistently(func(g Gomega) {
				key := types.NamespacedName{Name: backupJobName(finbackup2), Namespace: namespace}
				var job batchv1.Job
				err := k8sClient.Get(ctx, key, &job)
				g.Expect(err).To(MatchError(k8serrors.IsNotFound, "should not find the Job"))
			}, "3s", "1s").Should(Succeed())
		})

		It("should set ChecksumMismatched=True when verification Job exits with code 2", func(ctx SpecContext) {
			// Description:
			//   Ensure that when the verification Job exits with code 2 (checksum mismatch),
			//   FinBackup is set to ChecksumMismatched=True and subsequent incremental backups
			//   do not start a backup Job.
			//
			// Arrange:
			//   - Create a full FinBackup and make it StoredToNode.
			//
			// Act (1):
			//   - Make the verification Job fail with exit code 2.
			//
			// Assert (1):
			//   - The FinBackup has ChecksumMismatched=True condition.
			//
			// Act (2):
			//   - Create a new incremental FinBackup.
			//
			// Assert (2):
			//   - No backup Job is created for the incremental FinBackup.

			// Arrange
			By("creating a full FinBackup")
			finbackup1 := NewFinBackup(namespace, utils.GetUniqueName("fb-full-"), pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup1)

			// Act (1)
			By("making the verification Job fail with exit code 2")
			makeJobFailWithExitCode(ctx, verificationJobName(finbackup1), 2)

			// Assert (1)
			By("checking the FinBackup has the condition ChecksumMismatched=True")
			Eventually(func(g Gomega) {
				var updated finv1.FinBackup
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup1), &updated)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(updated.IsChecksumMismatched()).Should(BeTrue())
			}, "5s", "1s").Should(Succeed())

			// Act (2)
			By("creating the incremental FinBackup")
			finbackup2 := NewFinBackup(namespace, utils.GetUniqueName("fb-incremental-"), pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup2)).Should(Succeed())

			// Assert (2)
			By("confirming backup Job is not created")
			Consistently(func(g Gomega) {
				key := types.NamespacedName{Name: backupJobName(finbackup2), Namespace: namespace}
				var job batchv1.Job
				err := k8sClient.Get(ctx, key, &job)
				g.Expect(err).To(MatchError(k8serrors.IsNotFound, "should not find the Job"))
			}, "3s", "1s").Should(Succeed())
		})

		It("should set ChecksumMismatched=True when deletion Job exits with code 2", func(ctx SpecContext) {
			// Description:
			//   Ensure that when deletion Job detects a checksum mismatch (exit code 2),
			//   the FinBackup is set to ChecksumMismatched=True and the deletion process
			//   is stopped to prevent data corruption.
			//
			// Arrange:
			//   - Create a full FinBackup, make it StoredToNode and Verified.
			//
			// Act:
			//   - Delete the FinBackup to trigger deletion reconciliation.
			//   - Wait for cleanup Job to be created and make it succeed.
			//   - Wait for deletion Job to be created and make it fail with exit code 2.
			//
			// Assert:
			//   - The FinBackup has ChecksumMismatched=True condition.
			//   - The FinBackup has DeletionTimestamp set (deletion was initiated).
			//   - The deletion process is stopped (Finalizer remains).

			// Arrange
			By("creating a full FinBackup")
			finbackup1 := NewFinBackup(namespace, utils.GetUniqueName("fb-full-"), pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup1)
			MakeFinBackupVerified(ctx, finbackup1)

			// Act
			By("deleting the FinBackup to trigger deletion reconciliation")
			Expect(k8sClient.Delete(ctx, finbackup1)).Should(Succeed())

			By("waiting for the cleanup Job to be created")
			cleanupJobKey := client.ObjectKey{Namespace: namespace, Name: cleanupJobName(finbackup1)}
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, cleanupJobKey, &job)).To(Succeed())
			}, "30s", "1s").WithContext(ctx).Should(Succeed())

			By("making the cleanup Job succeed")
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, cleanupJobKey, &job)).To(Succeed())
				makeJobSucceeded(&job)
				g.Expect(k8sClient.Status().Update(ctx, &job)).Should(Succeed())
			}, "5s", "1s").WithContext(ctx).Should(Succeed())

			By("waiting for the deletion Job to be created")
			delJobKey := client.ObjectKey{Namespace: namespace, Name: deletionJobName(finbackup1)}
			Eventually(func(g Gomega, ctx SpecContext) {
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, delJobKey, &job)).To(Succeed())
			}, "30s", "1s").WithContext(ctx).Should(Succeed())

			By("making the deletion Job fail with exit code 2")
			makeJobFailWithExitCode(ctx, delJobKey.Name, 2)

			// Assert
			By("checking the FinBackup has ChecksumMismatched=True and deletion is stopped")
			Eventually(func(g Gomega) {
				var updated finv1.FinBackup
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup1), &updated)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(updated.IsChecksumMismatched()).Should(BeTrue())
				g.Expect(updated.DeletionTimestamp.IsZero()).Should(BeFalse())
			}, "5s", "1s").Should(Succeed())

			By("confirming the deletion process is stopped (Finalizer remains)")
			Consistently(func(g Gomega) {
				var updated finv1.FinBackup
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup1), &updated)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(updated.Finalizers).Should(ContainElement("finbackup.fin.cybozu.io/finalizer"))
			}, "3s", "1s").Should(Succeed())
		})
	})

	Describe("Auto Deletion Feature", func() {
		var pvc *corev1.PersistentVolumeClaim

		BeforeEach(func(ctx SpecContext) {
			By("creating a pair of PVC and PV")
			var pv *corev1.PersistentVolume
			pvc, pv = NewPVCAndPV(sc1, namespace, "pvc-sample", "pv-sample", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			By("cleaning up PVC and PV")
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
		})

		It("should set AutoDeleteCompleted condition even when there is no old FinBackup to delete", func(ctx SpecContext) {
			finbackup1 := NewFinBackup(namespace, "fb1-sample", pvc.Name, pvc.Namespace, "test-node")
			Expect(k8sClient.Create(ctx, finbackup1)).Should(Succeed())
			MakeFinBackupStoredToNode(ctx, finbackup1)

			By("making the verification job succeed")
			Eventually(func(g Gomega) {
				jobKey := client.ObjectKey{Namespace: namespace, Name: verificationJobName(finbackup1)}
				var job batchv1.Job
				g.Expect(k8sClient.Get(ctx, jobKey, &job)).To(Succeed())
				makeJobSucceeded(&job)
				err := k8sClient.Status().Update(ctx, &job)
				g.Expect(err).ShouldNot(HaveOccurred())
			}, "5s", "1s").Should(Succeed())

			By("checking the FinBackup has the condition AutoDeleteCompleted=True")
			Eventually(func(g Gomega) {
				var createdFinBackup finv1.FinBackup
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(finbackup1), &createdFinBackup)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(createdFinBackup.IsAutoDeleteCompleted()).Should(BeTrue(), "FinBackup should meet the condition")
			}, "5s", "1s").Should(Succeed())
		})
	})
})

var _ = Describe("FinBackup Controller Reconcile Test", Ordered, func() {
	var reconciler *FinBackupReconciler
	var rbdRepo *fake.RBDRepository2
	var sc *storagev1.StorageClass

	BeforeAll(func(ctx SpecContext) {
		sc = NewRBDStorageClass("unit", namespace, rbdPoolName)
		Expect(k8sClient.Create(ctx, sc)).Should(Succeed())
	})

	AfterAll(func(ctx SpecContext) {
		Expect(k8sClient.Delete(ctx, sc)).Should(Succeed())
	})

	BeforeEach(func(ctx SpecContext) {
		volumeInfo := &fake.VolumeInfo{
			Namespace: utils.GetUniqueName("ns-"),
			PVCName:   utils.GetUniqueName("pvc-"),
			PVName:    utils.GetUniqueName("pv-"),
			PoolName:  rbdPoolName,
			ImageName: rbdImageName,
		}

		rbdRepo = fake.NewRBDRepository2(volumeInfo.PoolName, volumeInfo.ImageName)
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
		var otherStorageClass *storagev1.StorageClass
		var pvc2 *corev1.PersistentVolumeClaim
		var pv2 *corev1.PersistentVolume
		var finbackup *finv1.FinBackup

		BeforeEach(func(ctx SpecContext) {
			otherStorageClass = NewRBDStorageClass("other", otherNamespace.Name, rbdPoolName)
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
			Expect(k8sClient.Delete(ctx, otherStorageClass)).Should(Succeed())
		})

		It("should neither return an error nor create a backup job during reconciliation", func(ctx SpecContext) {
			By("reconciling the FinBackup")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that no backup job is created")
			ExpectNoJob(ctx, k8sClient, backupJobName(finbackup), finbackup.Namespace)
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
		var nonCephStorageClass *storagev1.StorageClass
		var pvc2 *corev1.PersistentVolumeClaim
		var pv2 *corev1.PersistentVolume
		var finbackup *finv1.FinBackup

		BeforeEach(func(ctx SpecContext) {
			By("creating another namespace for non-Ceph tests")
			nonCephNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test-namespace-non-ceph"}}
			Expect(k8sClient.Create(ctx, nonCephNamespace)).Should(Succeed())

			By("creating a non-Ceph StorageClass")
			nonCephStorageClass = &storagev1.StorageClass{
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
			Expect(k8sClient.Delete(ctx, nonCephStorageClass)).Should(Succeed())
		})

		It("should neither return an error nor create a backup job during reconciliation", func(ctx SpecContext) {
			By("reconciling the FinBackup")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that no backup job is created")
			ExpectNoJob(ctx, k8sClient, backupJobName(finbackup), finbackup.Namespace)
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

		It("should neither return an error nor create a backup job during reconciliation", func(ctx SpecContext) {
			By("reconciling the FinBackup")
			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
			Expect(err).ShouldNot(HaveOccurred())

			By("checking that no backup job is created")
			ExpectNoJob(ctx, k8sClient, backupJobName(finbackup), finbackup.Namespace)
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
				Expect(err).To(MatchError(k8serrors.IsNotFound, "should not find the FinBackup"))

				By("checking that no cleanup or deletion jobs are created")
				ExpectNoJob(ctx, k8sClient, cleanupJobName(finbackup), finbackup.Namespace)
				ExpectNoJob(ctx, k8sClient, deletionJobName(finbackup), finbackup.Namespace)
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

			It("should return an error and not create a backup job during reconciliation", func(ctx SpecContext) {
				By("reconciling the FinBackup")
				_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
				Expect(err).Should(HaveOccurred())
				Expect(err).Should(MatchError(ContainSubstring("backup target PVC UID does not match (inLabel=")))

				By("checking that no backup job is created")
				ExpectNoJob(ctx, k8sClient, backupJobName(finbackup), finbackup.Namespace)
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

			It("should return an error and not create a backup job during reconciliation", func(ctx SpecContext) {
				By("reconciling the FinBackup")
				_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(finbackup)})
				Expect(err).Should(HaveOccurred())
				Expect(err).Should(MatchError(ContainSubstring("backup target PVC UID does not match (inStatus=")))

				By("checking that no backup job is created")
				ExpectNoJob(ctx, k8sClient, backupJobName(finbackup), finbackup.Namespace)
			})
		})
	})

	Context("Automatic deletion feature for FinBackup", func() {
		var oldFinBackup *finv1.FinBackup
		var finBackup *finv1.FinBackup
		var pvc *corev1.PersistentVolumeClaim
		var pv *corev1.PersistentVolume
		var imgName = "image-for-cleanup"
		var labels map[string]string

		var newFinBackup = func(
			ctx context.Context,
			name, node string,
			labels map[string]string,
			snapID int,
		) *finv1.FinBackup {
			fb := NewFinBackup(namespace, name, pvc.Name, pvc.Namespace, node)
			fb.Labels = labels
			Expect(k8sClient.Create(ctx, fb)).Should(Succeed())
			fb.Status.SnapID = ptr.To(snapID)
			Expect(k8sClient.Status().Update(ctx, fb)).Should(Succeed())
			return fb
		}

		BeforeEach(func(ctx SpecContext) {
			pvc, pv = NewPVCAndPV(sc, namespace, "test-pvc-cleanup", "test-pv-cleanup", imgName)
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pv)).Should(Succeed())

			labels = map[string]string{labelBackupTargetPVCUID: string(pvc.GetUID())}

			oldFinBackup = NewFinBackup(namespace, "old-finbackup", pvc.Name, pvc.Namespace, "test-node")
			oldFinBackup.Labels = labels
			Expect(k8sClient.Create(ctx, oldFinBackup)).Should(Succeed())
			oldFinBackup.Status.SnapID = ptr.To(1)
			Expect(k8sClient.Status().Update(ctx, oldFinBackup)).Should(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
			Expect(k8sClient.Delete(ctx, finBackup)).Should(Succeed())
			_ = k8sClient.Delete(ctx, oldFinBackup)
		})

		// Description:
		//   Delete the single older FinBackup targeting the same PVC.
		//
		// Preconditions:
		//   - A backup-target PVC and PV exist.
		//   - One older FinBackup is labeled with the backup-target PVC UID.
		//
		// Arrange:
		//   - Create a newer FinBackup with the backup-target PVC UID label.
		//
		// Act:
		//   - Call deleteOldFinBackup().
		//
		// Assert:
		//   - deleteOldFinBackup() returns no error.
		//   - The older FinBackup is deleted.
		It("should delete the older FinBackup targeting the same PVC", func(ctx SpecContext) {
			By("creating the newer FinBackup")
			finBackup = newFinBackup(ctx, "finbackup-test", "test-node", labels, 2)

			By("calling deleteOldFinBackup()")
			err := reconciler.deleteOldFinBackup(ctx, finBackup, pvc)
			Expect(err).ShouldNot(HaveOccurred())

			By("verifying the AutoDeleteCompleted condition is true")
			Eventually(func(g Gomega, ctx SpecContext) {
				var updated finv1.FinBackup
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(finBackup), &updated)).To(Succeed())
				g.Expect(updated.IsAutoDeleteCompleted()).To(BeTrue())
			}, "5s", "1s").WithContext(ctx).Should(Succeed())

			By("verifying the older FinBackup is deleted")
			var fb finv1.FinBackup
			err = k8sClient.Get(ctx, client.ObjectKeyFromObject(oldFinBackup), &fb)
			Expect(err).To(MatchError(k8serrors.IsNotFound, "should not find the older FinBackup"))
		})

		// Description:
		//   Return an error and retain all older FinBackups if more than one exists on the same node.
		//
		// Preconditions:
		//   - A backup-target PVC and PV exist.
		//   - One older FinBackup is labeled with backup-target PVC UID.
		//
		// Arrange:
		//   - Create a second older FinBackup with the same labels on the same node.
		//   - Create a newer FinBackup with a larger SnapID on the same node.
		//
		// Act:
		//   - Call deleteOldFinBackup().
		//
		// Assert:
		//   - deleteOldFinBackup() returns an error indicating only one older FinBackup is allowed.
		//   - Both older FinBackups exist.
		It("should return an error when more than one older FinBackup exists on the same node", func(ctx SpecContext) {
			By("creating the second older FinBackup")
			oldFinBackup2 := newFinBackup(ctx, "older-finbackup-test-2", "test-node", labels, 2)

			By("creating the newer FinBackup")
			finBackup = newFinBackup(ctx, "finbackup-test", "test-node", labels, 3)

			By("calling deleteOldFinBackup()")
			err := reconciler.deleteOldFinBackup(ctx, finBackup, pvc)
			Expect(err).To(MatchError(ContainSubstring("only one older FinBackup is allowed")))

			By("verifying both older FinBackups exist")
			var fb finv1.FinBackup
			err = k8sClient.Get(ctx, client.ObjectKeyFromObject(oldFinBackup), &fb)
			Expect(err).ShouldNot(HaveOccurred())

			err = k8sClient.Get(ctx, client.ObjectKeyFromObject(oldFinBackup2), &fb)
			Expect(err).ShouldNot(HaveOccurred())

			// Cleanup the additionally created older FinBackup
			Expect(k8sClient.Delete(ctx, oldFinBackup2)).Should(Succeed())
		})

		// Description:
		//   Delete the older FinBackup that resides on a different node.
		//
		// Preconditions:
		//   - A backup-target PVC and PV exist.
		//   - One older FinBackup is labeled with the backup-target PVC UID.
		//
		// Arrange:
		//   - Create a newer FinBackup on a different node.
		//
		// Act:
		//   - Call deleteOldFinBackup().
		//
		// Assert:
		//   - deleteOldFinBackup() returns no error.
		//   - The older FinBackup on the different node is deleted.
		It("should delete the older FinBackup on a different node", func(ctx SpecContext) {
			By("creating the newer FinBackup on a different node")
			finBackup = newFinBackup(ctx, "finbackup-test", "different-node", labels, 2)

			By("calling deleteOldFinBackup()")
			err := reconciler.deleteOldFinBackup(ctx, finBackup, pvc)
			Expect(err).ShouldNot(HaveOccurred())

			By("verifying the older FinBackup on the different node is deleted")
			var fb finv1.FinBackup
			err = k8sClient.Get(ctx, client.ObjectKeyFromObject(oldFinBackup), &fb)
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())
		})

	})
})

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
//     . On the same node, not ready and is not being deleted, returns false.
//     . On the same node, two Finbackups exists, returns false.
//   - If a larger SnapID exists, returns true.
func Test_snapIDPreconditionSatisfied(t *testing.T) {
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
					Type:   finv1.BackupConditionStoredToNode,
					Status: metav1.ConditionTrue,
				},
				{
					Type:   finv1.BackupConditionVerified,
					Status: metav1.ConditionTrue,
				},
			}
		}
		return fb
	}

	tests := []struct {
		name            string
		backup          *finv1.FinBackup
		otherFinBackups []finv1.FinBackup
		wantErr         bool
	}{
		{
			name:   "another FinBackup with nil SnapID",
			backup: createFinBackup("new-backup", 2, "node1", true, nil),
			otherFinBackups: []finv1.FinBackup{
				*createFinBackup("backup", 0, "node1", true, nil),
			},
			wantErr: true,
		},
		{
			name:   "another FinBackup in different node",
			backup: createFinBackup("new-backup", 3, "node1", true, nil),
			otherFinBackups: []finv1.FinBackup{
				*createFinBackup("backup", 2, "node1", true, nil),
				*createFinBackup("other-backup", 1, "node2", true, nil),
			},
			wantErr: true,
		},
		{
			name:   "only FinBackup in different node",
			backup: createFinBackup("new-backup", 2, "node1", true, nil),
			otherFinBackups: []finv1.FinBackup{
				*createFinBackup("other-backup", 1, "node2", true, nil),
			},
			wantErr: false,
		},
		{
			name:   "another FinBackup is deleted",
			backup: createFinBackup("new-backup", 2, "node1", true, nil),
			otherFinBackups: []finv1.FinBackup{
				*createFinBackup("backup-1", 1, "node1", false, &deletionTimestamp),
			},
			wantErr: false,
		},
		{
			name:   "another FinBackup is not ready",
			backup: createFinBackup("new-backup", 3, "node1", true, nil),
			otherFinBackups: []finv1.FinBackup{
				*createFinBackup("backup-2", 2, "node1", true, nil),
				*createFinBackup("backup-1", 1, "node1", false, nil),
			},
			wantErr: true,
		},
		{
			name:   "another FinBackup has a larger SnapID",
			backup: createFinBackup("new-backup", 3, "node1", true, nil),
			otherFinBackups: []finv1.FinBackup{
				*createFinBackup("larger-backup", 4, "node1", true, nil),
			},
			wantErr: false,
		},
		{
			name:   "two smaller SnapIDs",
			backup: createFinBackup("new-backup", 3, "node1", false, nil),
			otherFinBackups: []finv1.FinBackup{
				*createFinBackup("backup", 1, "node1", true, nil),
				*createFinBackup("other", 2, "node1", true, nil),
			},
			wantErr: true,
		},
		{
			name:   "two smaller SnapIDs one being deleted",
			backup: createFinBackup("new-backup", 3, "node1", false, nil),
			otherFinBackups: []finv1.FinBackup{
				*createFinBackup("backup", 1, "node1", true, &deletionTimestamp),
				*createFinBackup("other", 2, "node1", true, nil),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := snapIDPreconditionSatisfied(tt.backup, tt.otherFinBackups)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("snapIDPreconditionSatisfied() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("snapIDPreconditionSatisfied() succeeded unexpectedly")
			}
		})
	}
}

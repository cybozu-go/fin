package e2e

import (
	"fmt"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func deleteIncrementalBackupTestSuite() {
	var ns *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var finbackup1 *finv1.FinBackup
	var finbackup2 *finv1.FinBackup
	var err error

	BeforeAll(func(ctx SpecContext) {
		By("creating a namespace")
		ns = NewNamespace(utils.GetUniqueName("test-ns-"))
		Expect(CreateNamespace(ctx, k8sClient, ns)).NotTo(HaveOccurred())
		pvc = CreateBackupTargetPVC(ctx, k8sClient, ns, "Block", rookStorageClass, "ReadWriteOnce", "100Mi")
	})

	AfterAll(func(ctx SpecContext) {
		By("deleting the remaining resources")
		for _, fb := range []*finv1.FinBackup{finbackup1, finbackup2} {
			if fb == nil {
				continue
			}
			_ = DeleteFinBackup(ctx, ctrlClient, fb)
			Expect(WaitForFinBackupDeletion(ctx, ctrlClient, fb, 2*time.Minute)).NotTo(HaveOccurred())
		}
		Expect(DeletePVC(ctx, k8sClient, pvc)).NotTo(HaveOccurred())
		Expect(DeleteNamespace(ctx, k8sClient, ns)).NotTo(HaveOccurred())
	})

	// CSATEST-1606
	// Description:
	//	Delete incremental backup does not work.
	//
	// Precondition (set by BeforeAll):
	//  - An RBD PVC is created on the Ceph cluster.
	//  - FinBackup1 that references the PVC
	//    and has the "StoredToNode" condition set to "True" is created.
	//  - New random data is written to the RBD PVC.
	//
	// Arrange:
	//   - Create the full and incremental backup.
	//
	// Act:
	//   - Delete the incremental backup.
	//
	// Assert:
	//   - Incremental backup has deletion timestamp.
	//   - Both full backup and incremental backup remain.
	It("should create an incremental backup", func(ctx SpecContext) {
		// Arrange
		finbackup1 = CreateBackup(ctx, ctrlClient, rookNamespace, pvc, "minikube-worker")
		finbackup2 = CreateBackup(ctx, ctrlClient, rookNamespace, pvc, "minikube-worker")

		// Act
		Expect(DeleteFinBackup(ctx, ctrlClient, finbackup2)).NotTo(HaveOccurred())

		// Assert
		By("verifying incremental backup has deletion timestamp")
		Eventually(func() error {
			err := ctrlClient.Get(ctx, client.ObjectKeyFromObject(finbackup2), finbackup2)
			if err != nil {
				return err
			}
			if finbackup2.DeletionTimestamp == nil {
				return fmt.Errorf("finbackup2 does not have deletion timestamp yet")
			}
			return nil
		}, "5s", "1s").WithContext(ctx).Should(Succeed())

		By("verifying both backups remain and the deletion job is not completed")
		Consistently(func() error {
			var dummy finv1.FinBackup
			err = ctrlClient.Get(ctx, client.ObjectKeyFromObject(finbackup1), &dummy)
			if err != nil {
				return err
			}
			err = ctrlClient.Get(ctx, client.ObjectKeyFromObject(finbackup2), &dummy)
			if err != nil {
				return err
			}
			var job batchv1.Job
			err = ctrlClient.Get(ctx, client.ObjectKey{
				Namespace: rookNamespace,
				Name:      fmt.Sprintf("fin-deletion-%s", finbackup1.UID),
			}, &job)
			completed, err := jobCompleted(&job)
			if err != nil {
				return err
			}
			if completed {
				return fmt.Errorf("deletion job should not be completed")
			}
			return nil
		}, "10s", "1s").WithContext(ctx).Should(Succeed())
	})
}

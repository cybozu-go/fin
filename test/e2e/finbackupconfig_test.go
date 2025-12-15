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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func finbackupconfigTestSuite() {
	var finNS, pvcNS *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var fbc *finv1.FinBackupConfig
	var finbackups []*finv1.FinBackup
	var err error

	BeforeAll(func(ctx SpecContext) {
		By("creating a namespace")

		pvcNS = NewNamespace(utils.GetUniqueName("fbc-pvc-"))
		Expect(CreateNamespace(ctx, k8sClient, pvcNS)).NotTo(HaveOccurred())

		pvc = CreateBackupTargetPVC(ctx, k8sClient, pvcNS, "Block", rookStorageClass, "ReadWriteOnce", "100Mi")

		finNS = NewNamespace(utils.GetUniqueName("fin-"))
		Expect(CreateNamespace(ctx, k8sClient, finNS)).NotTo(HaveOccurred())
		CopyFBCServiceAccount(ctx, k8sClient, rookNamespace, finNS.Name)
	})

	AfterAll(func(ctx SpecContext) {
		By("cleaning up resources")
		for _, fb := range finbackups {
			if fb != nil {
				_ = DeleteFinBackup(ctx, ctrlClient, fb)
				_ = WaitForFinBackupDeletion(ctx, ctrlClient, fb, 30*time.Second)
			}
		}

		opt := &client.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationForeground)}
		Expect(ctrlClient.Delete(ctx, fbc, opt)).NotTo(HaveOccurred())

		Expect(DeletePVC(ctx, k8sClient, pvc)).NotTo(HaveOccurred())
		Expect(DeleteNamespace(ctx, k8sClient, finNS)).NotTo(HaveOccurred())
		Expect(DeleteNamespace(ctx, k8sClient, pvcNS)).NotTo(HaveOccurred())
	})

	// Description:
	//   Verify that FinBackupConfig correctly manages scheduled and manual backups,
	//   and that auto-deletion respects backup retention policy (at most 1 backup).
	//
	// Arrange:
	//   - Create namespace for PVC with test data
	//   - Create namespace for Fin resources
	//   - Copy ServiceAccount create-finbackup-job from Rook namespace to Fin namespace
	//
	// Act:
	//   1. Create FinBackupConfig with suspend=true and verify CronJob is created
	//   2. Schedule backup via CronJob managed by FinBackupConfig
	//   3. Create manual FinBackup
	//   4. Change FinBackupConfig node and schedule backup
	//
	// Assert:
	//   - Scheduled backup via FinBackupConfig succeeds
	//   - Auto-deletion respects backup retention policy (max 1 backup)
	//   - Scheduled backup and auto-deletion work correctly even with manual creation and node changes
	It("should manage automated and manual backups with single older backup retention", func(ctx SpecContext) {
		// 1. Create FinBackupConfig with suspend=true and verify CronJob is created
		By("creating FBC with suspend=true and dummy schedule")
		fbc, err = NewFinBackupConfig(finNS.Name, utils.GetUniqueName("test-fbc-"), pvc, nodes[0], "0 0 * * *")
		Expect(err).NotTo(HaveOccurred())
		fbc.Spec.Suspend = true
		Expect(ctrlClient.Create(ctx, fbc)).NotTo(HaveOccurred())

		By("verifying CronJob created and suspended")
		cj := &batchv1.CronJob{}
		Eventually(func(g Gomega) {
			key := client.ObjectKey{Namespace: fbc.Namespace, Name: fmt.Sprintf("fbc-%s", fbc.UID)}
			g.Expect(ctrlClient.Get(ctx, key, cj)).NotTo(HaveOccurred())
			g.Expect(*cj.Spec.Suspend).To(Equal(true))
		}, "60s", "1s").Should(Succeed())

		// 2. Schedule backup via CronJob managed by FinBackupConfig
		By("creating FB_a and waiting for Verified")
		fb_a_jobName, err := CreateJobFromCronJob(ctx, cj)
		Expect(err).NotTo(HaveOccurred())

		fb_a := WaitForFinBackupVerifiedFromJobName(ctx, ctrlClient, fbc, fb_a_jobName, 20*time.Second)
		finbackups = append(finbackups, fb_a)

		By("creating FB_b and waiting for Verified")
		fb_b_jobName, err := CreateJobFromCronJob(ctx, cj)
		Expect(err).NotTo(HaveOccurred())

		fb_b := WaitForFinBackupVerifiedFromJobName(ctx, ctrlClient, fbc, fb_b_jobName, 20*time.Second)
		finbackups = append(finbackups, fb_b)

		By("FB_a should be auto-deleted")
		WaitForFinBackupNotFound(ctx, ctrlClient, fb_a, 20*time.Second)

		// 3. Create manual FinBackup
		By("creating FB_manual and waiting for Verified")
		fb_manual, err := NewFinBackup(fbc.Namespace, utils.GetUniqueName("test-fb-manual-"), pvc, fbc.Spec.Node)
		Expect(err).NotTo(HaveOccurred())
		Expect(CreateFinBackup(ctx, ctrlClient, fb_manual)).NotTo(HaveOccurred())
		finbackups = append(finbackups, fb_manual)

		Eventually(func(g Gomega) {
			fb := &finv1.FinBackup{}
			g.Expect(ctrlClient.Get(ctx, client.ObjectKeyFromObject(fb_manual), fb)).NotTo(HaveOccurred())
			g.Expect(fb.IsVerifiedTrue()).To(BeTrue())
		}, "20s", "1s").Should(Succeed())

		By("FB_b should be auto-deleted")
		WaitForFinBackupNotFound(ctx, ctrlClient, fb_b, 20*time.Second)

		By("creating FB_c and waiting for Verified")
		fb_c_jobName, err := CreateJobFromCronJob(ctx, cj)
		Expect(err).NotTo(HaveOccurred())

		fb_c := WaitForFinBackupVerifiedFromJobName(ctx, ctrlClient, fbc, fb_c_jobName, 20*time.Second)
		finbackups = append(finbackups, fb_c)

		By("FB_manual should be auto-deleted")
		WaitForFinBackupNotFound(ctx, ctrlClient, fb_manual, 20*time.Second)

		// 4. Change FinBackupConfig node and schedule backup
		By("changing FBC node to other node")
		UpdateFinBackupConfigNode(ctx, ctrlClient, fbc, nodes[1])

		By("creating FB_d on other node and waiting for Verified")
		fb_d_jobName, err := CreateJobFromCronJob(ctx, cj)
		Expect(err).NotTo(HaveOccurred())

		fb_d := WaitForFinBackupVerifiedFromJobName(ctx, ctrlClient, fbc, fb_d_jobName, 20*time.Second)
		finbackups = append(finbackups, fb_d)

		By("FB_c should be auto-deleted")
		WaitForFinBackupNotFound(ctx, ctrlClient, fb_c, 30*time.Second)

		By("reverting FBC node to initial node")
		UpdateFinBackupConfigNode(ctx, ctrlClient, fbc, nodes[0])

		By("creating FB_e on initial node and waiting for Verified")
		fb_e_jobName, err := CreateJobFromCronJob(ctx, cj)
		Expect(err).NotTo(HaveOccurred())

		fb_e := WaitForFinBackupVerifiedFromJobName(ctx, ctrlClient, fbc, fb_e_jobName, 20*time.Second)
		finbackups = append(finbackups, fb_e)

		By("FB_d should be auto-deleted")
		WaitForFinBackupNotFound(ctx, ctrlClient, fb_d, 20*time.Second)
	})
}

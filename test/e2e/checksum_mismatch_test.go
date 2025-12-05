package e2e

import (
	"path/filepath"
	"strconv"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func checksumMismatchTestSuite() {
	// Description:
	//   Deletion Job fails with checksum mismatch when diff checksum is corrupted
	//
	// Arrange:
	//   - Create full backup and incremental backup.
	//   - Corrupt diff checksum file before deleting full backup.
	//
	// Act:
	//   - Delete full backup to trigger diff application.
	//
	// Assert:
	//   - Full FinBackup sets ChecksumMismatched=True after merge attempt.
	//   - Deletion process fails due to the checksum mismatch.
	It("should fail deletion job when diff checksum is corrupted", func(ctx SpecContext) {
		// Arrange
		By("creating a namespace for the checksum mismatch test")
		ns := NewNamespace(utils.GetUniqueName("test-ns-"))
		err := CreateNamespace(ctx, k8sClient, ns)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DeleteNamespace(ctx, k8sClient, ns)
		})

		pvc := CreateBackupTargetPVC(ctx, k8sClient, ns, "Block", rookStorageClass, "ReadWriteOnce", "100Mi")
		pod := CreatePodForBlockPVC(ctx, k8sClient, pvc)
		DeferCleanup(func() {
			_ = DeletePod(ctx, k8sClient, pod)
			_ = DeletePVC(ctx, k8sClient, pvc)
		})

		dataSize := int64(4 * 1024)
		_ = WriteRandomDataToPVC(ctx, pod, devicePathInPodForPVC, dataSize)

		By("creating full backup")
		fullBackup := CreateBackup(ctx, ctrlClient, rookNamespace, pvc, nodes[0])
		DeferCleanup(func() {
			fb := &finv1.FinBackup{}
			if err := ctrlClient.Get(ctx, client.ObjectKeyFromObject(fullBackup), fb); err == nil {
				fb.Finalizers = nil
				_ = ctrlClient.Update(ctx, fb)
			}
			_ = DeleteFinBackup(ctx, ctrlClient, fullBackup)
		})

		By("writing additional data for incremental backup")
		_ = WriteRandomDataToPVC(ctx, pod, devicePathInPodForPVC, dataSize)

		By("creating incremental backup")
		incBackup, err := NewFinBackup(rookNamespace, utils.GetUniqueName("test-finbackup-"), pvc, nodes[0])
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinBackup(ctx, ctrlClient, incBackup)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			fb := &finv1.FinBackup{}
			if err := ctrlClient.Get(ctx, client.ObjectKeyFromObject(incBackup), fb); err == nil {
				fb.Finalizers = nil
				_ = ctrlClient.Update(ctx, fb)
			}
			_ = DeleteFinBackup(ctx, ctrlClient, incBackup)
		})

		By("waiting for incremental backup to be stored")
		Eventually(func(g Gomega, ctx SpecContext) {
			err := ctrlClient.Get(ctx, client.ObjectKeyFromObject(incBackup), incBackup)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(incBackup.IsStoredToNode()).To(BeTrue())
			g.Expect(incBackup.Status.SnapID).NotTo(BeNil())
		}, "60s", "1s").WithContext(ctx).Should(Succeed())

		By("checking diff file exists")
		diffChecksumPath := filepath.Join("/fin", pvc.Namespace, pvc.Name, "diff", strconv.Itoa(*incBackup.Status.SnapID), "part-0.csum")
		Eventually(func(g Gomega) {
			_, stderr, err := minikubeSSH(nodes[0], nil, "test", "-f", diffChecksumPath)
			g.Expect(err).NotTo(HaveOccurred(), "diff checksum file should exist. stderr: "+string(stderr))
		}, "3m", "2s").Should(Succeed())

		By("corrupting diff checksum file before deletion")
		CorruptFileOnNode(nodes[0], diffChecksumPath)

		// Act
		By("deleting full backup to trigger incremental merge")
		err = DeleteFinBackup(ctx, ctrlClient, fullBackup)
		Expect(err).NotTo(HaveOccurred())

		// Assert
		By("waiting for checksum mismatch during incremental merge")
		err = WaitForFinBackupChecksumMismatch(ctx, ctrlClient, fullBackup, 1*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("checking full backup has ChecksumMismatched condition")
		err = ctrlClient.Get(ctx, client.ObjectKeyFromObject(fullBackup), fullBackup)
		Expect(err).NotTo(HaveOccurred())
		Expect(fullBackup.IsChecksumMismatched()).To(BeTrue())
	})
}

package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func incrementalBackupTestSuite() {
	var ns *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var podForPVC *corev1.Pod
	var finbackup1 *finv1.FinBackup
	var finbackup2 *finv1.FinBackup
	var dataOnFullBackup, dataOnIncrementalBackup []byte
	var volumePath string
	var devicePath string
	var err error

	BeforeAll(func(ctx SpecContext) {
		By("creating a namespace")
		ns = NewNamespace(utils.GetUniqueName("test-ns-"))
		Expect(CreateNamespace(ctx, k8sClient, ns)).NotTo(HaveOccurred())

		By("creating a PVC")
		pvc, err = NewPVC(ns.Name, utils.GetUniqueName("test-pvc-"), "Block", rookStorageClass, "ReadWriteOnce", "100Mi")
		Expect(err).NotTo(HaveOccurred())
		Expect(CreatePVC(ctx, k8sClient, pvc)).NotTo(HaveOccurred())

		By("creating a Pod")
		devicePath = "/data"
		podForPVC, err = NewPod(ns.Name, utils.GetUniqueName("test-pod-"),
			pvc.Name, "ghcr.io/cybozu/ubuntu:24.04", devicePath)
		Expect(err).NotTo(HaveOccurred())
		Expect(CreatePod(ctx, k8sClient, podForPVC)).NotTo(HaveOccurred())
		Expect(WaitForPodReady(ctx, k8sClient, podForPVC, 2*time.Minute)).NotTo(HaveOccurred())

		By("writing data to the PVC")
		var stderr []byte
		_, stderr, err = kubectl("exec", "-n", ns.Name, podForPVC.Name, "--",
			"dd", "if=/dev/urandom", fmt.Sprintf("of=%s", devicePath), "bs=4K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("exec", "-n", ns.Name, podForPVC.Name, "--", "sync")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("reading the data from the PVC")
		dataOnFullBackup, stderr, err = kubectl("exec", "-n", ns.Name, podForPVC.Name, "--",
			"dd", fmt.Sprintf("if=%s", devicePath), "bs=4K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("creating a full backup")
		finbackup1, err = NewFinBackup(rookNamespace, utils.GetUniqueName("test-finbackup-"),
			pvc, "minikube-worker")
		Expect(err).NotTo(HaveOccurred())
		Expect(CreateFinBackup(ctx, ctrlClient, finbackup1)).NotTo(HaveOccurred())
		Expect(WaitForFinBackupStoredToNodeAndVerified(ctx, ctrlClient, finbackup1, 1*time.Minute)).
			NotTo(HaveOccurred())

		By("verifying the data in raw.img from the full backup")
		volumePath = filepath.Join("/fin", ns.Name, pvc.Name)
		// `--native-ssh=false` is used to avoid issues of conversion from LF to CRLF.
		var rawImageData []byte
		rawImageData, stderr, err = execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"dd", fmt.Sprintf("if=%s/raw.img", volumePath), "bs=4K", "count=1", "status=none")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(rawImageData).To(Equal(dataOnFullBackup), "Data in raw.img does not match the expected data")

		By("writing incremental data on the pvc")
		_, stderr, err = kubectl("exec", "-n", ns.Name, podForPVC.Name, "--",
			"dd", "if=/dev/urandom", fmt.Sprintf("of=%s", devicePath), "bs=4K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("exec", "-n", ns.Name, podForPVC.Name, "--", "sync")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
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
		Expect(DeletePod(ctx, k8sClient, podForPVC)).NotTo(HaveOccurred())
		Expect(DeletePVC(ctx, k8sClient, pvc)).NotTo(HaveOccurred())
		Expect(DeleteNamespace(ctx, k8sClient, ns)).NotTo(HaveOccurred())
	})

	// CSATEST-1619
	// Description:
	//	Creating FinBackup after a full backup should produce
	//	an incremental backup.
	//
	// Precondition (set by BeforeAll):
	//  - An RBD PVC is created on the Ceph cluster.
	//  - Random data is written to the RBD PVC.
	//  - FinBackup1 that references the PVC
	//    and has the "StoredToNode" condition set to "True" is created.
	//  - New random data is written to the RBD PVC.
	//
	// Act:
	//   - Create FinBackup2 for the incremental backup.
	//
	// Assert:
	//   - The FinBackup2's condition "StoredToNode" becomes "True".
	//   - On the Fin node for the FinBackup2, the backup directory has:
	//     1. raw.img with the data from step 2.
	//     2. A diff file under the diff directory for the incremental backup.
	It("should create an incremental backup", func(ctx SpecContext) {
		// Act
		By("creating an incremental backup")
		finbackup2, err = NewFinBackup(rookNamespace, utils.GetUniqueName("test-finbackup-"),
			pvc, "minikube-worker")
		Expect(err).NotTo(HaveOccurred())
		Expect(CreateFinBackup(ctx, ctrlClient, finbackup2)).NotTo(HaveOccurred())
		Expect(WaitForFinBackupStoredToNodeAndVerified(ctx, ctrlClient, finbackup2, 1*time.Minute)).
			NotTo(HaveOccurred())

		// Assert
		By("verifying the data in raw.img as full backup")
		var rawImageData, stderr []byte
		rawImageData, stderr, err = execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"dd", fmt.Sprintf("if=%s/raw.img", volumePath), "bs=4K", "count=1", "status=none")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(rawImageData).To(Equal(dataOnFullBackup), "Data in raw.img does not match the expected data")

		By("verifying the existence of the diff file")
		Expect(ctrlClient.Get(ctx, client.ObjectKeyFromObject(finbackup2), finbackup2)).NotTo(HaveOccurred())
		_, stderr, err = execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"ls", filepath.Join(volumePath, "diff", strconv.Itoa(*finbackup2.Status.SnapID), "part-0"))
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr), "diff file does not exist")
	})

	// CSATEST-1618
	// Description:
	//   Restore from incremental backup with no error.
	//
	// Precondition:
	//   - An RBD PVC exists. The first 4KiB of this PVC is filled with the new random data.
	//   - Two FinBackups corresponding to the RBD PVC exists
	//     for both full backup and incremental backup and are ready to use.
	//
	// Arrange:
	//   - Reading the data from the PVC.
	//
	// Act:
	//   Create FinRestore, referring the FinBackup corresponding to the incremental backup.
	//
	// Assert:
	//   - FinRestore becomes ready to use.
	//   - The first 4KiB of the restore PVC is filled with the same data
	//     as the first 4KiB of the PVC.
	//   - The size of the restore PVC is the same as the snapshot.
	It("should restore from incremental backup", func(ctx SpecContext) {
		// Arrange
		By("reading the data from the pvc")
		var stderr []byte
		dataOnIncrementalBackup, stderr, err = kubectl("exec", "-n", ns.Name, podForPVC.Name, "--",
			"dd", "if=/data", "bs=4K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		// Act
		By("restoring from the incremental backup")
		finRestoreName := utils.GetUniqueName("test-finrestore-")
		restore, err := NewFinRestore(
			finRestoreName, finbackup2, ns.Name, finRestoreName)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, restore)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DeleteFinRestoreAndRestorePVC(context.Background(), ctrlClient, k8sClient, restore)
		})
		err = WaitForFinRestoreReady(ctx, ctrlClient, restore, 10*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		// Assert
		VerifyDataInRestorePVC(ctx, k8sClient, restore, dataOnIncrementalBackup)
		VerifySizeOfRestorePVC(ctx, ctrlClient, restore, finbackup2)
	})

	// CSATEST-1617
	// Description:
	//   Restore from full backup with no error.
	//
	// Precondition:
	//   - An RBD PVC exists. The first 4KiB of this PVC is filled with the new random data.
	//   - Two FinBackup corresponding to the RBD PVC exists
	//     for both full backup and incremental backup and are ready to use.
	//
	// Arrange:
	//   - Noting.
	//
	// Act:
	//   Create FinRestore, referring to the full backup.
	//
	// Assert:
	//   - FinRestore becomes ready to use.
	//   - The first 4KiB of the restore PVC is filled with the same data
	//     as the first 4KiB of the PVC.
	//   - The size of the restore PVC is the same as the snapshot.
	It("should restore from full backup", func(ctx SpecContext) {
		// Act
		By("restoring from full backup")
		finRestoreName := utils.GetUniqueName("test-finrestore-")
		restore, err := NewFinRestore(
			finRestoreName, finbackup1, ns.Name, finRestoreName)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, restore)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DeleteFinRestoreAndRestorePVC(context.Background(), ctrlClient, k8sClient, restore)
		})
		err = WaitForFinRestoreReady(ctx, ctrlClient, restore, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		// Assert
		VerifyDataInRestorePVC(ctx, k8sClient, restore, dataOnFullBackup)
		VerifySizeOfRestorePVC(ctx, ctrlClient, restore, finbackup1)
	})
	// CSATEST-1605
	// Description:
	//   Delete a full backup with no error.
	//
	// Precondition:
	//   - An RBD PVC exists.
	//   - Two FinBackup corresponding to the RBD PVC exists
	//     for both full backup and incremental backup and are ready to use.
	//
	// Arrange:
	//   - Nothing
	//
	// Act:
	//   Delete FinBackup for full backup.
	//
	// Assert:
	//   - Deleted the FinBackup resource for full backup.
	//   - The FinBackup for incremental backup remains.
	//   - Updated the raw.img file.
	//   - Deleted the cleanup and deletion jobs.
	//   - Deleted the snapshot reference in FinBackup.
	It("should delete full backup", func(ctx SpecContext) {
		// Act
		By("deleting the backup")
		err = DeleteFinBackup(ctx, ctrlClient, finbackup1)
		Expect(err).NotTo(HaveOccurred())

		// Assert
		By("verifying the the deletion of incremental backup")
		err = WaitForFinBackupDeletion(ctx, ctrlClient, finbackup1, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the remaining of incremental backup")
		var dummy finv1.FinBackup
		err = ctrlClient.Get(ctx, client.ObjectKeyFromObject(finbackup2), &dummy)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the non-existence of the diff file")
		Expect(ctrlClient.Get(ctx, client.ObjectKeyFromObject(finbackup2), finbackup2)).NotTo(HaveOccurred())
		var stderr []byte
		_, stderr, err = execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"ls", filepath.Join(volumePath, "diff", strconv.Itoa(*finbackup2.Status.SnapID), "part-0"))
		Expect(err).To(HaveOccurred(), "stderr: "+string(stderr), "diff file does not exist")

		By("verifying the data in raw.img as incremental backup")
		var rawImageData []byte
		rawImageData, stderr, err = execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"dd", fmt.Sprintf("if=%s/raw.img", volumePath), "bs=4K", "count=1", "status=none")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(rawImageData).To(Equal(dataOnIncrementalBackup), "Data in raw.img does not match the expected data")

		By("verifying the deletion of jobs")
		err = WaitForJobDeletion(ctx, k8sClient, rookNamespace,
			fmt.Sprintf("fin-cleanup-%s", finbackup1.UID), 10*time.Second)
		Expect(err).NotTo(HaveOccurred(), "Cleanup job should be deleted.")
		err = WaitForJobDeletion(ctx, k8sClient, rookNamespace,
			fmt.Sprintf("fin-deletion-%s", finbackup1.UID), 10*time.Second)
		Expect(err).NotTo(HaveOccurred(), "Deletion job should be deleted.")

		By("verifying the deletion of snapshot reference in FinBackup")
		rbdImage := finbackup1.Annotations["fin.cybozu.io/backup-target-rbd-image"]
		var stdout []byte
		stdout, stderr, err = kubectl("exec", "-n", rookNamespace, "deploy/rook-ceph-tools", "--",
			"rbd", "info", fmt.Sprintf("%s/%s@fin-backup-%s", poolName, rbdImage, finbackup1.UID))
		Expect(err).To(HaveOccurred(), "Snapshot should be deleted. stdout: %s, stderr: %s", stdout, stderr)
	})

	// Description:
	//   Restore from the remaining backup with no error.
	//
	// Precondition:
	//   - An RBD PVC exists. The first 4KiB of this PVC is filled with random data.
	//   - A FinBackup corresponding to the RBD PVC exists and is ready to use.
	//
	// Arrange:
	//   - Nothing
	//
	// Act:
	//   Create FinRestore, referring the remaining FinBackup.
	//
	// Assert:
	//   - FinRestore becomes ready to use.
	//   - The first 4KiB of the restore PVC is filled with the same data
	//     as the first 4KiB of the PVC.
	//   - The size of the restore PVC is the same as the snapshot.
	It("should restore from the remaining backup", func(ctx SpecContext) {
		// Act
		By("restoring from the remaining backup")
		finRestoreName := utils.GetUniqueName("test-finrestore-")
		restore, err := NewFinRestore(
			finRestoreName, finbackup2, ns.Name, finRestoreName)
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinRestore(ctx, ctrlClient, restore)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DeleteFinRestoreAndRestorePVC(context.Background(), ctrlClient, k8sClient, restore)
		})
		err = WaitForFinRestoreReady(ctx, ctrlClient, restore, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())

		// Assert
		VerifyDataInRestorePVC(ctx, k8sClient, restore, dataOnIncrementalBackup)
		VerifySizeOfRestorePVC(ctx, ctrlClient, restore, finbackup2)
	})
}

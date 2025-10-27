package e2e

import (
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

// CSATEST-1619
// Description:
//
//	Creating a FinBackup after a full backup should produce
//	an incremental backup.
//
// Arrange:
//  1. An RBD PVC is created on the Ceph cluster.
//  2. Random data is written to the RBD PVC.
//  3. A FinBackup1 that references the PVC
//     and has the "StoredToNode" condition set to "True" is created.
//  4. New random data is written to the RBD PVC.
//
// Act:
//   - Create FinBackup2 for the incremental backup.
//
// Assert:
//   - The FinBackup2's condition "StoredToNode" becomes "True".
//   - On the Fin node for the FinBackup2, the backup directory has:
//     1. raw.img with the data from step 2.
//     2. A diff file under the diff directory for the incremental backup.
func incrementalBackupTestSuite() {
	var ns *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var pod *corev1.Pod
	var finbackup1 *finv1.FinBackup
	var finbackup2 *finv1.FinBackup
	var fullBackupData []byte
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
		pod, err = NewPod(ns.Name, utils.GetUniqueName("test-pod-"), pvc.Name, "ghcr.io/cybozu/ubuntu:24.04", devicePath)
		Expect(err).NotTo(HaveOccurred())
		Expect(CreatePod(ctx, k8sClient, pod)).NotTo(HaveOccurred())
		Expect(WaitForPodReady(ctx, k8sClient, pod, 2*time.Minute)).NotTo(HaveOccurred())

		By("writing data to the PVC")
		_, stderr, err := kubectl("exec", "-n", ns.Name, pod.Name, "--",
			"dd", "if=/dev/urandom", fmt.Sprintf("of=%s", devicePath), "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("exec", "-n", ns.Name, pod.Name, "--", "sync")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("reading the data from the PVC")
		expectedWrittenData, stderr, err := kubectl("exec", "-n", ns.Name, pod.Name, "--",
			"dd", fmt.Sprintf("if=%s", devicePath), "bs=1K", "count=1")
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
		fullBackupData, stderr, err = execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"dd", fmt.Sprintf("if=%s/raw.img", volumePath), "bs=1K", "count=1", "status=none")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(fullBackupData).To(Equal(expectedWrittenData), "Data in raw.img does not match the expected data")

		By("writing incremental data on the pvc")
		_, stderr, err = kubectl("exec", "-n", ns.Name, pod.Name, "--",
			"dd", "if=/dev/urandom", fmt.Sprintf("of=%s", devicePath), "bs=1K", "count=1")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		_, stderr, err = kubectl("exec", "-n", ns.Name, pod.Name, "--", "sync")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
	})

	AfterAll(func(ctx SpecContext) {
		for _, fb := range []*finv1.FinBackup{finbackup1, finbackup2} {
			if fb == nil {
				continue
			}
			Expect(DeleteFinBackup(ctx, ctrlClient, fb)).NotTo(HaveOccurred())
			Expect(WaitForFinBackupDeletion(ctx, ctrlClient, fb, 2*time.Minute)).NotTo(HaveOccurred())
		}
		Expect(DeletePod(ctx, k8sClient, pod)).NotTo(HaveOccurred())
		Expect(DeletePVC(ctx, k8sClient, pvc)).NotTo(HaveOccurred())
		Expect(DeleteNamespace(ctx, k8sClient, ns)).NotTo(HaveOccurred())
	})

	It("should create an incremental backup", func(ctx SpecContext) {
		By("creating an incremental backup")
		finbackup2, err := NewFinBackup(rookNamespace, utils.GetUniqueName("test-finbackup-"),
			pvc, "minikube-worker")
		Expect(err).NotTo(HaveOccurred())
		Expect(CreateFinBackup(ctx, ctrlClient, finbackup2)).NotTo(HaveOccurred())
		Expect(WaitForFinBackupStoredToNodeAndVerified(ctx, ctrlClient, finbackup2, 1*time.Minute)).
			NotTo(HaveOccurred())

		By("verifying the data in raw.img as full backup")
		afterFullBackupData, stderr, err := execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"dd", fmt.Sprintf("if=%s/raw.img", volumePath), "bs=1K", "count=1", "status=none")
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
		Expect(afterFullBackupData).To(Equal(fullBackupData), "Data in raw.img does not match the expected data")

		By("verifying the existence of the diff file")
		Expect(ctrlClient.Get(ctx, client.ObjectKeyFromObject(finbackup2), finbackup2)).NotTo(HaveOccurred())
		_, stderr, err = execWrapper(minikube, nil, "ssh", "--native-ssh=false", "--",
			"ls", filepath.Join(volumePath, "diff", strconv.Itoa(*finbackup2.Status.SnapID), "part-0"))
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr), "diff file does not exist")
	})

	// TODO: CSATEST-1618: Verify the data in the incremental backup
}

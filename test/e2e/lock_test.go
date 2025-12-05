package e2e

import (
	"encoding/json"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func lockTestSuite() {
	var ns *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var fb *finv1.FinBackup
	dummyLockID := "dummy-lock-id"
	var poolName, imageName string

	It("should setup environment", func(ctx SpecContext) {
		ns = NewNamespace(utils.GetUniqueName("ns-"))
		err := CreateNamespace(ctx, k8sClient, ns)
		Expect(err).NotTo(HaveOccurred())

		pvc = CreateBackupTargetPVC(ctx, k8sClient, ns, "Filesystem", rookStorageClass, "ReadWriteOnce", "100Mi")
		// Create a Pod to make filesystem to pass verification during backup
		_ = CreatePodForFilesystemPVC(ctx, k8sClient, pvc)
	})

	It("should lock the volume", func(ctx SpecContext) {
		// get current pvc to find pool and image name
		var err error
		pvc, err = k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		pv, err := GetPvByPvc(ctx, k8sClient, pvc)
		Expect(err).NotTo(HaveOccurred())
		poolName = pv.Spec.CSI.VolumeAttributes["pool"]
		imageName = pv.Spec.CSI.VolumeAttributes["imageName"]

		// locked
		_, _, err = kubectl("exec", "-n", rookNamespace, "deployment/"+finDeploymentName, "--",
			"rbd", "-p", poolName, "lock", "add", imageName, dummyLockID)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should create backup and wait for the log", func(ctx SpecContext) {
		var err error
		fb, err = NewFinBackup(rookNamespace, utils.GetUniqueName("fb-"), pvc, nodes[0])
		Expect(err).NotTo(HaveOccurred())
		err = CreateFinBackup(ctx, ctrlClient, fb)
		Expect(err).NotTo(HaveOccurred())

		// get current fb to get UID
		currentFB := &finv1.FinBackup{}
		err = ctrlClient.Get(ctx, client.ObjectKeyFromObject(fb), currentFB)
		Expect(err).NotTo(HaveOccurred())

		err = WaitControllerLog(ctx,
			"the volume is locked by another process.*"+string(currentFB.GetUID()),
			3*time.Minute)
		Expect(err).NotTo(HaveOccurred())
	})

	It("checks that the snapshot is not created", func(ctx SpecContext) {
		snapshots, err := ListRBDSnapshots(ctx, poolName, imageName)
		Expect(err).NotTo(HaveOccurred())
		Expect(snapshots).To(BeEmpty())
	})

	It("should unlock the volume", func() {
		stdout, _, err := kubectl("exec", "-n", rookNamespace, "deployment/"+finDeploymentName, "--",
			"rbd", "-p", poolName, "--format", "json", "lock", "ls", imageName)
		Expect(err).NotTo(HaveOccurred())
		var locks []*model.RBDLock
		err = json.Unmarshal(stdout, &locks)
		Expect(err).NotTo(HaveOccurred())
		Expect(locks).To(HaveLen(1))

		// unlock
		_, _, err = kubectl("exec", "-n", rookNamespace, "deployment/"+finDeploymentName, "--",
			"rbd", "-p", poolName, "lock", "rm", imageName, dummyLockID, locks[0].Locker)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should resume backup creation and complete it", func(ctx SpecContext) {
		_, err := WaitForFinBackupStoredToNodeAndVerified(ctx, ctrlClient, fb, 1*time.Minute)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should not exist locks after backup completion", func() {
		stdout, _, err := kubectl("exec", "-n", rookNamespace, "deployment/"+finDeploymentName, "--",
			"rbd", "-p", poolName, "--format", "json", "lock", "ls", imageName)
		Expect(err).NotTo(HaveOccurred())
		var locks []*model.RBDLock
		err = json.Unmarshal(stdout, &locks)
		Expect(err).NotTo(HaveOccurred())
		Expect(locks).To(HaveLen(0))
	})
}

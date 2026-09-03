package e2e

import (
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func nodeDeletionTestSuite() {
	var ns *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim

	BeforeAll(func(ctx SpecContext) {
		By("creating a namespace and a backup target PVC")
		ns = NewNamespace(utils.GetUniqueName("test-ns-"))
		Expect(CreateNamespace(ctx, k8sClient, ns)).NotTo(HaveOccurred())
		pvc = CreateBackupTargetPVC(ctx, k8sClient, ns, "Block", rookStorageClass, "ReadWriteOnce", "100Mi")
	})

	AfterAll(func(ctx SpecContext) {
		By("deleting the remaining resources")
		_ = DeletePVC(ctx, k8sClient, pvc)
		Expect(DeleteNamespace(ctx, k8sClient, ns)).NotTo(HaveOccurred())
	})

	// Description:
	//   A FinBackup is owned by the node holding its data.
	//
	// Act:
	//   - Take a backup on a live node.
	//
	// Assert:
	//   - The FinBackup refers to that node as its owner, by name and by UID.
	It("should be owned by its backup destination node", func(ctx SpecContext) {
		finbackup := CreateBackup(ctx, ctrlClient, rookNamespace, pvc, nodes[0])
		DeferCleanup(func(ctx SpecContext) {
			_ = DeleteFinBackup(ctx, ctrlClient, finbackup)
			Expect(WaitForFinBackupDeletion(ctx, ctrlClient, finbackup, 2*time.Minute)).NotTo(HaveOccurred())
		})

		By("reading the node the backup was stored to")
		node, err := k8sClient.CoreV1().Nodes().Get(ctx, nodes[0], metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("checking the owner reference points at that node")
		var got finv1.FinBackup
		Expect(ctrlClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &got)).NotTo(HaveOccurred())
		Expect(got.OwnerReferences).To(HaveLen(1))
		Expect(got.OwnerReferences[0].APIVersion).To(Equal("v1"))
		Expect(got.OwnerReferences[0].Kind).To(Equal("Node"))
		Expect(got.OwnerReferences[0].Name).To(Equal(nodes[0]))
		Expect(got.OwnerReferences[0].UID).To(Equal(node.UID))
		Expect(got.OwnerReferences[0].BlockOwnerDeletion).To(HaveValue(BeFalse()))
	})

	// Description:
	//   Deleting the Node resource removes the FinBackups stored on it.
	//
	// Arrange:
	//   - Create a Node resource with no machine behind it, so that deleting it does
	//     not disturb the cluster the other scenarios run on.
	//   - Create a FinBackup pointing at it and wait for the owner reference.
	//
	// Act:
	//   - Delete the Node resource.
	//
	// Assert:
	//   - The FinBackup disappears even though nothing completed its jobs.
	It("should be removed when its backup destination node is deleted", func(ctx SpecContext) {
		nodeName := utils.GetUniqueName("test-node-")

		By("creating a Node resource with no machine behind it")
		_, err := k8sClient.CoreV1().Nodes().Create(ctx,
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			err := k8sClient.CoreV1().Nodes().Delete(ctx, nodeName, metav1.DeleteOptions{})
			if err != nil && !k8serrors.IsNotFound(err) {
				Expect(err).NotTo(HaveOccurred())
			}
		})

		By("creating a FinBackup on that node")
		finbackup, err := NewFinBackup(rookNamespace, utils.GetUniqueName("test-finbackup-"), pvc, nodeName)
		Expect(err).NotTo(HaveOccurred())
		Expect(CreateFinBackup(ctx, ctrlClient, finbackup)).NotTo(HaveOccurred())

		By("waiting for the owner reference to be set")
		Eventually(func(g Gomega, ctx SpecContext) {
			var got finv1.FinBackup
			g.Expect(ctrlClient.Get(ctx, client.ObjectKeyFromObject(finbackup), &got)).NotTo(HaveOccurred())
			g.Expect(got.OwnerReferences).To(HaveLen(1))
		}, "1m", "1s").WithContext(ctx).Should(Succeed())

		By("deleting the Node resource")
		Expect(k8sClient.CoreV1().Nodes().Delete(ctx, nodeName, metav1.DeleteOptions{})).NotTo(HaveOccurred())

		By("checking the FinBackup is removed without anyone completing its jobs")
		Expect(WaitForFinBackupDeletion(ctx, ctrlClient, finbackup, 2*time.Minute)).NotTo(HaveOccurred())
	})
}

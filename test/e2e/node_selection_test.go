package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// The default --node-selector of the controller selects the nodes with this label.
	finNodeLabelKey   = "csa.cybozu.io/reserved-for"
	finNodeLabelValue = "fin"

	// fillerPath is a file taking up the fin volume to control the free space of a node.
	fillerPath = "/fin/node-selection-filler"

	// The controller requeues an unassigned FinBackupConfig every minute, and the free
	// space reaches it through the scrape interval of Prometheus and its cache.
	nodeSelectionTimeout = 3 * time.Minute
)

func setFinNodeLabel(ctx context.Context, node string, labeled bool) {
	GinkgoHelper()
	var value any
	if labeled {
		value = finNodeLabelValue
	}
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"labels": map[string]any{finNodeLabelKey: value}},
	})
	Expect(err).NotTo(HaveOccurred())
	_, err = k8sClient.CoreV1().Nodes().Patch(ctx, node, types.MergePatchType, patch, metav1.PatchOptions{})
	Expect(err).NotTo(HaveOccurred())
}

// queryFinVolumeFreeSpace asks Prometheus, through the API server proxy, the free space
// of the fin volume on each node, as the controller does.
func queryFinVolumeFreeSpace(ctx context.Context) (map[string]float64, error) {
	body, err := k8sClient.CoreV1().Services("monitoring").
		ProxyGet("http", "prometheus-k8s", "9090", "api/v1/query",
			map[string]string{"query": `node_filesystem_avail_bytes{mountpoint="/fin"}`}).
		DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	freeSpace := map[string]float64{}
	for _, r := range resp.Data.Result {
		s, ok := r.Value[1].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected sample value %v", r.Value[1])
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		freeSpace[r.Metric["instance"]] = v
	}
	return freeSpace, nil
}

func nodeSelectionTestSuite() {
	var finNS, pvcNS *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var fbc *finv1.FinBackupConfig
	var cj *batchv1.CronJob
	var finbackups []*finv1.FinBackup

	BeforeAll(func(ctx SpecContext) {
		By("creating namespaces and a backup target PVC")
		pvcNS = NewNamespace(utils.GetUniqueName("node-selection-pvc-"))
		Expect(CreateNamespace(ctx, k8sClient, pvcNS)).NotTo(HaveOccurred())
		pvc = CreateBackupTargetPVC(ctx, k8sClient, pvcNS, "Block", rookStorageClass, "ReadWriteOnce", "100Mi")

		finNS = NewNamespace(utils.GetUniqueName("node-selection-fin-"))
		Expect(CreateNamespace(ctx, k8sClient, finNS)).NotTo(HaveOccurred())
		CopyFBCServiceAccount(ctx, k8sClient, rookNamespace, finNS.Name)

		// The node labels and the filler change the whole cluster, so restore them even
		// when a spec fails, not to break the other scenarios.
		DeferCleanup(func(ctx SpecContext) {
			for _, node := range nodes {
				setFinNodeLabel(ctx, node, false)
				_, _, _ = minikubeSSH(node, nil, "sudo", "rm", "-f", fillerPath)
			}
		})

		By("labeling the nodes for fin")
		for _, node := range nodes {
			setFinNodeLabel(ctx, node, true)
		}

		By("taking up the fin volume of nodes[0] so that nodes[1] has more free space")
		_, stderr, err := minikubeSSH(nodes[0], nil, "sudo", "fallocate", "-l", "500M", fillerPath)
		Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

		By("waiting for Prometheus to report the free space")
		Eventually(func(g Gomega) {
			freeSpace, err := queryFinVolumeFreeSpace(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(freeSpace).To(HaveKey(nodes[0]))
			g.Expect(freeSpace).To(HaveKey(nodes[1]))
			g.Expect(freeSpace[nodes[0]]).To(BeNumerically("<", freeSpace[nodes[1]]))
		}, "2m", "5s").Should(Succeed())
	})

	AfterAll(func(ctx SpecContext) {
		By("cleaning up resources")
		for _, fb := range finbackups {
			_ = DeleteFinBackup(ctx, ctrlClient, fb)
			_ = WaitForFinBackupDeletion(ctx, ctrlClient, fb, 30*time.Second)
		}
		if fbc != nil {
			opt := &client.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationForeground)}
			Expect(client.IgnoreNotFound(ctrlClient.Delete(ctx, fbc, opt))).NotTo(HaveOccurred())
		}
		Expect(DeletePVC(ctx, k8sClient, pvc)).NotTo(HaveOccurred())
		Expect(DeleteNamespace(ctx, k8sClient, finNS)).NotTo(HaveOccurred())
		Expect(DeleteNamespace(ctx, k8sClient, pvcNS)).NotTo(HaveOccurred())
	})

	// Description:
	//   A FinBackupConfig without spec.node is assigned the fin node with the most free
	//   space, read from Prometheus, and its backups are taken there.
	//
	// Arrange:
	//   - Both nodes are labeled for fin, and nodes[1] has more free space.
	//
	// Act:
	//   - Create a FinBackupConfig without spec.node and a backup from its CronJob.
	//
	// Assert:
	//   - status.node is nodes[1], and the FinBackup is taken on nodes[1].
	It("should assign the node with the most free space", func(ctx SpecContext) {
		By("creating a FinBackupConfig without spec.node")
		var err error
		fbc, err = NewFinBackupConfig(finNS.Name, utils.GetUniqueName("test-fbc-"), pvc, "", "0 0 * * *")
		Expect(err).NotTo(HaveOccurred())
		fbc.Spec.Suspend = true
		Expect(ctrlClient.Create(ctx, fbc)).NotTo(HaveOccurred())

		By("waiting for nodes[1] to be assigned")
		WaitForFinBackupConfigStatusNode(ctx, ctrlClient, fbc, nodes[1], nodeSelectionTimeout)

		By("waiting for the CronJob")
		cj = &batchv1.CronJob{}
		Eventually(func(g Gomega) {
			key := client.ObjectKey{Namespace: fbc.Namespace, Name: fmt.Sprintf("fbc-%s", fbc.UID)}
			g.Expect(ctrlClient.Get(ctx, key, cj)).NotTo(HaveOccurred())
		}, "60s", "1s").Should(Succeed())

		By("taking a backup on nodes[1]")
		jobName, err := CreateJobFromCronJob(ctx, cj)
		Expect(err).NotTo(HaveOccurred())
		fb := WaitForFinBackupVerifiedFromJobName(ctx, ctrlClient, fbc, jobName, 60*time.Second)
		finbackups = append(finbackups, fb)
		Expect(fb.Spec.Node).To(Equal(nodes[1]))
	})

	// Description:
	//   Removing the label from a node, as when retiring it, moves the backups of its
	//   FinBackupConfigs to another node in advance.
	//
	// Act:
	//   - Remove the label from nodes[1] and take a backup.
	//
	// Assert:
	//   - status.node becomes nodes[0], and the FinBackup is taken on nodes[0].
	//   - The older FinBackup on nodes[1] is deleted once the new one is verified.
	It("should move the backups when the label is removed from the node", func(ctx SpecContext) {
		oldFB := finbackups[len(finbackups)-1]

		By("removing the label from nodes[1]")
		setFinNodeLabel(ctx, nodes[1], false)

		By("waiting for nodes[0] to be assigned")
		WaitForFinBackupConfigStatusNode(ctx, ctrlClient, fbc, nodes[0], nodeSelectionTimeout)

		By("taking a backup on nodes[0]")
		jobName, err := CreateJobFromCronJob(ctx, cj)
		Expect(err).NotTo(HaveOccurred())
		fb := WaitForFinBackupVerifiedFromJobName(ctx, ctrlClient, fbc, jobName, 60*time.Second)
		finbackups = append(finbackups, fb)
		Expect(fb.Spec.Node).To(Equal(nodes[0]))

		By("the older FinBackup on nodes[1] should be auto-deleted")
		WaitForFinBackupNotFound(ctx, ctrlClient, oldFB, 60*time.Second)
	})

	// Description:
	//   When no node is available, the backups stop so that administrators notice it, and
	//   they resume once a node becomes available.
	//
	// Act:
	//   1. Remove the label from every node and run the CronJob.
	//   2. Label the nodes again and run the CronJob.
	//
	// Assert:
	//   1. status.node is cleared, and the Job fails without creating a FinBackup.
	//   2. A node is assigned again, and a FinBackup is taken on it.
	It("should stop the backups while no node is available and resume them", func(ctx SpecContext) {
		By("removing the label from every node")
		for _, node := range nodes {
			setFinNodeLabel(ctx, node, false)
		}

		By("waiting for status.node to be cleared")
		WaitForFinBackupConfigStatusNode(ctx, ctrlClient, fbc, "", nodeSelectionTimeout)

		By("running the CronJob, which should fail without creating a FinBackup")
		jobName, err := CreateJobFromCronJob(ctx, cj)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			pods, err := k8sClient.CoreV1().Pods(fbc.Namespace).List(ctx, metav1.ListOptions{
				LabelSelector: "batch.kubernetes.io/job-name=" + jobName,
			})
			g.Expect(err).NotTo(HaveOccurred())
			failed := false
			for _, pod := range pods.Items {
				for _, cs := range pod.Status.ContainerStatuses {
					if cs.RestartCount > 0 || (cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0) {
						failed = true
					}
				}
			}
			g.Expect(failed).To(BeTrue())
		}, "2m", "2s").Should(Succeed())
		_, err = GetFinBackupNameFromJobName(ctx, ctrlClient, string(fbc.UID), jobName)
		Expect(err).To(HaveOccurred())

		// The Job retries with growing delays. Delete it so that its retry does not create
		// a FinBackup alongside the one checked below.
		Expect(k8sClient.BatchV1().Jobs(fbc.Namespace).Delete(ctx, jobName, metav1.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationForeground),
		})).NotTo(HaveOccurred())
		Expect(WaitForJobDeletion(ctx, k8sClient, fbc.Namespace, jobName, 2*time.Minute)).NotTo(HaveOccurred())

		By("labeling the nodes again")
		for _, node := range nodes {
			setFinNodeLabel(ctx, node, true)
		}

		By("waiting for a node to be assigned")
		var node string
		Eventually(func(g Gomega) {
			updated := &finv1.FinBackupConfig{}
			g.Expect(ctrlClient.Get(ctx, client.ObjectKeyFromObject(fbc), updated)).NotTo(HaveOccurred())
			node = updated.Status.Node
			g.Expect(node).NotTo(BeEmpty())
		}, nodeSelectionTimeout, "2s").Should(Succeed())

		By("taking a backup on the assigned node")
		jobName, err = CreateJobFromCronJob(ctx, cj)
		Expect(err).NotTo(HaveOccurred())
		fb := WaitForFinBackupVerifiedFromJobName(ctx, ctrlClient, fbc, jobName, 60*time.Second)
		finbackups = append(finbackups, fb)
		Expect(fb.Spec.Node).To(Equal(node))
	})
}

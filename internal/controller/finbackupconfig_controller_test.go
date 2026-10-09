package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/infrastructure/fake"
	"github.com/cybozu-go/fin/internal/pkg/metrics"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// newTestFinBackupConfigReconciler assigns test-node to a FinBackupConfig without spec.node.
func newTestFinBackupConfigReconciler(overwriteFBCSchedule string) *FinBackupConfigReconciler {
	GinkgoHelper()
	selector, err := ParseNodeSelectorTerms(
		`[{"matchFields":[{"key":"metadata.name","operator":"In","values":["test-node"]}]}]`)
	Expect(err).NotTo(HaveOccurred())
	return NewFinBackupConfigReconciler(
		k8sClient,
		scheme.Scheme,
		overwriteFBCSchedule,
		cephClusterID,
		"test:latest",
		"test-sa",
		selector,
		fake.NewNodeFreeSpaceRepository(map[string]float64{"test-node": 1 << 30}),
		resource.MustParse("1Mi"),
	)
}

func findEnvVar(envVars []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range envVars {
		if envVars[i].Name == name {
			return &envVars[i]
		}
	}
	return nil
}

var _ = Describe("FinBackupConfig Controller under Manager", Ordered, func() {
	var fbcNS *corev1.Namespace
	var pvc *corev1.PersistentVolumeClaim
	var pv *corev1.PersistentVolume
	var reconciler *FinBackupConfigReconciler
	var stopFunc context.CancelFunc

	BeforeAll(func() {
		ctx := context.Background()
		fbcNS = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: utils.GetUniqueName("fbc-namespace")}}
		Expect(k8sClient.Create(ctx, fbcNS)).NotTo(HaveOccurred())

		pvc, pv = NewPVCAndPV(normalSC, userNamespace, "test-pvc", "test-pv", rbdImageName)
		err := k8sClient.Create(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())
		err = k8sClient.Create(ctx, pv)
		Expect(err).NotTo(HaveOccurred())

		reconciler = newTestFinBackupConfigReconciler("")

		mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme.Scheme})
		Expect(err).ToNot(HaveOccurred())
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

	AfterAll(func(ctx SpecContext) {
		stopFunc()
		Expect(k8sClient.Delete(ctx, fbcNS)).To(Succeed())
		DeletePVCAndPV(ctx, userNamespace, pvc.Name)
	})

	It("should delete FinBackupConfig", func(ctx SpecContext) {
		fbc := &finv1.FinBackupConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-fbc",
				Namespace: fbcNS.Name,
			},
			Spec: finv1.FinBackupConfigSpec{
				PVCNamespace: pvc.Namespace,
				PVC:          pvc.Name,
				Schedule:     "0 2 * * *",
				Suspend:      false,
			},
		}
		By("creating FinBackupConfig")
		Expect(k8sClient.Create(ctx, fbc)).NotTo(HaveOccurred())

		By("waiting for reconcile to add finalizer")
		Eventually(func(g Gomega) {
			updated := &finv1.FinBackupConfig{}
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fbc), updated)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(updated.Finalizers).To(ContainElement(finBackupConfigFinalizerName))
		}, "5s", "1s").Should(Succeed())

		By("deleting FinBackupConfig successfully")
		Expect(k8sClient.Delete(ctx, fbc)).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fbc), &finv1.FinBackupConfig{})
			g.Expect(k8serrors.IsNotFound(err)).To(BeTrue())
		}, "5s", "1s").Should(Succeed())
	})
})

var _ = Describe("FinBackupConfig Controller", func() {
	var fbcNamespace string

	BeforeEach(func(ctx SpecContext) {
		fbcNamespace = utils.GetUniqueName("fbc-namespace")

		fbcNSObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fbcNamespace}}
		Expect(k8sClient.Create(ctx, fbcNSObj)).NotTo(HaveOccurred())
	})

	AfterEach(func(ctx SpecContext) {
		fbcNSObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fbcNamespace}}
		_ = k8sClient.Delete(ctx, fbcNSObj)
	})

	Describe("CronJob creation", func() {
		It("should create a CronJob when FBC is created", func(ctx SpecContext) {
			// Description:
			//   Verify a CronJob is correctly generated from a FinBackupConfig.
			//
			// Arrange:
			//   - Prepare an RBD StorageClass, PVC/PV, controller Pod, and env vars.
			//   - Create a FinBackupConfig.
			//
			// Act:
			//   - Call Reconcile().
			//
			// Assert:
			//   - No error is returned.
			//   - CronJob's schedule, owner, JobTemplate and container settings are as expected.

			// Arrange
			reconciler := newTestFinBackupConfigReconciler("")

			By("creating PVC and PV using the StorageClass")
			pvc, pv := NewPVCAndPV(normalSC, userNamespace, "test-pvc", "test-pv", rbdImageName)
			err := k8sClient.Create(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Create(ctx, pv)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name)
			}()

			By("creating FinBackupConfig in a different namespace from the controller Pod")

			fbc := &finv1.FinBackupConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-fbc",
					Namespace: fbcNamespace,
				},
				Spec: finv1.FinBackupConfigSpec{
					PVCNamespace: pvc.Namespace,
					PVC:          pvc.Name,
					Schedule:     "0 2 * * *",
					Suspend:      false,
				},
			}
			err = k8sClient.Create(ctx, fbc)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, fbc) }()

			// Act
			By("triggering reconcile")
			req := ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(fbc),
			}
			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Assert
			By("verifying CronJob was created with correct specifications")
			cronJobName := "fbc-" + string(fbc.UID)
			cronJob := &batchv1.CronJob{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: cronJobName, Namespace: fbc.Namespace}, cronJob)
			}, "5s", "1s").Should(Succeed())

			Expect(cronJob.Spec.Schedule).To(Equal("0 2 * * *"))
			Expect(*cronJob.Spec.Suspend).To(Equal(false))
			Expect(*cronJob.Spec.StartingDeadlineSeconds).To(Equal(int64(3600)))
			Expect(cronJob.Spec.ConcurrencyPolicy).To(Equal(batchv1.ForbidConcurrent))
			Expect(*cronJob.Spec.JobTemplate.Spec.BackoffLimit).To(Equal(int32(65535)))

			By("verifying container configuration")
			podSpec := cronJob.Spec.JobTemplate.Spec.Template.Spec
			Expect(podSpec.ServiceAccountName).To(Equal("test-sa"))
			Expect(podSpec.RestartPolicy).To(Equal(corev1.RestartPolicyOnFailure))
			Expect(len(podSpec.Containers)).To(Equal(1))

			container := podSpec.Containers[0]
			Expect(container.Name).To(Equal("create-finbackup-job"))
			Expect(container.Image).To(Equal("test:latest"))
			Expect(container.Command).To(Equal([]string{
				"/manager",
				"create-finbackup-job",
				"--fin-backup-config-name=" + fbc.Name,
				"--fin-backup-config-namespace=" + fbc.Namespace,
			}))

			By("verifying environment variables are set via Downward API")
			jobNameEnv := findEnvVar(container.Env, "JOB_NAME")
			Expect(jobNameEnv).NotTo(BeNil())
			Expect(jobNameEnv.ValueFrom.FieldRef.FieldPath).To(Equal("metadata.labels['batch.kubernetes.io/job-name']"))

			podNsEnv := findEnvVar(container.Env, "POD_NAMESPACE")
			Expect(podNsEnv).NotTo(BeNil())
			Expect(podNsEnv.ValueFrom.FieldRef.FieldPath).To(Equal("metadata.namespace"))

			By("verifying owner reference is set")
			ownerRefs := cronJob.GetOwnerReferences()
			Expect(len(ownerRefs)).To(Equal(1))
			Expect(ownerRefs[0].UID).To(Equal(fbc.UID))
			Expect(ownerRefs[0].Kind).To(Equal("FinBackupConfig"))
			Expect(*ownerRefs[0].Controller).To(BeTrue())
		})

		It("should not create CronJob when StorageClass clusterID mismatches", func(ctx SpecContext) {
			// Description:
			//   Verify that no CronJob is created when the StorageClass clusterID does not match.
			//
			// Arrange:
			//   - Prepare an RBD StorageClass (with different clusterID) and its PVC/PV.
			//   - Prepare a controller Pod and env vars.
			//   - Create a FinBackupConfig.
			//
			// Act:
			//   - Call Reconcile().
			//
			// Assert:
			//   - CronJob is not created.

			// Arrange
			reconciler := newTestFinBackupConfigReconciler("")

			pvc, pv := NewPVCAndPV(otherSC, userNamespace, "pvc-mismatch", "pv-mismatch", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).NotTo(HaveOccurred())
			Expect(k8sClient.Create(ctx, pv)).NotTo(HaveOccurred())
			defer func() { DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name) }()

			fbc := &finv1.FinBackupConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "fbc-mismatch", Namespace: fbcNamespace},
				Spec: finv1.FinBackupConfigSpec{
					PVCNamespace: pvc.Namespace,
					PVC:          pvc.Name,
					Schedule:     "0 2 * * *",
					Suspend:      false,
				},
			}
			Expect(k8sClient.Create(ctx, fbc)).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, fbc) }()

			// Act
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fbc)}
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Assert
			cronJob := &batchv1.CronJob{}
			cronJobName := "fbc-" + string(fbc.UID)
			Consistently(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: cronJobName, Namespace: fbc.Namespace}, cronJob)
			}, 3*time.Second).ShouldNot(Succeed())
		})

		It("should use overwriteFBCSchedule when reconciler is initialized with it", func(ctx SpecContext) {
			// Description:
			//   Verify that the reconciler's overwriteFBCSchedule takes precedence for CronJob schedule.
			//
			// Arrange:
			//   - Create a Reconciler with overwriteFBCSchedule set.
			//   - Prepare an RBD StorageClass, PVC/PV, controller Pod and env vars.
			//   - Create a FinBackupConfig.
			//
			// Act:
			//   - Call Reconcile() on the local reconciler.
			//
			// Assert:
			//   - CronJob schedule is the overwriteFBCSchedule value.

			// Arrange
			reconciler := newTestFinBackupConfigReconciler("15 3 * * *")

			pvc, pv := NewPVCAndPV(normalSC, userNamespace, "pvc-overwrite", "pv-overwrite", rbdImageName)
			Expect(k8sClient.Create(ctx, pvc)).NotTo(HaveOccurred())
			Expect(k8sClient.Create(ctx, pv)).NotTo(HaveOccurred())
			defer func() { DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name) }()

			fbc := &finv1.FinBackupConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "fbc-overwrite", Namespace: fbcNamespace},
				Spec: finv1.FinBackupConfigSpec{
					PVCNamespace: pvc.Namespace,
					PVC:          pvc.Name,
					Schedule:     "0 2 * * *",
					Suspend:      false,
				},
			}
			Expect(k8sClient.Create(ctx, fbc)).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, fbc) }()

			// Act
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fbc)}
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Assert
			cronJob := &batchv1.CronJob{}
			cronJobName := "fbc-" + string(fbc.UID)
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: cronJobName, Namespace: fbc.Namespace}, cronJob)
			}, "5s", "1s").Should(Succeed())

			Expect(cronJob.Spec.Schedule).To(Equal("15 3 * * *"))
			Expect(cronJob.OwnerReferences).To(HaveLen(1))
			Expect(cronJob.OwnerReferences[0].Name).To(Equal(fbc.Name))
		})
	})
})

var _ = Describe("node ranking", func() {
	DescribeTable("should pick the node with the fewest FinBackupConfigs among the top quarter by free space",
		func(candidates []nodeCandidate, counts map[string]int, expected string) {
			Expect(fewestFinBackupConfigs(topByFreeSpace(candidates), counts)).To(Equal(expected))
		},
		Entry("a single node",
			[]nodeCandidate{{"a", 1}}, map[string]int{"a": 10}, "a"),
		Entry("the top quarter of three nodes is one node",
			[]nodeCandidate{{"a", 1}, {"b", 3}, {"c", 2}}, map[string]int{"b": 10}, "b"),
		Entry("the top quarter of five nodes is two nodes",
			[]nodeCandidate{{"a", 5}, {"b", 4}, {"c", 3}, {"d", 2}, {"e", 1}},
			map[string]int{"a": 2, "b": 1}, "b"),
		Entry("a node outside the top quarter is not picked even with fewer FinBackupConfigs",
			[]nodeCandidate{{"a", 8}, {"b", 7}, {"c", 6}, {"d", 5}, {"e", 4}, {"f", 3}, {"g", 2}, {"h", 1}},
			map[string]int{"a": 1, "b": 1}, "a"),
		Entry("equal free space is ordered by name",
			[]nodeCandidate{{"c", 1}, {"b", 1}, {"a", 1}, {"d", 1}}, map[string]int{}, "a"),
	)
})

var _ = Describe("FinBackupConfig node selection", func() {
	const (
		nodeLabelKey = "fin.cybozu.io/test-node-selection"
		gi           = float64(1 << 30)
	)
	var (
		fbcNamespace  string
		nodeLabel     string
		pvc           *corev1.PersistentVolumeClaim
		freeSpaceRepo *fake.NodeFreeSpaceRepository
		reconciler    *FinBackupConfigReconciler
	)

	createNode := func(ctx SpecContext, name string, labeled bool) {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if labeled {
			node.Labels = map[string]string{nodeLabelKey: nodeLabel}
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, node))).To(Succeed())
		})
	}

	createFBC := func(ctx SpecContext, name, specNode string) *finv1.FinBackupConfig {
		fbc := &finv1.FinBackupConfig{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: fbcNamespace},
			Spec: finv1.FinBackupConfigSpec{
				PVCNamespace: pvc.Namespace,
				PVC:          pvc.Name,
				Node:         specNode,
				Schedule:     "0 2 * * *",
			},
		}
		Expect(k8sClient.Create(ctx, fbc)).To(Succeed())
		return fbc
	}

	// infoMetricNodes returns the node label of every finbackupconfig_info series of fbc.
	infoMetricNodes := func(fbc *finv1.FinBackupConfig) []string {
		ch := make(chan prometheus.Metric, 1024)
		metrics.FinBackupConfigInfoMetricForTest().Collect(ch)
		close(ch)
		var nodes []string
		for m := range ch {
			var pb dto.Metric
			Expect(m.Write(&pb)).To(Succeed())
			labels := map[string]string{}
			for _, l := range pb.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["namespace"] == fbc.Namespace && labels["finbackupconfig"] == fbc.Name {
				nodes = append(nodes, labels["node"])
			}
		}
		return nodes
	}

	reconcileStatusNode := func(ctx SpecContext, fbc *finv1.FinBackupConfig) (string, error) {
		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fbc)})
		var updated finv1.FinBackupConfig
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(fbc), &updated)).To(Succeed())
		return updated.Status.Node, err
	}

	BeforeEach(func(ctx SpecContext) {
		fbcNamespace = utils.GetUniqueName("fbc-namespace")
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fbcNamespace}})).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			_ = k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fbcNamespace}})
		})

		var pv *corev1.PersistentVolume
		pvc, pv = NewPVCAndPV(normalSC, userNamespace,
			utils.GetUniqueName("pvc-select"), utils.GetUniqueName("pv-select"), rbdImageName)
		Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
		Expect(k8sClient.Create(ctx, pv)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) { DeletePVCAndPV(ctx, pvc.Namespace, pvc.Name) })

		// A label value unique to the spec keeps the Nodes of other specs out of the selection.
		nodeLabel = utils.GetUniqueName("select")
		selector, err := ParseNodeSelectorTerms(fmt.Sprintf(
			`[{"matchExpressions":[{"key":%q,"operator":"In","values":[%q]}]}]`, nodeLabelKey, nodeLabel))
		Expect(err).NotTo(HaveOccurred())
		freeSpaceRepo = fake.NewNodeFreeSpaceRepository(nil)
		reconciler = NewFinBackupConfigReconciler(
			k8sClient, scheme.Scheme, "", cephClusterID, "test:latest", "test-sa", selector, freeSpaceRepo,
			resource.MustParse("1Gi"))
	})

	It("should spread FinBackupConfigs over the top quarter of the nodes by free space", func(ctx SpecContext) {
		// Arrange
		prefix := utils.GetUniqueName("spread-")
		freeSpace := map[string]float64{}
		for i := range 8 {
			name := fmt.Sprintf("%s-%d", prefix, i)
			createNode(ctx, name, true)
			freeSpace[name] = float64(800-100*i) * gi
		}
		// Neither a node outside the selector nor a node without metrics is selected,
		// however much free space they have.
		createNode(ctx, prefix+"-unlabeled", false)
		freeSpace[prefix+"-unlabeled"] = 10000 * gi
		createNode(ctx, prefix+"-no-metrics", true)
		freeSpaceRepo.SetFreeSpace(freeSpace)

		// Act & Assert
		// The top quarter of the eight nodes is node 0 and node 1.
		for i, expected := range []string{"-0", "-1", "-0", "-1"} {
			fbc := createFBC(ctx, fmt.Sprintf("fbc-%d", i), "")
			node, err := reconcileStatusNode(ctx, fbc)
			Expect(err).NotTo(HaveOccurred())
			Expect(node).To(Equal(prefix + expected))
		}
	})

	It("should skip a node in the top quarter without room for the PVC and the margin", func(ctx SpecContext) {
		// Arrange
		// The PVC requests 10Mi and the margin is 1Gi, so a node needs 1Gi+10Mi.
		prefix := utils.GetUniqueName("margin-")
		freeSpace := map[string]float64{}
		for i, free := range []float64{3 * gi, 2 * gi, gi, gi / 2, gi / 4, gi / 4, gi / 4, gi / 4} {
			name := fmt.Sprintf("%s-%d", prefix, i)
			createNode(ctx, name, true)
			freeSpace[name] = free
		}
		freeSpaceRepo.SetFreeSpace(freeSpace)
		fbc := createFBC(ctx, "fbc-0", "")
		node, err := reconcileStatusNode(ctx, fbc)
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(prefix + "-0"))

		// Act
		// The top quarter is node 0 and node 1, and node 1 no longer has room. It has fewer
		// FinBackupConfigs than node 0, so it would be picked otherwise.
		freeSpace[prefix+"-1"] = gi
		freeSpaceRepo.SetFreeSpace(freeSpace)
		fbc = createFBC(ctx, "fbc-1", "")
		node, err = reconcileStatusNode(ctx, fbc)

		// Assert
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(prefix+"-0"), "node 1 should be skipped, and node 2 is outside the top quarter")

		By("leaving status.node empty when no node in the top quarter has room")
		freeSpace[prefix+"-0"] = gi
		freeSpaceRepo.SetFreeSpace(freeSpace)
		fbc = createFBC(ctx, "fbc-2", "")
		node, err = reconcileStatusNode(ctx, fbc)
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(BeEmpty())
	})

	It("should keep the selected node while it exists", func(ctx SpecContext) {
		// Arrange
		prefix := utils.GetUniqueName("keep-")
		createNode(ctx, prefix+"-a", true)
		freeSpaceRepo.SetFreeSpace(map[string]float64{prefix + "-a": 100 * gi})
		fbc := createFBC(ctx, "fbc", "")
		node, err := reconcileStatusNode(ctx, fbc)
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(prefix + "-a"))

		// Act
		createNode(ctx, prefix+"-b", true)
		freeSpaceRepo.SetFreeSpace(map[string]float64{prefix + "-a": 100 * gi, prefix + "-b": 1000 * gi})
		node, err = reconcileStatusNode(ctx, fbc)

		// Assert
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(prefix + "-a"))
	})

	It("should select another node when the selected node is deleted", func(ctx SpecContext) {
		// Arrange
		prefix := utils.GetUniqueName("reselect-")
		createNode(ctx, prefix+"-a", true)
		freeSpaceRepo.SetFreeSpace(map[string]float64{prefix + "-a": 100 * gi})
		fbc := createFBC(ctx, "fbc", "")
		node, err := reconcileStatusNode(ctx, fbc)
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(prefix + "-a"))

		Expect(infoMetricNodes(fbc)).To(ConsistOf(prefix + "-a"))

		// Act
		createNode(ctx, prefix+"-b", true)
		freeSpaceRepo.SetFreeSpace(map[string]float64{prefix + "-b": 100 * gi})
		Expect(k8sClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: prefix + "-a"}})).To(Succeed())
		node, err = reconcileStatusNode(ctx, fbc)

		// Assert
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(prefix + "-b"))
		Expect(infoMetricNodes(fbc)).To(ConsistOf(prefix+"-b"),
			"the series with the deleted node should be replaced")
	})

	It("should select another node when the selected node no longer matches the selector", func(ctx SpecContext) {
		// Arrange
		prefix := utils.GetUniqueName("unlabel-")
		createNode(ctx, prefix+"-a", true)
		freeSpaceRepo.SetFreeSpace(map[string]float64{prefix + "-a": 100 * gi})
		fbc := createFBC(ctx, "fbc", "")
		node, err := reconcileStatusNode(ctx, fbc)
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(prefix + "-a"))

		// Act
		// Removing the label is how a node being retired moves its backups away in advance.
		createNode(ctx, prefix+"-b", true)
		freeSpaceRepo.SetFreeSpace(map[string]float64{prefix + "-a": 1000 * gi, prefix + "-b": 100 * gi})
		var nodeA corev1.Node
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: prefix + "-a"}, &nodeA)).To(Succeed())
		delete(nodeA.Labels, nodeLabelKey)
		Expect(k8sClient.Update(ctx, &nodeA)).To(Succeed())
		node, err = reconcileStatusNode(ctx, fbc)

		// Assert
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(prefix + "-b"))
	})

	It("should select a node when spec.node not matching the selector is cleared", func(ctx SpecContext) {
		// Arrange
		name := utils.GetUniqueName("cleared-")
		createNode(ctx, name, true)
		freeSpaceRepo.SetFreeSpace(map[string]float64{name: 100 * gi})
		fbc := createFBC(ctx, "fbc", "test-node")
		node, err := reconcileStatusNode(ctx, fbc)
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal("test-node"))

		// Act
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(fbc), fbc)).To(Succeed())
		fbc.Spec.Node = ""
		Expect(k8sClient.Update(ctx, fbc)).To(Succeed())
		node, err = reconcileStatusNode(ctx, fbc)

		// Assert
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(name))
	})

	It("should use spec.node without querying the free space", func(ctx SpecContext) {
		// Arrange
		// test-node does not match the selector, which spec.node does not have to.
		freeSpaceRepo.SetError(errors.New("must not be called"))
		fbc := createFBC(ctx, "fbc", "test-node")

		// Act
		node, err := reconcileStatusNode(ctx, fbc)

		// Assert
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal("test-node"))
	})

	It("should leave status.node empty and retry later when no node is available", func(ctx SpecContext) {
		// Arrange
		// The node does not report its free space yet, as a node that has just joined.
		name := utils.GetUniqueName("unavailable-")
		createNode(ctx, name, true)
		fbc := createFBC(ctx, "fbc", "")

		// Act
		result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fbc)})

		// Assert
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(unassignedRequeueInterval))
		var updated finv1.FinBackupConfig
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(fbc), &updated)).To(Succeed())
		Expect(updated.Status.Node).To(BeEmpty())
		Expect(infoMetricNodes(fbc)).To(ConsistOf(""),
			"the metric should be set with an empty node until a node is selected")
		// The CronJob is created anyway, and its Jobs fail until a node is assigned.
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Name: "fbc-" + string(updated.UID), Namespace: fbc.Namespace},
			&batchv1.CronJob{})).To(Succeed())

		By("assigning the node once it reports its free space")
		freeSpaceRepo.SetFreeSpace(map[string]float64{name: 100 * gi})
		result, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fbc)})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(fbc), &updated)).To(Succeed())
		Expect(updated.Status.Node).To(Equal(name))
	})

	It("should clear status.node when the selected node loses its label and no node has room", func(ctx SpecContext) {
		// Arrange
		prefix := utils.GetUniqueName("no-room-")
		createNode(ctx, prefix+"-a", true)
		createNode(ctx, prefix+"-b", true)
		freeSpaceRepo.SetFreeSpace(map[string]float64{prefix + "-a": 100 * gi, prefix + "-b": gi / 2})
		fbc := createFBC(ctx, "fbc", "")
		node, err := reconcileStatusNode(ctx, fbc)
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(Equal(prefix + "-a"))

		// Act
		var nodeA corev1.Node
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: prefix + "-a"}, &nodeA)).To(Succeed())
		delete(nodeA.Labels, nodeLabelKey)
		Expect(k8sClient.Update(ctx, &nodeA)).To(Succeed())
		node, err = reconcileStatusNode(ctx, fbc)

		// Assert
		Expect(err).NotTo(HaveOccurred())
		Expect(node).To(BeEmpty(), "the backups should stop rather than stay on the retiring node")
	})

	It("should fail when the free space cannot be queried", func(ctx SpecContext) {
		// Arrange
		name := utils.GetUniqueName("query-error-")
		createNode(ctx, name, true)
		freeSpaceRepo.SetFreeSpace(map[string]float64{name: 100 * gi})
		freeSpaceRepo.SetError(errors.New("prometheus is down"))
		fbc := createFBC(ctx, "fbc", "")

		// Act
		node, err := reconcileStatusNode(ctx, fbc)

		// Assert
		Expect(err).To(HaveOccurred())
		Expect(node).To(BeEmpty())
	})

	It("should count recorded assignments the cache does not show yet", func(ctx SpecContext) {
		// Arrange
		// The FinBackupConfig has neither the finalizer nor status.node, as a stale cache
		// would show one just assigned.
		fbc := createFBC(ctx, "fbc", "")
		key := client.ObjectKeyFromObject(fbc)
		reconciler.assignments[key] = "recorded-node"

		// Act & Assert
		counts, err := reconciler.countFinBackupConfigsPerNode(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(counts).To(HaveKeyWithValue("recorded-node", 1))
		Expect(reconciler.assignments).To(HaveKey(key))

		By("dropping the record once the FinBackupConfig is gone")
		Expect(k8sClient.Delete(ctx, fbc)).To(Succeed())
		counts, err = reconciler.countFinBackupConfigsPerNode(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(counts).NotTo(HaveKey("recorded-node"))
		Expect(reconciler.assignments).NotTo(HaveKey(key))
	})
})

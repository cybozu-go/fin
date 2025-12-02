package controller

import (
	"context"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newControllerPod(name, namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "test-sa",
			Containers: []corev1.Container{
				{
					Name:  "manager",
					Image: "test:latest",
				},
			},
		},
	}
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
	var sc *storagev1.StorageClass
	var pvc *corev1.PersistentVolumeClaim
	var pv *corev1.PersistentVolume
	var reconciler *FinBackupConfigReconciler
	var stopFunc context.CancelFunc

	BeforeAll(func() {
		ctx := context.Background()
		fbcNS = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: utils.GetUniqueName("fbc-namespace")}}
		Expect(k8sClient.Create(ctx, fbcNS)).NotTo(HaveOccurred())

		sc = NewRBDStorageClass(utils.GetUniqueName("sc"), namespace, rbdPoolName)
		err := k8sClient.Create(ctx, sc)
		Expect(err).NotTo(HaveOccurred())

		pvc, pv = NewPVCAndPV(sc, namespace, "test-pvc", "test-pv", rbdImageName)
		err = k8sClient.Create(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())
		err = k8sClient.Create(ctx, pv)
		Expect(err).NotTo(HaveOccurred())

		reconciler = NewFinBackupConfigReconciler(
			k8sClient,
			scheme.Scheme,
			"",
			namespace,
			"test:latest",
			"test-sa",
		)

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
		Expect(k8sClient.Delete(ctx, sc)).To(Succeed())
		DeletePVCAndPV(ctx, namespace, pvc.Name)
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

		pod := newControllerPod(utils.GetUniqueName("test-controller"), namespace)
		Expect(k8sClient.Create(ctx, pod)).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
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
			reconciler := NewFinBackupConfigReconciler(
				k8sClient,
				scheme.Scheme,
				"",
				namespace,
				"test:latest",
				"test-sa",
			)
			By("creating RBD StorageClass")
			sc := NewRBDStorageClass("test", namespace, rbdPoolName)
			err := k8sClient.Create(ctx, sc)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, sc) }()

			By("creating PVC and PV using the StorageClass")
			pvc, pv := NewPVCAndPV(sc, namespace, "test-pvc", "test-pv", rbdImageName)
			err = k8sClient.Create(ctx, pvc)
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
			reconciler := NewFinBackupConfigReconciler(
				k8sClient,
				scheme.Scheme,
				"",
				namespace,
				"test:latest",
				"test-sa",
			)
			sc := NewRBDStorageClass("test-mismatch", "different-cluster-id", rbdPoolName)
			Expect(k8sClient.Create(ctx, sc)).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, sc) }()

			pvc, pv := NewPVCAndPV(sc, namespace, "pvc-mismatch", "pv-mismatch", rbdImageName)
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
			reconciler := NewFinBackupConfigReconciler(
				k8sClient, scheme.Scheme, "15 3 * * *", namespace, "test:latest", "test-sa")

			sc := NewRBDStorageClass("test-overwrite", namespace, rbdPoolName)
			Expect(k8sClient.Create(ctx, sc)).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, sc) }()

			pvc, pv := NewPVCAndPV(sc, namespace, "pvc-overwrite", "pv-overwrite", rbdImageName)
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

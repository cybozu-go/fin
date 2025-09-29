package v1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	finv1 "github.com/cybozu-go/fin/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

var _ = Describe("FinBackup Webhook", func() {
	var (
		namespace *corev1.Namespace
	)

	BeforeEach(func(ctx SpecContext) {
		// Create a unique namespace for each test to avoid conflicts
		namespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "webhook-test-",
			},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
	})

	Describe("ValidateDelete", func() {
		Context("full vs incremental deletion validation", func() {
			It("should allow deletion of the full backup", func(ctx SpecContext) {
				fullBackup := &finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "full-backup",
						Namespace: namespace.Name,
					},
					Spec: finv1.FinBackupSpec{
						PVC:          "test-pvc",
						PVCNamespace: namespace.Name,
						Node:         "test-node",
					},
					Status: finv1.FinBackupStatus{
						SnapID: ptr.To(1),
					},
				}

				incrementalBackup := &finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "incremental-backup",
						Namespace: namespace.Name,
					},
					Spec: finv1.FinBackupSpec{
						PVC:          "test-pvc",
						PVCNamespace: namespace.Name,
						Node:         "test-node",
					},
					Status: finv1.FinBackupStatus{
						SnapID: ptr.To(2),
					},
				}

				// Create the full backup (snapID = 1)
				Expect(k8sClient.Create(ctx, fullBackup)).To(Succeed())
				fullBackup.Status.SnapID = ptr.To(1)
				Expect(k8sClient.Status().Update(ctx, fullBackup)).To(Succeed())

				// Create an incremental backup (snapID = 2)
				Expect(k8sClient.Create(ctx, incrementalBackup)).To(Succeed())
				incrementalBackup.Status.SnapID = ptr.To(2)
				Expect(k8sClient.Status().Update(ctx, incrementalBackup)).To(Succeed())

				// Deleting the full backup should be allowed by the validating webhook.
				Expect(k8sClient.Delete(ctx, fullBackup)).To(Succeed())
			})

			It("should deny deletion of the incremental backup", func(ctx SpecContext) {
				fullBackup := &finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "full-backup",
						Namespace: namespace.Name,
					},
					Spec: finv1.FinBackupSpec{
						PVC:          "test-pvc",
						PVCNamespace: namespace.Name,
						Node:         "test-node",
					},
					Status: finv1.FinBackupStatus{
						SnapID: ptr.To(1),
					},
				}

				incrementalBackup := &finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "incremental-backup",
						Namespace: namespace.Name,
					},
					Spec: finv1.FinBackupSpec{
						PVC:          "test-pvc",
						PVCNamespace: namespace.Name,
						Node:         "test-node",
					},
					Status: finv1.FinBackupStatus{
						SnapID: ptr.To(2),
					},
				}

				// Create the full backup (snapID = 1)
				Expect(k8sClient.Create(ctx, fullBackup)).To(Succeed())
				fullBackup.Status.SnapID = ptr.To(1)
				Expect(k8sClient.Status().Update(ctx, fullBackup)).To(Succeed())

				// Create an incremental backup (snapID = 2)
				Expect(k8sClient.Create(ctx, incrementalBackup)).To(Succeed())
				incrementalBackup.Status.SnapID = ptr.To(2)
				Expect(k8sClient.Status().Update(ctx, incrementalBackup)).To(Succeed())

				// Deleting the incremental backup should be denied by the validating webhook.
				err := k8sClient.Delete(ctx, incrementalBackup)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(
					"admission webhook \"vfinbackup-v1.kb.io\" denied the request: deletion denied: FinBackup(name=incremental-backup, namespace=" + namespace.Name + ") is not a full backup " +
						"(node=test-node, pvc=" + namespace.Name + "/test-pvc); deletable full backup has snapID=1",
				))
			})
		})
	})
})

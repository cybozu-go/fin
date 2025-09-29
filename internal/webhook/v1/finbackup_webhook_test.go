package v1

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/utils/ptr"

	finv1 "github.com/cybozu-go/fin/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("FinBackup validation webhook", func() {
	var (
		validator *FinBackupCustomValidator
		ctx       context.Context
		k8sClient client.Client
	)

	BeforeEach(func() {
		ctx = context.TODO()
		scheme := runtime.NewScheme()
		Expect(finv1.AddToScheme(scheme)).To(Succeed())

		k8sClient = fake.NewClientBuilder().
			WithScheme(scheme).
			Build()

		validator = &FinBackupCustomValidator{
			client:    k8sClient,
			apiReader: k8sClient,
		}
	})

	Describe("ValidateDelete", func() {
		Context("full vs incremental deletion validation", func() {
			It("should allow deletion of the full backup", func() {
				// Create the full backup (snapID = 1)
				fullBackup := &finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "full-backup",
						Namespace: "default",
					},
					Spec: finv1.FinBackupSpec{
						PVC:          "test-pvc",
						PVCNamespace: "default",
						Node:         "test-node",
					},
					Status: finv1.FinBackupStatus{
						SnapID: ptr.To(1),
					},
				}

				// Create an incremental backup (snapID = 2)
				incrementalBackup := &finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "incremental-backup",
						Namespace: "default",
					},
					Spec: finv1.FinBackupSpec{
						PVC:          "test-pvc",
						PVCNamespace: "default",
						Node:         "test-node",
					},
					Status: finv1.FinBackupStatus{
						SnapID: ptr.To(2),
					},
				}

				// Add backups to fake client
				Expect(k8sClient.Create(ctx, fullBackup)).To(Succeed())
				Expect(k8sClient.Create(ctx, incrementalBackup)).To(Succeed())

				// Deletion of the full backup should be allowed
				warnings, err := validator.ValidateDelete(ctx, fullBackup)
				Expect(err).NotTo(HaveOccurred())
				Expect(warnings).To(BeNil())
			})

			It("should deny deletion of the incremental backup", func() {
				// Create the full backup
				fullBackup := &finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "full-backup",
						Namespace: "default",
					},
					Spec: finv1.FinBackupSpec{
						PVC:          "test-pvc",
						PVCNamespace: "default",
						Node:         "test-node",
					},
					Status: finv1.FinBackupStatus{
						SnapID: ptr.To(1),
					},
				}

				// Create an incremental backup to be deleted
				incrementalBackup := &finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "incremental-backup",
						Namespace: "default",
					},
					Spec: finv1.FinBackupSpec{
						PVC:          "test-pvc",
						PVCNamespace: "default",
						Node:         "test-node",
					},
					Status: finv1.FinBackupStatus{
						SnapID: ptr.To(2),
					},
				}

				// Add backups to fake client
				Expect(k8sClient.Create(ctx, fullBackup)).To(Succeed())
				Expect(k8sClient.Create(ctx, incrementalBackup)).To(Succeed())

				// Deletion of the incremental backup should be denied
				warnings, err := validator.ValidateDelete(ctx, incrementalBackup)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(
					"deletion denied: FinBackup(name=incremental-backup, namespace=default) is not a full backup " +
						"(node=test-node, pvc=default/test-pvc); deletable full backup has snapID=1",
				))
				Expect(warnings).To(BeNil())
			})
		})
	})
})

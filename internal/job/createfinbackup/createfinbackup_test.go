package createfinbackup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/finbackupbuilder"
	"github.com/cybozu-go/fin/internal/job/input"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	testNamespace    = "ns1"
	testPVCNamespace = "pvcns"
	testJobNamespace = "job-ns"
	testNode         = "node"
	testPVC          = "pvc"
)

func Test_constructFinBackupName(t *testing.T) {
	tests := []struct {
		name         string
		fbcName      string
		jobName      string
		jobCreatedAt time.Time
		want         string
	}{
		{
			name:         "normal-name",
			fbcName:      "example-fbc",
			jobName:      "myjob-abcdef12",
			jobCreatedAt: time.Date(2021, 12, 31, 23, 59, 59, 0, time.UTC),
			want:         "example-fbc-20211231235959-abcdef12",
		},
		{
			name:         "long-name-truncated",
			fbcName:      strings.Repeat("a", 240),
			jobName:      "short",
			jobCreatedAt: time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
			want:         fmt.Sprintf("%s-20200102030405-short", strings.Repeat("a", 229)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := constructFinBackupName(tt.fbcName, tt.jobName, tt.jobCreatedAt)
			if got != tt.want {
				t.Fatalf("constructFinBackupName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCreateFinBackup_Perform(t *testing.T) {
	if err := finv1.AddToScheme(scheme.Scheme); err != nil {
		t.Fatalf("failed to add finv1 to scheme: %v", err)
	}

	now := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)
	fbc := &finv1.FinBackupConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "fbc-ok", Namespace: "ns"},
		Spec:       finv1.FinBackupConfigSpec{PVC: testPVC, PVCNamespace: testPVCNamespace, Node: testNode},
		Status:     finv1.FinBackupConfigStatus{Node: testNode},
	}
	unassignedFBC := &finv1.FinBackupConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "fbc-unassigned", Namespace: "ns"},
		Spec:       finv1.FinBackupConfigSpec{PVC: testPVC, PVCNamespace: testPVCNamespace},
	}

	makeJob := func(name, namespace string, timestamp time.Time) *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Annotations: map[string]string{
					"batch.kubernetes.io/cronjob-scheduled-timestamp": timestamp.Format(time.RFC3339),
				},
			},
		}
	}

	tests := []struct {
		name            string
		existingObjects []client.Object
		input           *input.CreateFinBackup
		interceptor     *interceptor.Funcs
		wantErr         bool
	}{
		{
			name: "success-path",
			existingObjects: []client.Object{
				fbc,
				makeJob("job-0001", testJobNamespace, now),
			},
			input: &input.CreateFinBackup{
				FinBackupConfigName:      fbc.Name,
				FinBackupConfigNamespace: fbc.Namespace,
				JobName:                  "job-0001",
				JobNamespace:             testJobNamespace,
			},
			wantErr: false,
		},
		{
			name: "already-exists-case",
			existingObjects: []client.Object{
				fbc,
				makeJob("job-0002", testJobNamespace, now),
				&finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      constructFinBackupName(fbc.GetName(), "job-0002", now),
						Namespace: "ns",
					},
				},
			},
			input: &input.CreateFinBackup{
				FinBackupConfigName:      fbc.Name,
				FinBackupConfigNamespace: fbc.Namespace,
				JobName:                  "job-0002",
				JobNamespace:             testJobNamespace,
			},
			wantErr: false,
		},
		{
			name: "missing-finbackupconfig",
			existingObjects: []client.Object{
				makeJob("job-0003", testJobNamespace, now),
			},
			input: &input.CreateFinBackup{
				FinBackupConfigName:      "no-such-fbc",
				FinBackupConfigNamespace: fbc.Namespace,
				JobName:                  "job-0003",
				JobNamespace:             testJobNamespace,
			},
			wantErr: true,
		},
		{
			name: "create-error",
			existingObjects: []client.Object{
				fbc,
				makeJob("job-0004", testJobNamespace, now),
			},
			input: &input.CreateFinBackup{
				FinBackupConfigName:      fbc.Name,
				FinBackupConfigNamespace: fbc.Namespace,
				JobName:                  "job-0004",
				JobNamespace:             testJobNamespace,
			},
			interceptor: &interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					return errors.New("mocked create error")
				},
			},
			wantErr: true,
		},
		{
			name: "fallback-to-creation-timestamp",
			existingObjects: []client.Object{
				fbc,
				&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "job-0005",
						Namespace:         testJobNamespace,
						CreationTimestamp: metav1.NewTime(now),
					},
				},
			},
			input: &input.CreateFinBackup{
				FinBackupConfigName:      fbc.Name,
				FinBackupConfigNamespace: fbc.Namespace,
				JobName:                  "job-0005",
				JobNamespace:             testJobNamespace,
			},
			wantErr: false,
		},
		{
			name: "unassigned-node",
			existingObjects: []client.Object{
				unassignedFBC,
				makeJob("job-0006", "job-ns", now),
			},
			input: &input.CreateFinBackup{
				FinBackupConfigName:      unassignedFBC.Name,
				FinBackupConfigNamespace: unassignedFBC.Namespace,
				JobName:                  "job-0006",
				JobNamespace:             "job-ns",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(tt.existingObjects...)
			if tt.interceptor != nil {
				builder = builder.WithInterceptorFuncs(*tt.interceptor)
			}
			tt.input.CtrlClient = builder.Build()

			gotErr := NewCreateFinBackup(tt.input).Perform()
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Perform() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Perform() succeeded unexpectedly")
			}
		})
	}
}

func TestCreateFinBackup_Perform_CreatedFinBackup(t *testing.T) {
	if err := finv1.AddToScheme(scheme.Scheme); err != nil {
		t.Fatalf("failed to add finv1 to scheme: %v", err)
	}

	now := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)
	fbc := &finv1.FinBackupConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "fbc-ok", Namespace: "ns", UID: "uid-123"},
		Spec:       finv1.FinBackupConfigSpec{PVC: testPVC, PVCNamespace: testPVCNamespace},
		Status:     finv1.FinBackupConfigStatus{Node: testNode},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job-0001",
			Namespace: testJobNamespace,
			Annotations: map[string]string{
				"batch.kubernetes.io/cronjob-scheduled-timestamp": now.Format(time.RFC3339),
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(fbc, job).Build()

	err := NewCreateFinBackup(&input.CreateFinBackup{
		CtrlClient:               c,
		FinBackupConfigName:      fbc.Name,
		FinBackupConfigNamespace: fbc.Namespace,
		JobName:                  job.Name,
		JobNamespace:             job.Namespace,
	}).Perform()
	if err != nil {
		t.Fatalf("Perform() failed: %v", err)
	}

	var got finv1.FinBackup
	name := constructFinBackupName(fbc.Name, job.Name, now)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: fbc.Namespace, Name: name}, &got); err != nil {
		t.Fatalf("failed to get created FinBackup %s: %v", name, err)
	}
	want := finbackupbuilder.Build(fbc, name)
	if !equality.Semantic.DeepEqual(got.Labels, want.Labels) ||
		!equality.Semantic.DeepEqual(got.Annotations, want.Annotations) ||
		!equality.Semantic.DeepEqual(got.Spec, want.Spec) {
		t.Fatalf("created FinBackup mismatch:\n got: %+v\n want: %+v", got, *want)
	}
}

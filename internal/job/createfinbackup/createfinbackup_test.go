package createfinbackup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/controller"
	"github.com/cybozu-go/fin/internal/job/input"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func Test_newFinBackupFromConfig(t *testing.T) {
	tests := []struct {
		name         string
		fbc          *finv1.FinBackupConfig
		jobName      string
		jobCreatedAt time.Time
		want         *finv1.FinBackup
	}{
		{
			name: "basic-fields-copied",
			fbc: &finv1.FinBackupConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fbc1",
					Namespace: "ns1",
					UID:       "uid-123",
				},
				Spec: finv1.FinBackupConfigSpec{
					PVC:          "pvc1",
					PVCNamespace: "pvcns",
					Node:         "node1",
				},
			},
			jobName:      "jobname-zzzzzz01",
			jobCreatedAt: time.Date(2022, 6, 7, 8, 9, 10, 0, time.UTC),
			want: &finv1.FinBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "fbc1-20220607080910-zzzzzz01",
					Namespace:   "ns1",
					Labels:      map[string]string{controller.LabelFinBackupConfigUID: "uid-123"},
					Annotations: map[string]string{annotationFinBackupConfigName: "fbc1", annotationFinBackupConfigNS: "ns1"},
				},
				Spec: finv1.FinBackupSpec{PVC: "pvc1", PVCNamespace: "pvcns", Node: "node1"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newFinBackupFromConfig(tt.fbc, tt.jobName, tt.jobCreatedAt)
			if !equality.Semantic.DeepEqual(got, tt.want) {
				t.Fatalf("newFinBackupFromConfig() mismatch:\n got: %+v\n want: %+v", got, tt.want)
			}
		})
	}
}

func TestCreateFinBackup_Perform(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := finv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add finv1 to scheme: %v", err)
	}

	now := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)
	fbc := &finv1.FinBackupConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "fbc-ok", Namespace: "ns"},
		Spec:       finv1.FinBackupConfigSpec{PVC: "pvc", PVCNamespace: "pvcns", Node: "node"},
	}

	tests := []struct {
		name            string
		existingObjects []client.Object
		input           *input.CreateFinBackup
		interceptor     *interceptor.Funcs
		wantErr         bool
	}{
		{
			name:            "success-path",
			existingObjects: []client.Object{fbc},
			input: &input.CreateFinBackup{
				FinBackupConfigName:         "fbc-ok",
				FinBackupConfigNamespace:    "ns",
				CurrentJobName:              "job-0001",
				CurrentJobCreationTimestamp: now,
			},
			wantErr: false,
		},
		{
			name: "already-exists-case",
			existingObjects: []client.Object{
				fbc,
				&finv1.FinBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      constructFinBackupName(fbc.GetName(), "job-0002", now),
						Namespace: "ns",
					},
				},
			},
			input: &input.CreateFinBackup{
				FinBackupConfigName:         "fbc-ok",
				FinBackupConfigNamespace:    "ns",
				CurrentJobName:              "job-0002",
				CurrentJobCreationTimestamp: now,
			},
			wantErr: false,
		},
		{
			name:            "missing-finbackupconfig",
			existingObjects: []client.Object{},
			input: &input.CreateFinBackup{
				FinBackupConfigName:         "no-such-fbc",
				FinBackupConfigNamespace:    "ns",
				CurrentJobName:              "job-0003",
				CurrentJobCreationTimestamp: now,
			},
			wantErr: true,
		},
		{
			name:            "create-error",
			existingObjects: []client.Object{fbc},
			input: &input.CreateFinBackup{
				FinBackupConfigName:         "fbc-ok",
				FinBackupConfigNamespace:    "ns",
				CurrentJobName:              "job-0004",
				CurrentJobCreationTimestamp: now,
			},
			interceptor: &interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					return errors.New("mocked create error")
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.existingObjects...)
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

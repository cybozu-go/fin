package finbackupbuilder

import (
	"testing"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuild(t *testing.T) {
	tests := []struct {
		name   string
		fbc    *finv1.FinBackupConfig
		fbName string
		want   *finv1.FinBackup
	}{
		{
			name: "basic-fields-copied",
			fbc: &finv1.FinBackupConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "fbc1",
					Namespace: "ns",
					UID:       "uid-123",
				},
				Spec: finv1.FinBackupConfigSpec{
					PVC:          "pvc1",
					PVCNamespace: "pvc-ns",
				},
				Status: finv1.FinBackupConfigStatus{
					Node: "node1",
				},
			},
			fbName: "fbc1-20220607080910-zzzzzz01",
			want: &finv1.FinBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "fbc1-20220607080910-zzzzzz01",
					Namespace:   "ns",
					Labels:      map[string]string{finv1.LabelFinBackupConfigUID: "uid-123"},
					Annotations: map[string]string{finv1.AnnotationFinBackupConfigName: "fbc1", finv1.AnnotationFinBackupConfigNamespace: "ns"},
				},
				Spec: finv1.FinBackupSpec{PVC: "pvc1", PVCNamespace: "pvc-ns", Node: "node1"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Build(tt.fbc, tt.fbName)
			if !equality.Semantic.DeepEqual(got, tt.want) {
				t.Fatalf("Build() mismatch:\n got: %+v\n want: %+v", got, tt.want)
			}
		})
	}
}

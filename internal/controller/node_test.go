package controller

import (
	"context"
	"testing"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func Test_lookupNode(t *testing.T) {
	tests := []struct {
		name       string
		nodeName   string
		wantUID    types.UID
		wantExists bool
	}{
		{name: "the node exists", nodeName: "node0", wantUID: "uid-0", wantExists: true},
		{name: "the node does not exist", nodeName: "no-such-node"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testScheme(t)
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node0", UID: "uid-0"}},
			).Build()

			uid, exists, err := lookupNode(context.Background(), c, tt.nodeName)
			require.NoError(t, err)
			require.Equal(t, tt.wantExists, exists)
			require.Equal(t, tt.wantUID, uid)
		})
	}
}

func Test_nodeHoldsBackupData(t *testing.T) {
	tests := []struct {
		name    string
		nodeUID types.UID
		want    bool
	}{
		{
			name: "a FinBackup whose node has not been recorded yet",
			want: true,
		},
		{
			name:    "a FinBackup recorded against the node that is there now",
			nodeUID: "uid-0",
			want:    true,
		},
		{
			name:    "a FinBackup recorded against a node recreated under the same name",
			nodeUID: "uid-old",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backup := &finv1.FinBackup{
				Spec:   finv1.FinBackupSpec{Node: "node0"},
				Status: finv1.FinBackupStatus{NodeUID: tt.nodeUID},
			}
			require.Equal(t, tt.want, nodeHoldsBackupData(backup, "uid-0"))
		})
	}
}

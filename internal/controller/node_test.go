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
	nodeOwnerRef := func(uid types.UID) []metav1.OwnerReference {
		return []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: "node0", UID: uid}}
	}

	tests := []struct {
		name      string
		ownerRefs []metav1.OwnerReference
		want      bool
	}{
		{
			name: "a FinBackup not bound to any node yet",
			want: true,
		},
		{
			name:      "a FinBackup owned by the node that is there now",
			ownerRefs: nodeOwnerRef("uid-0"),
			want:      true,
		},
		{
			name:      "a FinBackup owned by a node recreated under the same name",
			ownerRefs: nodeOwnerRef("uid-old"),
			want:      false,
		},
		{
			name:      "a FinBackup owned by something that is not a Node",
			ownerRefs: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "job0", UID: "uid-old"}},
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backup := &finv1.FinBackup{
				ObjectMeta: metav1.ObjectMeta{OwnerReferences: tt.ownerRefs},
				Spec:       finv1.FinBackupSpec{Node: "node0"},
			}
			require.Equal(t, tt.want, nodeHoldsBackupData(backup, "uid-0"))
		})
	}
}

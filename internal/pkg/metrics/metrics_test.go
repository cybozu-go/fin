package metrics

import (
	"fmt"
	"strings"
	"testing"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testCephNS = "ceph"

func newFinBackupConfig(name, node string) *finv1.FinBackupConfig {
	return &finv1.FinBackupConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:       finv1.FinBackupConfigSpec{PVC: "pvc-" + name, PVCNamespace: "pvc-ns"},
		Status:     finv1.FinBackupConfigStatus{Node: node},
	}
}

// requireFinBackupConfigInfo asserts that finbackupconfig_info has exactly the series of
// the given FinBackupConfigs, each labeled with its status.node.
func requireFinBackupConfigInfo(t *testing.T, fbcs ...*finv1.FinBackupConfig) {
	t.Helper()
	expected := "# HELP fin_finbackupconfig_info Information about FinBackupConfig\n" +
		"# TYPE fin_finbackupconfig_info gauge\n"
	for _, fbc := range fbcs {
		expected += fmt.Sprintf(
			"fin_finbackupconfig_info{ceph_namespace=%q,finbackupconfig=%q,namespace=%q,node=%q,pvc=%q,pvc_namespace=%q} 1\n",
			testCephNS, fbc.Name, fbc.Namespace, fbc.Status.Node, fbc.Spec.PVC, fbc.Spec.PVCNamespace)
	}
	require.NoError(t, testutil.CollectAndCompare(finbackupconfigInfo, strings.NewReader(expected)))
}

func TestFinBackupConfigInfo(t *testing.T) {
	finbackupconfigInfo.Reset()
	t.Cleanup(finbackupconfigInfo.Reset)

	fbc := newFinBackupConfig("fbc", "")
	other := newFinBackupConfig("other", "node-x")
	SetFinBackupConfigInfo(other, testCephNS)

	// An unassigned FinBackupConfig reports an empty node.
	SetFinBackupConfigInfo(fbc, testCephNS)
	requireFinBackupConfigInfo(t, fbc, other)

	// The series with the old node is replaced, not left beside the new one.
	fbc.Status.Node = "node-a"
	SetFinBackupConfigInfo(fbc, testCephNS)
	requireFinBackupConfigInfo(t, fbc, other)

	// Only the series of the deleted FinBackupConfig goes away.
	DeleteFinBackupConfigInfo(fbc, testCephNS)
	requireFinBackupConfigInfo(t, other)
}

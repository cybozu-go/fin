package controller

import (
	"context"
	"slices"
	"testing"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/finbackupbuilder"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const testFBCName = "fbc"

func testFinBackupUID(name string) types.UID {
	return types.UID(name + "-uid")
}

func newAggregateTestFinBackupConfig() *finv1.FinBackupConfig {
	return &finv1.FinBackupConfig{
		ObjectMeta: metav1.ObjectMeta{Name: testFBCName, Namespace: "ns", UID: "fbc-uid"},
	}
}

func newAggregateTestFinBackup(fbc *finv1.FinBackupConfig, name string, created time.Time) *finv1.FinBackup {
	fb := finbackupbuilder.Build(fbc, name)
	fb.UID = testFinBackupUID(name)
	fb.CreationTimestamp = metav1.NewTime(created)
	return fb
}

func withUID(fb *finv1.FinBackup, uid types.UID) *finv1.FinBackup {
	fb.UID = uid
	return fb
}

func uidOf(fb *finv1.FinBackup) types.UID {
	if fb == nil {
		return ""
	}
	return fb.UID
}

func reversed(fbs []*finv1.FinBackup) []*finv1.FinBackup {
	r := slices.Clone(fbs)
	slices.Reverse(r)
	return r
}

func Test_latestFinBackup(t *testing.T) {
	fbc := newAggregateTestFinBackupConfig()
	earlier := time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Hour)

	tests := []struct {
		name    string
		fbs     []*finv1.FinBackup
		wantUID types.UID
	}{
		{
			name: "no FinBackup",
		},
		{
			name: "the FinBackup created last, whose name sorts first",
			fbs: []*finv1.FinBackup{
				newAggregateTestFinBackup(fbc, "b", earlier),
				newAggregateTestFinBackup(fbc, "a", later),
			},
			wantUID: testFinBackupUID("a"),
		},
		{
			name: "the UID, not the name, breaks a tie of the creation time",
			fbs: []*finv1.FinBackup{
				withUID(newAggregateTestFinBackup(fbc, "b", later), "uid-1"),
				withUID(newAggregateTestFinBackup(fbc, "a", later), "uid-2"),
			},
			wantUID: "uid-2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantUID, uidOf(latestFinBackup(tt.fbs)))
			require.Equal(t, tt.wantUID, uidOf(latestFinBackup(reversed(tt.fbs))),
				"the order of the FinBackups should not matter")
		})
	}
}

func Test_latestVerifiedFinBackup(t *testing.T) {
	fbc := newAggregateTestFinBackupConfig()
	earlier := time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Hour)
	withVerified := func(fb *finv1.FinBackup, status metav1.ConditionStatus, reason string) *finv1.FinBackup {
		meta.SetStatusCondition(&fb.Status.Conditions, metav1.Condition{
			Type: finv1.BackupConditionVerified, Status: status, Reason: reason,
		})
		return fb
	}
	verified := func(name string, created, snapshot time.Time) *finv1.FinBackup {
		fb := withVerified(newAggregateTestFinBackup(fbc, name, created), metav1.ConditionTrue, "VerificationComplete")
		fb.Status.CreatedAt = metav1.NewTime(snapshot)
		return fb
	}

	tests := []struct {
		name    string
		fbs     []*finv1.FinBackup
		wantUID types.UID
	}{
		{
			name: "no FinBackup is verified",
			fbs: []*finv1.FinBackup{
				newAggregateTestFinBackup(fbc, "in-progress", later),
				withVerified(newAggregateTestFinBackup(fbc, "failed", earlier), metav1.ConditionFalse, "VerificationFailed"),
			},
		},
		{
			name: "a FinBackup created later and not verified yet does not hide a verified one",
			fbs: []*finv1.FinBackup{
				verified("a", earlier, earlier),
				newAggregateTestFinBackup(fbc, "b", later),
			},
			wantUID: testFinBackupUID("a"),
		},
		{
			name: "the snapshot time decides rather than the creation time",
			fbs: []*finv1.FinBackup{
				verified("a", later, earlier.Add(time.Minute)),
				verified("b", earlier, earlier.Add(2*time.Minute)),
			},
			wantUID: testFinBackupUID("b"),
		},
		{
			name: "the UID, not the creation time or the name, breaks a tie of the snapshot time",
			fbs: []*finv1.FinBackup{
				withUID(verified("b", later, later), "uid-1"),
				withUID(verified("a", earlier, later), "uid-2"),
			},
			wantUID: "uid-2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantUID, uidOf(latestVerifiedFinBackup(tt.fbs)))
			require.Equal(t, tt.wantUID, uidOf(latestVerifiedFinBackup(reversed(tt.fbs))),
				"the order of the FinBackups should not matter")
		})
	}
}

func Test_finBackupSummary(t *testing.T) {
	fbc := newAggregateTestFinBackupConfig()
	fb := newAggregateTestFinBackup(fbc, "fb", time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC))
	ref := finv1.FinBackupReference{Name: fb.Name, UID: fb.UID}

	require.Nil(t, finBackupSummary(nil), "no FinBackup should give no summary")
	require.Equal(t, &finv1.FinBackupSummary{Ref: ref}, finBackupSummary(fb),
		"no snapshot time should be given before the snapshot is taken")

	fb.Status.CreatedAt = metav1.NewTime(time.Date(2026, 11, 12, 2, 0, 3, 0, time.UTC))
	require.Equal(t, &finv1.FinBackupSummary{Ref: ref, SnapshotTime: &fb.Status.CreatedAt}, finBackupSummary(fb))
}

func Test_calculateFinBackupConfigStatus(t *testing.T) {
	fbc := newAggregateTestFinBackupConfig()
	const node = "node-1"
	fbc.Status.Node = node
	fbc.Status.LatestBackup = &finv1.FinBackupSummary{Ref: finv1.FinBackupReference{Name: "gone", UID: "gone-uid"}}
	fb := newAggregateTestFinBackup(fbc, "fb", time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC))
	fb.Status.CreatedAt = metav1.NewTime(time.Date(2026, 11, 12, 2, 0, 3, 0, time.UTC))

	require.Equal(t, finv1.FinBackupConfigStatus{
		Node: node,
		LatestBackup: &finv1.FinBackupSummary{
			Ref:          finv1.FinBackupReference{Name: fb.Name, UID: fb.UID},
			SnapshotTime: &fb.Status.CreatedAt,
		},
	}, calculateFinBackupConfigStatus(fbc, []*finv1.FinBackup{fb}))

	require.Equal(t, finv1.FinBackupConfigStatus{Node: node}, calculateFinBackupConfigStatus(fbc, nil),
		"no FinBackup should clear the aggregated fields")
	require.Equal(t, "gone", fbc.Status.LatestBackup.Ref.Name, "the status of the config should not be modified")
}

func Test_updateStatus(t *testing.T) {
	fbc := newAggregateTestFinBackupConfig()
	fb := newAggregateTestFinBackup(fbc, "fb", time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC))
	deleting := newAggregateTestFinBackup(fbc, "deleting", fb.CreationTimestamp.Add(time.Hour))
	deleting.Finalizers = []string{"fin.cybozu.io/test"}
	deleting.DeletionTimestamp = &metav1.Time{Time: deleting.CreationTimestamp.Time}

	s := runtime.NewScheme()
	require.NoError(t, finv1.AddToScheme(s))
	var statusUpdates int
	c := fakeclient.NewClientBuilder().
		WithScheme(s).
		WithObjects(fbc.DeepCopy(), fb, deleting).
		WithStatusSubresource(&finv1.FinBackupConfig{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResource string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				statusUpdates++
				return c.SubResource(subResource).Update(ctx, obj, opts...)
			},
		}).
		Build()
	r := &FinBackupConfigReconciler{Client: c}

	var current finv1.FinBackupConfig
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(fbc), &current))
	require.NoError(t, r.updateStatus(t.Context(), &current))
	require.Equal(t, 1, statusUpdates)
	require.Equal(t, finv1.FinBackupReference{Name: fb.Name, UID: fb.UID}, current.Status.LatestBackup.Ref,
		"a FinBackup being deleted should not be reported even when created later")

	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(fbc), &current))
	require.NoError(t, r.updateStatus(t.Context(), &current))
	require.Equal(t, 1, statusUpdates, "the status should not be written when the aggregate does not change")
}

func Test_enqueueFinBackupConfigOfFinBackup(t *testing.T) {
	fbc := newAggregateTestFinBackupConfig()
	fb := newAggregateTestFinBackup(fbc, "fb", time.Now())
	require.Equal(t,
		[]ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: "ns", Name: testFBCName}}},
		enqueueFinBackupConfigOfFinBackup(t.Context(), fb))

	fb.Annotations = nil
	require.Empty(t, enqueueFinBackupConfigOfFinBackup(t.Context(), fb),
		"a FinBackup not built from a config should not be mapped")
}

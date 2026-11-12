package controller

import (
	"context"
	"testing"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/finbackupbuilder"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const testFBCName = "fbc"

func testFinBackupUID(name string) types.UID {
	return types.UID(name + "-uid")
}

func newSummaryTestFinBackupConfig() *finv1.FinBackupConfig {
	return &finv1.FinBackupConfig{
		ObjectMeta: metav1.ObjectMeta{Name: testFBCName, Namespace: "ns", UID: "fbc-uid"},
	}
}

func newSummaryTestFinBackup(fbc *finv1.FinBackupConfig, name string, created time.Time) finv1.FinBackup {
	fb := finbackupbuilder.Build(fbc, name)
	fb.UID = testFinBackupUID(name)
	fb.CreationTimestamp = metav1.NewTime(created)
	return *fb
}

func Test_relatedFinBackups(t *testing.T) {
	fbc := newSummaryTestFinBackupConfig()
	created := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		modify func(*finv1.FinBackup)
		want   bool
	}{
		{
			name:   "a FinBackup built from the config",
			modify: func(*finv1.FinBackup) {},
			want:   true,
		},
		{
			name: "a FinBackup of an earlier config with the same name",
			modify: func(fb *finv1.FinBackup) {
				fb.Labels[finbackupbuilder.LabelFinBackupConfigUID] = "old-fbc-uid"
			},
		},
		{
			name: "a FinBackup being deleted",
			modify: func(fb *finv1.FinBackup) {
				fb.DeletionTimestamp = &metav1.Time{Time: created}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb := newSummaryTestFinBackup(fbc, "fb", created)
			tt.modify(&fb)

			related := relatedFinBackups(fbc, []finv1.FinBackup{fb})

			if tt.want {
				require.Len(t, related, 1)
				require.Equal(t, fb.UID, related[0].UID)
			} else {
				require.Empty(t, related)
			}
		})
	}
}

func Test_latestFinBackup(t *testing.T) {
	fbc := newSummaryTestFinBackupConfig()
	earlier := time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Hour)

	withUID := func(fb finv1.FinBackup, uid types.UID) finv1.FinBackup {
		fb.UID = uid
		return fb
	}

	tests := []struct {
		name    string
		fbs     []finv1.FinBackup
		wantUID types.UID
	}{
		{
			name: "no FinBackup",
		},
		{
			name: "the FinBackup created last, whose name sorts first",
			fbs: []finv1.FinBackup{
				newSummaryTestFinBackup(fbc, "b", earlier),
				newSummaryTestFinBackup(fbc, "a", later),
			},
			wantUID: testFinBackupUID("a"),
		},
		{
			name: "the name breaks a tie of the creation time",
			fbs: []finv1.FinBackup{
				newSummaryTestFinBackup(fbc, "a", later),
				newSummaryTestFinBackup(fbc, "b", later),
			},
			wantUID: testFinBackupUID("b"),
		},
		{
			name: "the UID breaks a tie of the creation time and the name",
			fbs: []finv1.FinBackup{
				withUID(newSummaryTestFinBackup(fbc, "a", later), "uid-1"),
				withUID(newSummaryTestFinBackup(fbc, "a", later), "uid-2"),
			},
			wantUID: "uid-2",
		},
	}
	uidOf := func(fb *finv1.FinBackup) types.UID {
		if fb == nil {
			return ""
		}
		return fb.UID
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fbs := relatedFinBackups(fbc, tt.fbs)
			require.Equal(t, tt.wantUID, uidOf(latestFinBackup(fbs)))

			reversed := make([]*finv1.FinBackup, len(fbs))
			for i, fb := range fbs {
				reversed[len(fbs)-1-i] = fb
			}
			require.Equal(t, tt.wantUID, uidOf(latestFinBackup(reversed)), "the order of the FinBackups should not matter")
		})
	}
}

func Test_refAndSnapshotTime(t *testing.T) {
	fbc := newSummaryTestFinBackupConfig()
	fb := newSummaryTestFinBackup(fbc, "fb", time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC))

	ref, snapshotTime := refAndSnapshotTime(nil)
	require.Nil(t, ref, "no FinBackup should give no reference")
	require.Nil(t, snapshotTime)

	ref, snapshotTime = refAndSnapshotTime(&fb)
	require.Equal(t, &finv1.FinBackupReference{Name: fb.Name, UID: fb.UID}, ref)
	require.Nil(t, snapshotTime, "no snapshot time should be given before the snapshot is taken")

	fb.Status.CreatedAt = metav1.NewTime(time.Date(2026, 11, 12, 2, 0, 3, 0, time.UTC))
	ref, snapshotTime = refAndSnapshotTime(&fb)
	require.Equal(t, &finv1.FinBackupReference{Name: fb.Name, UID: fb.UID}, ref)
	require.Equal(t, &fb.Status.CreatedAt, snapshotTime)
}

func Test_updateBackupSummary(t *testing.T) {
	fbc := newSummaryTestFinBackupConfig()
	fb := newSummaryTestFinBackup(fbc, "fb", time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC))

	s := runtime.NewScheme()
	require.NoError(t, finv1.AddToScheme(s))
	var statusUpdates int
	c := fakeclient.NewClientBuilder().
		WithScheme(s).
		WithObjects(fbc.DeepCopy(), &fb).
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
	require.NoError(t, r.updateBackupSummary(t.Context(), &current))
	require.Equal(t, 1, statusUpdates)
	require.Equal(t, &finv1.FinBackupReference{Name: fb.Name, UID: fb.UID}, current.Status.LatestBackupRef)

	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(fbc), &current))
	require.NoError(t, r.updateBackupSummary(t.Context(), &current))
	require.Equal(t, 1, statusUpdates, "the status should not be written when the summary does not change")
}

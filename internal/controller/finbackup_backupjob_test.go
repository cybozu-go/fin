package controller

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"testing"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite the golden files under testdata")

func newBackupJobTestReconciler(t *testing.T, objs ...client.Object) *FinBackupReconciler {
	t.Helper()
	scheme := clientgoscheme.Scheme
	require.NoError(t, finv1.AddToScheme(scheme))
	maxPartSize := resource.MustParse("1Gi")
	return &FinBackupReconciler{
		Client:                  fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		Scheme:                  scheme,
		cephClusterNamespace:    cephNamespace,
		podImage:                podImage,
		maxPartSize:             &maxPartSize,
		rawImgExpansionUnitSize: 2048,
		rawChecksumChunkSize:    4096,
		diffChecksumChunkSize:   8192,
	}
}

func newBackupJobTestFinBackup() *finv1.FinBackup {
	return &finv1.FinBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fb1",
			Namespace: workNamespace,
			UID:       types.UID("fb-uid"),
			Annotations: map[string]string{
				annotationRBDPool:              rbdPoolName,
				AnnotationBackupTargetRBDImage: rbdImageName,
			},
		},
		Spec: finv1.FinBackupSpec{
			PVC:          "pvc1",
			PVCNamespace: userNamespace,
			Node:         "node1",
		},
		Status: finv1.FinBackupStatus{SnapID: ptr.To(7)},
	}
}

func TestCreateOrUpdateBackupJob_Spec(t *testing.T) {
	ctx := context.Background()
	backup := newBackupJobTestFinBackup()
	r := newBackupJobTestReconciler(t)

	require.NoError(t, r.createOrUpdateBackupJob(ctx, backup, "3", "pvc-uid", r.maxPartSize))

	var job batchv1.Job
	require.NoError(t, r.Get(ctx, client.ObjectKey{Namespace: cephNamespace, Name: backupJobName(backup)}, &job))

	require.Equal(t, map[string]string{
		labelKeyAppName:      labelAppNameValue,
		labelKeyAppComponent: labelComponentBackupJob,
	}, job.Labels)
	require.Equal(t, map[string]string{
		annotationFinBackupName:      "fb1",
		annotationFinBackupNamespace: workNamespace,
	}, job.Annotations)

	got, err := json.MarshalIndent(job.Spec, "", "  ")
	require.NoError(t, err)
	const golden = "testdata/backup_job_spec.golden.json"
	if *updateGolden {
		require.NoError(t, os.WriteFile(golden, append(got, '\n'), 0o644))
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	require.JSONEq(t, string(want), string(got))
}

// The spec is immutable once the Job exists, so a later call with other inputs
// must refresh only the labels and annotations.
func TestCreateOrUpdateBackupJob_ExistingJobSpecIsKept(t *testing.T) {
	ctx := context.Background()
	backup := newBackupJobTestFinBackup()
	r := newBackupJobTestReconciler(t)
	key := client.ObjectKey{Namespace: cephNamespace, Name: backupJobName(backup)}

	require.NoError(t, r.createOrUpdateBackupJob(ctx, backup, "3", "pvc-uid", r.maxPartSize))
	var created batchv1.Job
	require.NoError(t, r.Get(ctx, key, &created))
	// The fake client does not set creationTimestamp as the API server does.
	created.CreationTimestamp = metav1.Now()
	created.Labels["extra"] = "kept"
	created.Annotations[annotationFinBackupName] = "stale"
	require.NoError(t, r.Update(ctx, &created))
	before := created.Spec.DeepCopy()

	other := r.maxPartSize.DeepCopy()
	other.Add(resource.MustParse("1Gi"))
	require.NoError(t, r.createOrUpdateBackupJob(ctx, backup, "5", "other-pvc-uid", &other))

	var got batchv1.Job
	require.NoError(t, r.Get(ctx, key, &got))
	require.Equal(t, *before, got.Spec)
	require.Equal(t, "fb1", got.Annotations[annotationFinBackupName])
	require.Equal(t, "kept", got.Labels["extra"])
}

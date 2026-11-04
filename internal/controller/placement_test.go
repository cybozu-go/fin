package controller

import (
	"testing"

	finv1 "github.com/cybozu-go/fin/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestJobPlacement(t *testing.T) {
	backup := &finv1.FinBackup{Spec: finv1.FinBackupSpec{Node: "node-a"}}

	v := finVolume(backup)
	if v.Name != volumeNameFin {
		t.Errorf("volume name = %q, want %q", v.Name, volumeNameFin)
	}
	if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != "fin-node-a" {
		t.Errorf("volume source = %+v, want PVC fin-node-a", v.VolumeSource)
	}

	job := &batchv1.Job{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{}}}
	pinJobToBackupNode(job, backup)
	if got := job.Spec.Template.Spec.NodeName; got != "node-a" {
		t.Errorf("NodeName = %q, want node-a", got)
	}
}

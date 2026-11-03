package finbackupbuilder

import (
	finv1 "github.com/cybozu-go/fin/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Build leaves naming to the caller: the Job derives the name from its own Job
// name and schedule, which another creator would not have. It also makes no API
// calls, so Create and AlreadyExists handling stays with the caller.
func Build(fbc *finv1.FinBackupConfig, name string) *finv1.FinBackup {
	return &finv1.FinBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: fbc.Namespace,
			Labels: map[string]string{
				finv1.LabelFinBackupConfigUID: string(fbc.UID),
			},
			Annotations: map[string]string{
				finv1.AnnotationFinBackupConfigName:      fbc.Name,
				finv1.AnnotationFinBackupConfigNamespace: fbc.Namespace,
			},
		},
		Spec: finv1.FinBackupSpec{
			PVC:          fbc.Spec.PVC,
			PVCNamespace: fbc.Spec.PVCNamespace,
			Node:         fbc.Status.Node,
		},
	}
}

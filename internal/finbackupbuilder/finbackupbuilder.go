package finbackupbuilder

import (
	finv1 "github.com/cybozu-go/fin/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	LabelFinBackupConfigUID            = "fin.cybozu.io/fbc-uid"
	AnnotationFinBackupConfigName      = "fin.cybozu.io/fbc-name"
	AnnotationFinBackupConfigNamespace = "fin.cybozu.io/fbc-namespace"
)

// Build returns the FinBackup named name for fbc. The caller supplies the name
// because it is derived from the Job and its schedule, which another creator
// would not have.
func Build(fbc *finv1.FinBackupConfig, name string) *finv1.FinBackup {
	return &finv1.FinBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: fbc.Namespace,
			Labels: map[string]string{
				LabelFinBackupConfigUID: string(fbc.UID),
			},
			Annotations: map[string]string{
				AnnotationFinBackupConfigName:      fbc.Name,
				AnnotationFinBackupConfigNamespace: fbc.Namespace,
			},
		},
		Spec: finv1.FinBackupSpec{
			PVC:          fbc.Spec.PVC,
			PVCNamespace: fbc.Spec.PVCNamespace,
			Node:         fbc.Status.Node,
		},
	}
}

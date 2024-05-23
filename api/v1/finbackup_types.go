package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// FinBackupSpec defines the desired state of FinBackup
type FinBackupSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'replicas' specifies the number of backup replicas
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="replicas is immutable"
	Replicas int64 `json:"replicas"`

	// 'pvc' specifies backup target PVC
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="pvc is immutable"
	PVC string `json:"pvc"`

	// 'pvcNamespace' specifies backup target Namespace
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="pvcNamespace is immutable"
	PVCNamespace string `json:"pvcNamespace"`

	// 'snapshot' specifies the snapshot from which the backup is created
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="snapshot is immutable"
	Snapshot string `json:"snapshot"`

	// 'nodes' specifies the list of node names where the backup replicas are created
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nodes is immutable"
	Nodes []string `json:"nodes"`
}

type Node struct {
	// 'name' specifies the node name
	Name string `json:"name,omitempty"`

	// 'backupStatus' specifies the status of the backup on the node
	BackupStatus string `json:"backupStatus,omitempty"`

	// 'progress' specifies the backup progress
	Progress int64 `json:"progress,omitempty"`
}

// FinBackupStatus defines the observed state of FinBackup
type FinBackupStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'createdAt' specifies the creation date and time
	CreatedAt metav1.Time `json:"createdAt,omitempty"`

	// 'pvManifest' specifies the manifest of the backup target PV
	PVManifest metav1.Time `json:"pvManifest,omitempty"`

	// 'pvcManifest' specifies the manifest of the backup target PVC
	PVCManifest metav1.Time `json:"pvcManifest,omitempty"`

	// 'nodes' specifies the status of backup replicas
	Nodes []Node `json:"nodes,omitempty"`

	// 'conditions' specifies current backup conditions
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="PVC",type="string",JSONPath=".spec.pvc"
//+kubebuilder:printcolumn:name="PVC NAMESPACE",type="string",JSONPath=".spec.pvcNamespace"

// FinBackup is the Schema for the finbackups API
type FinBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FinBackupSpec   `json:"spec"`
	Status FinBackupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// FinBackupList contains a list of FinBackup
type FinBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FinBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FinBackup{}, &FinBackupList{})
}

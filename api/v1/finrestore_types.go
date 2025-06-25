package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// FinRestoreSpec defines the desired state of FinRestore
type FinRestoreSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'backup' specifies the name of FinBackup resource
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="backup is immutable"
	Backup string `json:"backup"`

	// 'pvc' specifies restore target PVC
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="pvc is immutable"
	PVC string `json:"pvc"`

	// 'pvcNamespace' specifies restore target Namespace
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="pvcNamespace is immutable"
	PVCNamespace string `json:"pvcNamespace"`
}

// FinRestoreStatus defines the observed state of FinRestore
type FinRestoreStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'actionUID' specifies the unique identifier for the restore action
	ActionUID string `json:"actionUID,omitempty"`

	// 'conditions' specifies current restore conditions
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="BACKUP",type="string",JSONPath=".spec.backup"
//+kubebuilder:printcolumn:name="PVC",type="string",JSONPath=".spec.pvc"
//+kubebuilder:printcolumn:name="PVC NAMESPACE",type="string",JSONPath=".spec.pvcNamespace"

// FinRestore is the Schema for the finrestores API
type FinRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FinRestoreSpec   `json:"spec,omitempty"`
	Status FinRestoreStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// FinRestoreList contains a list of FinRestore
type FinRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FinRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FinRestore{}, &FinRestoreList{})
}

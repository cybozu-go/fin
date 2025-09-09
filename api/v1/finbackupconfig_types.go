package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// FinBackupConfigSpec defines the desired state of FinBackupConfig
type FinBackupConfigSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	//+kubebuilder:validation:Pattern:=^\s*([0-5]?[0-9])\s+(0?[0-9]|1[0-9]|2[0-3])\s+\*\s+\*\s+\*\s*$
	Schedule string `json:"schedule"`

	//+kubebuilder:default:=false
	Suspend bool `json:"suspend,omitempty"`

	//+kubebuilder:validation:XValidation:message="spec.pvc is immutable",rule="self == oldSelf"
	PVC string `json:"pvc"`

	//+kubebuilder:validation:XValidation:message="spec.pvcNamespace is immutable",rule="self == oldSelf"
	PVCNamespace string `json:"pvcNamespace"`

	Node string `json:"node"`
}

// FinBackupConfigStatus defines the observed state of FinBackupConfig
type FinBackupConfigStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// FinBackupConfig is the Schema for the finbackupconfigs API
type FinBackupConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FinBackupConfigSpec   `json:"spec,omitempty"`
	Status FinBackupConfigStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// FinBackupConfigList contains a list of FinBackupConfig
type FinBackupConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FinBackupConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FinBackupConfig{}, &FinBackupConfigList{})
}

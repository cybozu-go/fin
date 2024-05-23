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

	// Foo is an example field of FinBackup. Edit finbackup_types.go to remove/update
	Foo string `json:"foo,omitempty"`
}

// FinBackupStatus defines the observed state of FinBackup
type FinBackupStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// FinBackup is the Schema for the finbackups API
type FinBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FinBackupSpec   `json:"spec,omitempty"`
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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

	// Node is the node the FinBackups of this config are taken on. It is
	// spec.node when set, and otherwise a node selected by the controller.
	// +optional
	Node string `json:"node,omitempty"`
}

// FinBackupConfigStatus defines the observed state of FinBackupConfig
type FinBackupConfigStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Node is the node the FinBackups of this config are taken on. It is
	// spec.node when set, and otherwise a node selected by the controller.
	// +optional
	Node string `json:"node,omitempty"`

	// LatestBackupRef refers to the FinBackup with the latest creationTimestamp.
	// Ties within the same second are broken by the name, not the creation order.
	// +optional
	LatestBackupRef *FinBackupReference `json:"latestBackupRef,omitempty"`

	// LatestBackupSnapshotTime is the snapshot time of latestBackupRef.
	// +optional
	LatestBackupSnapshotTime *metav1.Time `json:"latestBackupSnapshotTime,omitempty"`

	// PendingBackups is the number of FinBackups still in progress, excluding
	// failed ones and those that skipped verification.
	// +optional
	PendingBackups int32 `json:"pendingBackups,omitempty"`

	// LastSuccessfulBackupRef refers to the verified FinBackup with the latest
	// snapshot. It falls back to an older one when this one is deleted or corrupted.
	// +optional
	LastSuccessfulBackupRef *FinBackupReference `json:"lastSuccessfulBackupRef,omitempty"`

	// LastSuccessfulBackupSnapshotTime is the point in time lastSuccessfulBackupRef
	// restores to, not the time the backup completed.
	// +optional
	LastSuccessfulBackupSnapshotTime *metav1.Time `json:"lastSuccessfulBackupSnapshotTime,omitempty"`
}

// FinBackupReference refers to a FinBackup in the same namespace.
type FinBackupReference struct {
	Name string `json:"name"`

	// UID tells a FinBackup recreated with the same name apart.
	UID types.UID `json:"uid"`
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
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &FinBackupConfig{}, &FinBackupConfigList{})
		return nil
	})
}

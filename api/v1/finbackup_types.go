package v1

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// FinBackupSpec defines the desired state of FinBackup
type FinBackupSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'pvc' specifies backup target PVC
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="pvc is immutable"
	PVC string `json:"pvc"`

	// 'pvcNamespace' specifies backup target Namespace
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="pvcNamespace is immutable"
	PVCNamespace string `json:"pvcNamespace"`

	// 'node' specifies the node name where the backup is created
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="node is immutable"
	Node string `json:"node"`
}

// FinBackupStatus defines the observed state of FinBackup
type FinBackupStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'createdAt' specifies the creation date and time
	CreatedAt metav1.Time `json:"createdAt,omitempty"`

	// 'pvcManifest' specifies the manifest of the backup target PVC
	PVCManifest string `json:"pvcManifest,omitempty"`

	// 'snapID' specifies the unique identifier for the snapshot
	SnapID *int `json:"snapID,omitempty"`

	// 'snapSize' specifies the size of the snapshot
	SnapSize *int64 `json:"snapSize,omitempty"`

	// 'conditions' specifies current backup conditions
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

const (
	BackupConditionBackupInProgress    = "BackupInProgress"
	BackupConditionStoredToNode        = "StoredToNode"
	BackupConditionVerified            = "Verified"
	BackupConditionVerificationSkipped = "VerificationSkipped"
)

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

func (fb *FinBackup) IsStoredToNode() bool {
	return meta.IsStatusConditionTrue(fb.Status.Conditions, BackupConditionStoredToNode)
}

func (fb *FinBackup) DoesVerifiedExist() bool {
	return meta.FindStatusCondition(fb.Status.Conditions, BackupConditionVerified) != nil
}

func (fb *FinBackup) IsVerifiedTrue() bool {
	return meta.IsStatusConditionTrue(fb.Status.Conditions, BackupConditionVerified)
}

func (fb *FinBackup) IsVerifiedFalse() bool {
	return meta.IsStatusConditionFalse(fb.Status.Conditions, BackupConditionVerified)
}

func (fb *FinBackup) IsVerificationSkipped() bool {
	return meta.IsStatusConditionTrue(fb.Status.Conditions, BackupConditionVerificationSkipped)
}

func (fb *FinBackup) CanBeRestored(allowUnverified bool) bool {
	return fb.IsStoredToNode() &&
		(fb.IsVerifiedTrue() || (fb.IsVerificationSkipped() && allowUnverified))
}

func init() {
	SchemeBuilder.Register(&FinBackup{}, &FinBackupList{})
}

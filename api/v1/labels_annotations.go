package v1

// Label and annotation keys set on FinBackup by the component that creates it
// from a FinBackupConfig. Tools and users may select FinBackups with them.
const (
	// LabelFinBackupConfigUID holds the UID of the FinBackupConfig that the
	// FinBackup was created from.
	LabelFinBackupConfigUID = "fin.cybozu.io/fbc-uid"

	// AnnotationFinBackupConfigName holds the name of that FinBackupConfig.
	AnnotationFinBackupConfigName = "fin.cybozu.io/fbc-name"

	// AnnotationFinBackupConfigNamespace holds the namespace of that
	// FinBackupConfig.
	AnnotationFinBackupConfigNamespace = "fin.cybozu.io/fbc-namespace"
)

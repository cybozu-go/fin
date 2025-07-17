package model

// RestoreVolume is an interface for managing a restore volume.
type RestoreVolume interface {
	// GetPath return the path of the restore volume's block device file.
	GetPath() string

	// ZeroOut fills the block device with zero.
	ZeroOut() error

	// Apply diff applies diff to the restore volume
	ApplyDiff(diffFilePath string) error

	// CopyChunk copy a chunk from raw.img to the restore volume
	CopyChunk(rawPath string, index int, chunkSize int64) error
}

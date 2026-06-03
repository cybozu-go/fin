package cksumvfs

// #cgo CFLAGS: -DSQLITE_CKSUMVFS_STATIC
// extern int sqlite3_register_cksumvfs(const char*);
import "C"

func Register() {
	C.sqlite3_register_cksumvfs(nil)
}

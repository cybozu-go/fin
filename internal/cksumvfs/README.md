# cksumvfs

Package that statically links the SQLite Checksum VFS shim.

## File Structure

| File | Description |
|---|---|
| `cksumvfs.c` | Official SQLite Checksum VFS implementation (obtained from upstream) |
| `cksumvfs.go` | cgo binding (statically linked with `-DSQLITE_CKSUMVFS_STATIC`). `Register()` calls `sqlite3_register_cksumvfs()` to activate the VFS shim. |
| `sqlite3.h` | SQLite amalgamation header (obtained from upstream via `make update-cksumvfs`) |

## Updating C sources

Both `sqlite3.h` and `cksumvfs.c` must match the SQLite version bundled
with `go-sqlite3`. Use the `update-cksumvfs` Makefile target. It downloads
both files from sqlite.org automatically and cleans up afterwards:

```bash
make update-cksumvfs
```

### Verifying the Version

```bash
grep "^#define SQLITE_VERSION " internal/cksumvfs/sqlite3.h
```

The version must match the SQLite version in `sqlite3-binding.h` of the `github.com/mattn/go-sqlite3` module pinned in `go.mod`.

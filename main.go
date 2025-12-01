package main

import (
	"errors"
	"log/slog"
	"os"

	"github.com/cybozu-go/fin/cmd"
	"github.com/cybozu-go/fin/internal/infrastructure/ceph"
	"github.com/cybozu-go/fin/internal/job/verification"
)

func main() {
	if err := cmd.Execute(); err != nil {
		slog.Error("failed to execute command", "error", err)
		switch {
		case errors.Is(err, verification.ErrFsckFailed):
			os.Exit(3)
		case errors.Is(err, ceph.ErrChecksumMismatch):
			os.Exit(2)
		default:
			os.Exit(1)
		}
	}
}

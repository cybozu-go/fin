package main

import (
	"errors"
	"log/slog"
	"os"

	"github.com/cybozu-go/fin/cmd"
	"github.com/cybozu-go/fin/internal/job/verification"
)

func main() {
	if err := cmd.Execute(); err != nil {
		slog.Error("failed to execute command", "error", err)
		// TODO: when controller logic is updated, use exit code 3 for fsck failures and exit code 2 for csumio-detected checksum mismatches.
		if errors.Is(err, verification.ErrFsckFailed) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

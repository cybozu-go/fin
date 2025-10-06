package main

import (
	"log/slog"
	"os"

	"github.com/cybozu-go/fin/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		slog.Error("failed to execute command", "error", err)
		os.Exit(1)
	}
}

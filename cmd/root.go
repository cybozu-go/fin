package cmd

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:                "fin",
	DisableFlagParsing: true,
	Run: func(cmd *cobra.Command, args []string) {
		if err := controllerMain(args); err != nil {
			slog.Error("failed to run controller", "error", err)
			os.Exit(1)
		}
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		slog.Error("Error executing command", "error", err)
		os.Exit(1)
	}
}

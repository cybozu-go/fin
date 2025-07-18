package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:                "fin",
	DisableFlagParsing: true,
	Run: func(cmd *cobra.Command, args []string) {
		controllerMain(args)
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

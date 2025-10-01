package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:                "fin",
	DisableFlagParsing: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return controllerMain(args)
	},
}

func Execute() error {
	return rootCmd.Execute()
}

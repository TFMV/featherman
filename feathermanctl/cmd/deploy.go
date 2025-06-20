package cmd

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var deployCmd = &cobra.Command{
	Use:   "deploy [manifest]",
	Short: "Deploy a catalog or table manifest",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		color.Green("Deploying %s", args[0])
		return nil
	},
}

func init() {
	rootCmd.AddCommand(deployCmd)
}

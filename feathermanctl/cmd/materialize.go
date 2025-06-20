package cmd

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var materializeCmd = &cobra.Command{
	Use:   "materialize [manifest]",
	Short: "Materialize a view or table",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		dest := viper.GetString("to")
		color.Yellow("Materializing %s to %s", args[0], dest)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(materializeCmd)
	materializeCmd.Flags().String("to", "", "Destination path")
	viper.BindPFlag("to", materializeCmd.Flags().Lookup("to"))
}

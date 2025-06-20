package cmd

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show status of lake resources",
	RunE: func(cmd *cobra.Command, args []string) error {
		catalog := viper.GetString("catalog")
		table := viper.GetString("table")
		color.Cyan("Status for catalog %s table %s", catalog, table)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
	statusCmd.Flags().String("catalog", "", "Catalog name")
	statusCmd.Flags().String("table", "", "Table name")
	viper.BindPFlag("catalog", statusCmd.Flags().Lookup("catalog"))
	viper.BindPFlag("table", statusCmd.Flags().Lookup("table"))
}

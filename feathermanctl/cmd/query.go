package cmd

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Run an ad-hoc SQL query",
	RunE: func(cmd *cobra.Command, args []string) error {
		sql := viper.GetString("sql")
		catalog := viper.GetString("catalog")
		if sql == "" || catalog == "" {
			return fmt.Errorf("sql and catalog are required")
		}
		color.Green("Querying catalog %s: %s", catalog, sql)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().String("sql", "", "SQL statement to run")
	queryCmd.Flags().String("catalog", "", "Catalog name")
	queryCmd.Flags().StringP("output", "o", "table", "Output format (table,json,csv,arrow)")
	viper.BindPFlag("sql", queryCmd.Flags().Lookup("sql"))
	viper.BindPFlag("catalog", queryCmd.Flags().Lookup("catalog"))
	viper.BindPFlag("output", queryCmd.Flags().Lookup("output"))
}

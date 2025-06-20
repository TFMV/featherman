package cmd

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a Featherman environment",
	RunE: func(cmd *cobra.Command, args []string) error {
		ns := viper.GetString("namespace")
		withMinio, _ := cmd.Flags().GetBool("with-minio")
		color.Green("Initializing namespace %s (minio=%v)", ns, withMinio)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
	initCmd.Flags().Bool("with-minio", false, "Install MinIO")
	viper.BindPFlag("with-minio", initCmd.Flags().Lookup("with-minio"))
}

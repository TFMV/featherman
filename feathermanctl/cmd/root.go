package cmd

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "feathermanctl",
	Short: "Command line interface for Featherman",
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		color.Red("%v", err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.featherman/config.yaml)")
	rootCmd.PersistentFlags().StringP("namespace", "n", "default", "Kubernetes namespace")
	viper.BindPFlag("namespace", rootCmd.PersistentFlags().Lookup("namespace"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)
		viper.AddConfigPath(home + "/.featherman")
		viper.SetConfigType("yaml")
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config", viper.ConfigFileUsed())
	}
}

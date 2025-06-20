package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	cfgFile   string
	config    *Config
	outputter *Outputter
	logLevel  string
	logFormat string
	noColor   bool
	outputFmt string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "feathermanctl",
	Short: "Command line interface for Featherman - Data Lake Management Platform",
	Long: `feathermanctl is a CLI tool for managing Featherman data lake resources.

Featherman provides a Kubernetes-native data lake platform built on DuckDB,
enabling efficient analytics and data processing at scale.

Use feathermanctl to:
  • Deploy and manage data catalogs and tables
  • Execute SQL queries against your data lake
  • Monitor the status of lake resources
  • Initialize and configure your environment

For more information, visit: https://github.com/TFMV/featherman`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Initialize logger first
		if err := InitLogger(logLevel, logFormat); err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		// Load configuration
		var err error
		config, err = LoadConfig()
		if err != nil {
			Error("Failed to load configuration", zap.Error(err))
			return err
		}

		// Validate configuration
		if err := ValidateConfig(config); err != nil {
			Error("Invalid configuration", zap.Error(err))
			return err
		}

		// Initialize outputter
		if noColor {
			color.NoColor = true
		}
		outputter = NewOutputter(outputFmt)

		Debug("CLI initialized",
			zap.String("config_file", viper.ConfigFileUsed()),
			zap.String("log_level", logLevel),
			zap.String("output_format", outputFmt))

		return nil
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		// Ensure logs are flushed
		Sync()
	},
	Run: func(cmd *cobra.Command, args []string) {
		if err := cmd.Help(); err != nil {
			Error("Failed to display help", zap.Error(err))
			os.Exit(1)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		if logger != nil {
			Error("Command execution failed", zap.Error(err))
			Sync()
		} else {
			color.Red("Error: %v", err)
		}
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Configuration flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "",
		"config file (default is $HOME/.featherman/config.yaml)")

	// Kubernetes flags
	rootCmd.PersistentFlags().StringP("namespace", "n", "default",
		"Kubernetes namespace")
	rootCmd.PersistentFlags().String("kubeconfig", "",
		"path to kubeconfig file (default is $HOME/.kube/config)")

	// Output flags
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "table",
		"output format (table|json|yaml|csv)")
	rootCmd.PersistentFlags().BoolVar(&noColor, "no-color", false,
		"disable colored output")

	// Logging flags
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info",
		"log level (debug|info|warn|error)")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "console",
		"log format (console|json)")

	// Featherman-specific flags
	rootCmd.PersistentFlags().String("query-endpoint", "http://localhost:8080",
		"Featherman query service endpoint")
	rootCmd.PersistentFlags().String("api-version", "v1alpha1",
		"Featherman API version")

	// Bind flags to viper
	viper.BindPFlag("namespace", rootCmd.PersistentFlags().Lookup("namespace"))
	viper.BindPFlag("kubeconfig", rootCmd.PersistentFlags().Lookup("kubeconfig"))
	viper.BindPFlag("output_format", rootCmd.PersistentFlags().Lookup("output"))
	viper.BindPFlag("color_output", rootCmd.PersistentFlags().Lookup("no-color"))
	viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	viper.BindPFlag("log_format", rootCmd.PersistentFlags().Lookup("log-format"))
	viper.BindPFlag("query_endpoint", rootCmd.PersistentFlags().Lookup("query-endpoint"))
	viper.BindPFlag("api_version", rootCmd.PersistentFlags().Lookup("api-version"))
}

// initConfig reads in config file and ENV variables.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory
		home, err := os.UserHomeDir()
		if err != nil {
			Error("Failed to find home directory", zap.Error(err))
			os.Exit(1)
		}

		// Search config in home directory with name ".featherman" (without extension)
		configDir := filepath.Join(home, ".featherman")
		viper.AddConfigPath(configDir)
		viper.SetConfigType("yaml")
		viper.SetConfigName("config")

		// Create config directory if it doesn't exist
		if err := os.MkdirAll(configDir, 0755); err != nil {
			Error("Failed to create config directory",
				zap.String("path", configDir),
				zap.Error(err))
		}
	}

	// Read in environment variables that match
	viper.SetEnvPrefix("FEATHERMAN")
	viper.AutomaticEnv()

	// If a config file is found, read it in
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// Config file was found but another error was produced
			fmt.Fprintf(os.Stderr, "Error reading config file: %v\n", err)
		}
		// If no config file exists, that's okay - we'll use defaults
	}
}

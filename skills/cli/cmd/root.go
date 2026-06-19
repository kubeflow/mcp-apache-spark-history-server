package cmd

import (
	"time"

	"github.com/spf13/cobra"
)

var (
	appID      string
	attemptID  string
	serverName string
	configPath string
	outputFmt  string
	timeout    time.Duration
)

var rootCmd = &cobra.Command{
	Use:          "shs",
	Short:        "CLI for Apache Spark History Server",
	SilenceUsage: true,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&appID, "app-id", "a", "", "Spark application ID (or SHS_APP_ID env var)")
	rootCmd.PersistentFlags().StringVar(&attemptID, "attempt", "", "Application attempt ID (for YARN apps)")
	rootCmd.PersistentFlags().StringVarP(&serverName, "server", "s", "", "Server name from config")
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "", "Path to config file (default: ./config.yaml, then ~/.config/spark-mcp/config.yaml; env: SHS_CLI__CONFIG)")
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "txt", "Output format (txt|json|yaml)")
	rootCmd.PersistentFlags().DurationVar(&timeout, "timeout", 10*time.Second, "HTTP request timeout")

	rootCmd.AddCommand(
		newVersionCmd(),
		newAppsCmd(),
		newJobsCmd(),
		newStagesCmd(),
		newExecutorsCmd(),
		newThreaddumpCmd(),
		newSQLCmd(),
		newEnvironmentCmd(),
		//newStorageCmd(),
		newLogsCmd(),
		newPrimeCmd(),
		newCompareCmd(),
		newServersCmd(),
		newSetupCmd(),
		newTroubleshootCmd(),
	)
}

func Execute() error {
	return rootCmd.Execute()
}

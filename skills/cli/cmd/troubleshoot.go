package cmd

import (
	"fmt"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
	"github.com/spf13/cobra"
)

var (
	clusterID        string
	emrServerlessApp string
	emrServerlessRun string
)

func newTroubleshootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "troubleshoot",
		Short: "Analyze a failed EMR Spark workload for root cause and code fix recommendations",
		Long: `Analyze a failed or slow Spark workload using the AWS Spark Troubleshooting Agent.
Supports EMR on EC2 and EMR Serverless platforms.

Requires the aws_troubleshooting section in config.yaml and valid AWS credentials.`,
		PreRunE: requireAppID,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			if cfg.AwsTroubleshooting == nil {
				return fmt.Errorf("aws_troubleshooting section not found in config")
			}

			platformType, platformParams, err := resolvePlatform()
			if err != nil {
				return err
			}

			return runTroubleshoot(cfg.AwsTroubleshooting, platformType, platformParams)
		},
	}

	cmd.Flags().StringVar(&clusterID, "cluster", "", "EMR cluster ID (e.g., j-XXXXX)")
	cmd.Flags().StringVar(&emrServerlessApp, "emr-serverless-app", "", "EMR Serverless application ID")
	cmd.Flags().StringVar(&emrServerlessRun, "job-run", "", "EMR Serverless job run ID")
	cmd.MarkFlagsMutuallyExclusive("cluster", "emr-serverless-app")
	cmd.MarkFlagsOneRequired("cluster", "emr-serverless-app")

	return cmd
}

func resolvePlatform() (string, map[string]string, error) {
	switch {
	case clusterID != "":
		if appID == "" {
			return "", nil, fmt.Errorf("--app-id is required for EMR EC2 troubleshooting")
		}
		return "EMR_EC2", map[string]string{
			"cluster_id": clusterID,
			"step_id":    appID,
		}, nil

	case emrServerlessApp != "":
		if emrServerlessRun == "" {
			return "", nil, fmt.Errorf("--job-run is required with --emr-serverless-app")
		}
		return "EMR_SERVERLESS", map[string]string{
			"application_id": emrServerlessApp,
			"job_run_id":     emrServerlessRun,
		}, nil

	default:
		return "", nil, fmt.Errorf("specify --cluster or --emr-serverless-app")
	}
}

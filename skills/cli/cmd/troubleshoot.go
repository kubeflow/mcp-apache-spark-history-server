package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	emrEC2Cluster    string
	emrEC2Step       string
	emrServerlessApp string
	emrServerlessRun string
)

func newTroubleshootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "troubleshoot",
		Short: "Analyze a failed EMR Spark workload for root cause and code fix recommendations",
		Long: `Analyze a failed or slow Spark workload using the AWS Spark Troubleshooting Agent.
Supports EMR on EC2 and EMR Serverless platforms.

Requires valid AWS credentials (via environment variables, shared credentials, or IAM roles)
and a configured AWS region.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			platformType, platformParams, err := resolvePlatform()
			if err != nil {
				return err
			}

			return runTroubleshoot(platformType, platformParams)
		},
	}

	cmd.Flags().StringVar(&emrEC2Cluster, "emr-ec2-cluster", "", "EMR EC2 cluster ID (e.g., j-XXXXX)")
	cmd.Flags().StringVar(&emrEC2Step, "emr-ec2-step", "", "EMR EC2 step ID (e.g., s-XXXXX)")
	cmd.Flags().StringVar(&emrServerlessApp, "emr-serverless-app", "", "EMR Serverless application ID")
	cmd.Flags().StringVar(&emrServerlessRun, "emr-serverless-run", "", "EMR Serverless job run ID")
	cmd.MarkFlagsMutuallyExclusive("emr-ec2-cluster", "emr-serverless-app")
	cmd.MarkFlagsOneRequired("emr-ec2-cluster", "emr-serverless-app")

	return cmd
}

func resolvePlatform() (string, map[string]string, error) {
	switch {
	case emrEC2Cluster != "":
		if emrEC2Step == "" {
			return "", nil, fmt.Errorf("--emr-ec2-step is required with --emr-ec2-cluster")
		}
		return "EMR_EC2", map[string]string{
			"cluster_id": emrEC2Cluster,
			"step_id":    emrEC2Step,
		}, nil

	case emrServerlessApp != "":
		if emrServerlessRun == "" {
			return "", nil, fmt.Errorf("--emr-serverless-run is required with --emr-serverless-app")
		}
		return "EMR_SERVERLESS", map[string]string{
			"application_id": emrServerlessApp,
			"job_run_id":     emrServerlessRun,
		}, nil

	default:
		return "", nil, fmt.Errorf("specify --emr-ec2-cluster or --emr-serverless-app")
	}
}

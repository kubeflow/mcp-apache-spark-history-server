//go:build !aws

// This file provides a stub implementation when the binary is built without
// the "aws" build tag. It allows the CLI to compile without the AWS SDK
// dependency, producing a smaller binary for users who don't need
// troubleshooting. The default release binaries include the aws tag.

package cmd

import (
	"fmt"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
)

func runTroubleshoot(_ *config.AwsTroubleshooting, _ string, _ map[string]string) error {
	return fmt.Errorf("troubleshooting requires the 'aws' build tag; rebuild with: go build -tags aws")
}

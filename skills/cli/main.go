package main

import (
	"os"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

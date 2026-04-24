package cmd

import (
	"fmt"
	"io"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

var cliVersion = "dev"

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show CLI and server versions",
		RunE: func(cmd *cobra.Command, args []string) error {
			return showVersion(cmd)
		},
	}
}

type versionInfo struct {
	CLI    string `json:"cli" yaml:"cli"`
	Server string `json:"server,omitempty" yaml:"server,omitempty"`
}

func showVersion(cmd *cobra.Command) error {
	info := versionInfo{CLI: cliVersion}

	c, err := newClient(util.WithTimeout(5 * time.Second))
	if err == nil {
		resp, err := c.GetVersionWithResponse(cmd.Context())
		if err == nil && resp.JSON200 != nil && resp.JSON200.Spark != nil {
			info.Server = *resp.JSON200.Spark
		}
	}

	return util.PrintOutput(cmd.OutOrStdout(), info, outputFmt, func(w io.Writer) error {
		_, _ = fmt.Fprintf(w, "CLI:    %s\n", info.CLI)
		if info.Server != "" {
			_, _ = fmt.Fprintf(w, "Server: %s\n", info.Server)
		} else {
			_, _ = fmt.Fprintln(w, "Server: unavailable")
		}
		return nil
	})
}

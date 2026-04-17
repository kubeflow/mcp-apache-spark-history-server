package cmd

import (
	"fmt"
	"io"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

type serverRow struct {
	Name    string `col:"Name"`
	URL     string `col:"URL"`
	Default bool   `col:"Default"`
}

func newServersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "servers",
		Short: "List servers",
		RunE: func(cmd *cobra.Command, args []string) error {

			conf, err := config.Load(configPath)
			if err != nil {
				return fmt.Errorf("failed to load config %s", err)
			}
			return util.PrintOutput(cmd.OutOrStdout(), conf.Servers, outputFmt, func(w io.Writer) error {
				var rows []serverRow
				for name, server := range conf.Servers {
					rows = append(rows, serverRow{name, server.URL, server.Default})
				}
				return util.PrintTable(w, rows)
			})
		},
	}
	return cmd
}

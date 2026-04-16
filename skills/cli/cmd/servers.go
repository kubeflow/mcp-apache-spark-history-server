package cmd

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

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
				tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
				_, _ = fmt.Fprintf(tw, "Name\tURL\tDefault\n")
				for name, server := range conf.Servers {
					_, err := fmt.Fprintf(tw, "%s\t%s\t%t\n", name, server.URL, server.Default)
					if err != nil {
						return err
					}
				}
				return tw.Flush()
			})
		},
	}
	return cmd
}

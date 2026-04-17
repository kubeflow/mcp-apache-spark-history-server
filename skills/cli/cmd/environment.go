package cmd

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

func newEnvironmentCmd() *cobra.Command {
	var section string

	cmd := &cobra.Command{
		Use:     "environment",
		Short:   "Get environment info for an application",
		PreRunE: requireAppID,
		Aliases: []string{"env"},
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			return getEnvironment(cmd, c, section)
		},
	}

	cmd.Flags().StringVar(&section, "section", "", "Show specific section (runtime|spark|system|hadoop|metrics|classpath)")
	return cmd
}

func getEnvironment(cmd *cobra.Command, c client.ClientWithResponsesInterface, section string) error {
	resp, err := c.GetEnvironmentWithResponse(cmd.Context(), appID)
	if err != nil {
		return err
	}
	env, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}

	return util.PrintOutput(cmd.OutOrStdout(), env, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)

		if section == "" || section == "runtime" {
			if env.Runtime != nil {
				_, _ = fmt.Fprintln(tw, "=== Runtime ===")
				_, _ = fmt.Fprintf(tw, "Java Home\t%s\n", util.Deref(env.Runtime.JavaHome))
				_, _ = fmt.Fprintf(tw, "Java Version\t%s\n", util.Deref(env.Runtime.JavaVersion))
				_, _ = fmt.Fprintf(tw, "Scala Version\t%s\n", util.Deref(env.Runtime.ScalaVersion))
				_, _ = fmt.Fprintln(tw)
			}
		}

		sections := []struct {
			name string
			key  string
			data *[][]string
		}{
			{"Spark Properties", "spark", env.SparkProperties},
			{"System Properties", "system", env.SystemProperties},
			{"Hadoop Properties", "hadoop", env.HadoopProperties},
			{"Metrics Properties", "metrics", env.MetricsProperties},
			{"Classpath Entries", "classpath", env.ClasspathEntries},
		}

		for _, s := range sections {
			if section != "" && section != s.key {
				continue
			}
			if s.data != nil && len(*s.data) > 0 {
				_, _ = fmt.Fprintf(tw, "=== %s ===\n", s.name)
				for _, pair := range *s.data {
					if len(pair) >= 2 {
						_, _ = fmt.Fprintf(tw, "%s\t%s\n", pair[0], pair[1])
					}
				}
				_, _ = fmt.Fprintln(tw)
			}
		}

		return tw.Flush()
	})
}

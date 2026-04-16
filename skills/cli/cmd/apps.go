package cmd

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"text/tabwriter"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

func newAppsCmd() *cobra.Command {
	var status string
	var limit int
	var sortBy string
	var desc bool
	var allServers bool

	cmd := &cobra.Command{
		Use:   "apps",
		Short: "List applications",
		RunE: func(cmd *cobra.Command, args []string) error {
			if allServers {
				return listAppsAllServers(cmd, status, limit, sortBy, desc)
			}
			params := &client.ListApplicationsParams{}
			if status != "" {
				s := client.ListApplicationsParamsStatus(status)
				params.Status = &s
			}
			if limit != 0 {
				params.Limit = &limit
			}
			c, err := newClient()
			if err != nil {
				return err
			}
			return listApps(cmd, c, params, limit, sortBy, desc)
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (running|completed)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max number of applications to return (0 for all)")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (name|id|date|duration)")
	cmd.Flags().BoolVar(&desc, "desc", false, "Sort descending")
	cmd.Flags().BoolVar(&allServers, "all-servers", false, "Query all configured servers")
	return cmd
}

func sortApps(apps []client.Application, field string, desc bool) {
	slices.SortFunc(apps, func(a, b client.Application) int {
		var c int
		switch field {
		case "name":
			c = cmp.Compare(util.Deref(a.Name), util.Deref(b.Name))
		case "id":
			c = cmp.Compare(util.Deref(a.Id), util.Deref(b.Id))
		case "date":
			c = cmp.Compare(latestAttemptEpoch(a), latestAttemptEpoch(b))
		case "duration":
			c = cmp.Compare(latestAttemptDuration(a), latestAttemptDuration(b))
		}
		if desc {
			return -c
		}
		return c
	})
}

func latestAttemptEpoch(a client.Application) int64 {
	if a.Attempts == nil {
		return 0
	}
	for _, att := range *a.Attempts {
		if att.StartTimeEpoch != nil {
			return *att.StartTimeEpoch
		}
	}
	return 0
}

func latestAttemptDuration(a client.Application) int64 {
	if a.Attempts == nil {
		return 0
	}
	for _, att := range *a.Attempts {
		if att.Duration != nil {
			return *att.Duration
		}
	}
	return 0
}

type appEntry struct {
	App    client.Application
	Server string
}

func listAppsAllServers(cmd *cobra.Command, status string, limit int, sortBy string, desc bool) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	params := &client.ListApplicationsParams{}
	if status != "" {
		s := client.ListApplicationsParamsStatus(status)
		params.Status = &s
	}

	var entries []appEntry
	for name := range cfg.Servers {
		c, err := newClient(util.WithTimeout(timeout), util.WithServer(name))
		if err != nil {
			return fmt.Errorf("server %s: %w", name, err)
		}
		resp, err := c.ListApplicationsWithResponse(cmd.Context(), params)
		if err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "warning: server %s: %v\n", name, err)
			continue
		}
		body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
		if err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "warning: server %s: %v\n", name, err)
			continue
		}
		for _, app := range *body {
			entries = append(entries, appEntry{App: app, Server: name})
		}
	}

	if sortBy != "" {
		slices.SortFunc(entries, func(a, b appEntry) int {
			c := 0
			switch sortBy {
			case "server":
				c = cmp.Compare(a.Server, b.Server)
			case "name":
				c = cmp.Compare(util.Deref(a.App.Name), util.Deref(b.App.Name))
			case "id":
				c = cmp.Compare(util.Deref(a.App.Id), util.Deref(b.App.Id))
			case "date":
				c = cmp.Compare(latestAttemptEpoch(a.App), latestAttemptEpoch(b.App))
			case "duration":
				c = cmp.Compare(latestAttemptDuration(a.App), latestAttemptDuration(b.App))
			}
			if desc {
				return -c
			}
			return c
		})
	}

	entries, total := util.ApplyLimit(entries, limit)

	return util.PrintOutput(cmd.OutOrStdout(), entries, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "SERVER\tID\tNAME\tDURATION\tATTEMPTS")
		for _, e := range entries {
			name, id := util.Deref(e.App.Name), util.Deref(e.App.Id)
			attempts := 0
			if e.App.Attempts != nil {
				attempts = len(*e.App.Attempts)
			}
			dur := util.FormatMsVal(latestAttemptDuration(e.App))
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\n", e.Server, id, name, dur, attempts)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		util.PrintLimitFooter(w, limit, total, "applications")
		return nil
	})
}

func listApps(cmd *cobra.Command, c client.ClientWithResponsesInterface, params *client.ListApplicationsParams, limit int, sortBy string, desc bool) error {
	resp, err := c.ListApplicationsWithResponse(cmd.Context(), params)
	if err != nil {
		return err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}

	apps := *body

	if sortBy != "" {
		sortApps(apps, sortBy, desc)
	}

	return util.PrintOutput(cmd.OutOrStdout(), apps, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "ID\tNAME\tDURATION\tATTEMPTS")
		for _, app := range apps {
			name, id := "", ""
			if app.Name != nil {
				name = *app.Name
			}
			if app.Id != nil {
				id = *app.Id
			}
			attempts := 0
			if app.Attempts != nil {
				attempts = len(*app.Attempts)
			}
			dur := util.FormatMsVal(latestAttemptDuration(app))
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n", id, name, dur, attempts)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		util.PrintLimitFooter(w, limit, len(apps), "applications")
		return nil
	})
}

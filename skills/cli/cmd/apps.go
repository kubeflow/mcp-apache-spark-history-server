package cmd

import (
	"cmp"
	"fmt"
	"io"
	"slices"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

type appRow struct {
	ID       string `col:"ID"`
	Name     string `col:"NAME"`
	Duration string `col:"DURATION"`
	Attempts int    `col:"ATTEMPTS"`
}

type appServerRow struct {
	Server   string `col:"SERVER"`
	ID       string `col:"ID"`
	Name     string `col:"NAME"`
	Duration string `col:"DURATION"`
	Attempts int    `col:"ATTEMPTS"`
}

type attemptRow struct {
	Attempt  string `col:"ATTEMPT"`
	Status   string `col:"STATUS"`
	Duration string `col:"DURATION"`
	Start    string `col:"START"`
	End      string `col:"END"`
}

func newAppsCmd() *cobra.Command {
	var status string
	var limit int
	var sortBy string
	var desc bool
	var allServers bool
	var showAttempts bool

	cmd := &cobra.Command{
		Use:   "apps",
		Short: "List applications",
		RunE: func(cmd *cobra.Command, args []string) error {
			if showAttempts {
				if appID == "" {
					return fmt.Errorf("--attempts requires --app-id")
				}
				return listAttempts(cmd)
			}
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
	cmd.Flags().BoolVar(&showAttempts, "attempts", false, "List attempts for an application (requires -a)")
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

func appAttemptCount(a client.Application) int {
	if a.Attempts == nil {
		return 0
	}
	return len(*a.Attempts)
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

	rows := make([]appServerRow, len(entries))
	for i, e := range entries {
		rows[i] = appServerRow{e.Server, util.Deref(e.App.Id), util.Deref(e.App.Name), util.FormatMsVal(latestAttemptDuration(e.App)), appAttemptCount(e.App)}
	}
	return util.PrintOutput(cmd.OutOrStdout(), entries, outputFmt, func(w io.Writer) error {
		if err := util.PrintTable(w, rows); err != nil {
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

	rows := make([]appRow, len(apps))
	for i, app := range apps {
		rows[i] = appRow{util.Deref(app.Id), util.Deref(app.Name), util.FormatMsVal(latestAttemptDuration(app)), appAttemptCount(app)}
	}
	return util.PrintOutput(cmd.OutOrStdout(), apps, outputFmt, func(w io.Writer) error {
		if err := util.PrintTable(w, rows); err != nil {
			return err
		}
		util.PrintLimitFooter(w, limit, len(apps), "applications")
		return nil
	})
}

func listAttempts(cmd *cobra.Command) error {
	c, err := newClient()
	if err != nil {
		return err
	}
	resp, err := c.GetApplicationWithResponse(cmd.Context(), appID)
	if err != nil {
		return err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}
	if body.Attempts == nil {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No attempts found.")
		return nil
	}
	attempts := *body.Attempts

	rows := make([]attemptRow, len(attempts))
	for i, a := range attempts {
		status := "INCOMPLETE"
		if util.Deref(a.Completed) {
			status = "COMPLETED"
		}
		dur := util.FormatMsVal(util.Deref(a.Duration))
		if dur == "0s" && !util.Deref(a.Completed) {
			dur = "-"
		}
		end := util.Deref(a.EndTime)
		if !util.Deref(a.Completed) {
			end = "-"
		}
		rows[i] = attemptRow{util.Deref(a.AttemptId), status, dur, util.Deref(a.StartTime), end}
	}
	return util.PrintOutput(cmd.OutOrStdout(), attempts, outputFmt, func(w io.Writer) error {
		return util.PrintTable(w, rows)
	})
}

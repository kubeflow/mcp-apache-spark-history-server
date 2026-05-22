package cmd

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

type threadRow struct {
	ID        int64  `col:"ID"`
	Name      string `col:"NAME"`
	State     string `col:"STATE"`
	Daemon    bool   `col:"DAEMON"`
	BlockedBy int64  `col:"BLOCKED_BY"`
	Lock      string `col:"LOCK"`
}

func newThreaddumpCmd() *cobra.Command {
	var stateFilter string
	var nameFilter string
	var blockedOnly bool

	cmd := &cobra.Command{
		Use:     "threaddump <executor-id>",
		Short:   "Get thread dump for a driver or executor. Requires the application to be running. Use 'driver' for the driver.",
		Long:    "Get the JVM thread dump for a driver or executor. Use 'driver' for the driver. The application must be running; completed applications return 404 because the Spark History Server does not persist thread dumps.",
		PreRunE: requireAppID,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			executorID := args[0]

			resp, err := c.GetExecutorThreadsWithResponse(cmd.Context(), appID, executorID)
			if err != nil {
				return err
			}
			body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
			if err != nil {
				return err
			}
			threads := util.Deref(body)

			threads = filterThreads(threads, stateFilter, nameFilter, blockedOnly)

			slices.SortFunc(threads, func(a, b client.ThreadStackTrace) int {
				return cmp.Compare(util.Deref(a.ThreadId), util.Deref(b.ThreadId))
			})

			return util.PrintOutput(cmd.OutOrStdout(), threads, outputFmt, func(w io.Writer) error {
				rows := make([]threadRow, len(threads))
				for i, t := range threads {
					rows[i] = threadRow{
						ID:        util.Deref(t.ThreadId),
						Name:      util.Deref(t.ThreadName),
						State:     util.Deref(t.ThreadState),
						Daemon:    util.Deref(t.IsDaemon),
						BlockedBy: util.Deref(t.BlockedByThreadId),
						Lock:      util.Deref(t.LockName),
					}
				}
				if err := util.PrintTable(w, rows); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(w, "\n%d threads (use -o json|yaml for full stack traces)\n", len(threads))
				return nil
			})
		},
	}

	cmd.Flags().StringVar(&stateFilter, "state", "", "Filter by thread state (RUNNABLE|BLOCKED|WAITING|TIMED_WAITING|...)")
	cmd.Flags().StringVar(&nameFilter, "name", "", "Filter by substring match on thread name (case-insensitive)")
	cmd.Flags().BoolVar(&blockedOnly, "blocked-only", false, "Show only threads blocked on a lock or another thread")
	return cmd
}

func filterThreads(threads []client.ThreadStackTrace, state, name string, blockedOnly bool) []client.ThreadStackTrace {
	if state == "" && name == "" && !blockedOnly {
		return threads
	}
	nameLow := strings.ToLower(name)
	out := make([]client.ThreadStackTrace, 0, len(threads))
	for _, t := range threads {
		if state != "" && !strings.EqualFold(util.Deref(t.ThreadState), state) {
			continue
		}
		if nameLow != "" && !strings.Contains(strings.ToLower(util.Deref(t.ThreadName)), nameLow) {
			continue
		}
		if blockedOnly && t.BlockedByThreadId == nil && t.LockName == nil {
			continue
		}
		out = append(out, t)
	}
	return out
}

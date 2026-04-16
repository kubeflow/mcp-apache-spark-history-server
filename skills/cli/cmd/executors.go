package cmd

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

type timelineEvent struct {
	Time time.Time
	Kind string // "started" or "removed"
	IDs  []string
}

type rawEvent struct {
	time time.Time
	id   string
	kind string
}

func newExecutorsCmd() *cobra.Command {
	var all bool
	var summary bool
	var timeline bool
	var sortBy string
	var limit int

	cmd := &cobra.Command{
		Use:     "executors [executorId]",
		Short:   "List or get executors for an application",
		PreRunE: requireAppID,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			if len(args) == 1 {
				return getExecutor(cmd, c, args[0])
			}
			if timeline {
				return listExecutorsTimeline(cmd, c)
			}
			if summary {
				return listExecutorsSummary(cmd, c, limit, sortBy)
			}
			return listExecutors(cmd, c, all, limit, sortBy)
		},
	}

	cmd.Flags().BoolVar(&all, "all", false, "Include dead executors")
	cmd.Flags().BoolVar(&summary, "summary", false, "Show peak memory metrics (implies --all)")
	cmd.Flags().BoolVar(&timeline, "timeline", false, "Show executor lifecycle timeline")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max number of executors to return (0 for all)")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (failed-tasks|duration|gc|id)")
	return cmd
}

func sortExecutors(execs []client.Executor, sortBy string) {
	slices.SortFunc(execs, func(a, b client.Executor) int {
		switch sortBy {
		case "failed-tasks":
			return -cmp.Compare(util.Deref(a.FailedTasks), util.Deref(b.FailedTasks))
		case "duration":
			return -cmp.Compare(util.Deref(a.TotalDuration), util.Deref(b.TotalDuration))
		case "gc":
			return -cmp.Compare(util.Deref(a.TotalGCTime), util.Deref(b.TotalGCTime))
		case "id":
			return cmp.Compare(util.Deref(a.Id), util.Deref(b.Id))
		case "summary":
			// dead first, then by task time desc
			aa, ab := util.Deref(a.IsActive), util.Deref(b.IsActive)
			if aa != ab {
				if ab {
					return -1
				}
				return 1
			}
			return -cmp.Compare(util.Deref(a.TotalDuration), util.Deref(b.TotalDuration))
		default:
			aa, ab := util.Deref(a.IsActive), util.Deref(b.IsActive)
			if aa != ab {
				if aa {
					return -1
				}
				return 1
			}
			return -cmp.Compare(util.Deref(a.TotalDuration), util.Deref(b.TotalDuration))
		}
	})
}

func fetchExecutors(ctx context.Context, c client.ClientWithResponsesInterface, all bool) ([]client.Executor, error) {
	if all {
		resp, err := c.ListAllExecutorsWithResponse(ctx, appID)
		if err != nil {
			return nil, err
		}
		body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
		if err != nil {
			return nil, err
		}
		return *body, nil
	}
	resp, err := c.ListActiveExecutorsWithResponse(ctx, appID)
	if err != nil {
		return nil, err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return nil, err
	}
	return util.Deref(body), nil
}

func listExecutors(cmd *cobra.Command, c client.ClientWithResponsesInterface, all bool, limit int, sortBy string) error {
	execs, err := fetchExecutors(cmd.Context(), c, all)
	if err != nil {
		return err
	}

	sortExecutors(execs, sortBy)

	execs, total := util.ApplyLimit(execs, limit)

	return util.PrintOutput(cmd.OutOrStdout(), execs, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "ID\tACTIVE\tHOST\tTASKS\tFAILED\tTASK_TIME\tGC_TIME\tINPUT\tSHUFFLE_READ\tSHUFFLE_WRITE")
		for _, e := range execs {
			_, _ = fmt.Fprintf(tw, "%s\t%v\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n",
				util.Deref(e.Id),
				util.Deref(e.IsActive),
				util.Deref(e.HostPort),
				util.Deref(e.TotalTasks),
				util.Deref(e.FailedTasks),
				util.FormatMs(e.TotalDuration),
				util.FormatMs(e.TotalGCTime),
				util.DerefBytes(e.TotalInputBytes),
				util.DerefBytes(e.TotalShuffleRead),
				util.DerefBytes(e.TotalShuffleWrite),
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		util.PrintLimitFooter(w, limit, total, "executors")
		return nil
	})
}

func getExecutor(cmd *cobra.Command, c client.ClientWithResponsesInterface, id string) error {
	// API has no single-executor endpoint; fetch all and filter
	execs, err := fetchExecutors(cmd.Context(), c, true)
	if err != nil {
		return err
	}

	idx := slices.IndexFunc(execs, func(e client.Executor) bool {
		return util.Deref(e.Id) == id
	})
	if idx == -1 {
		return fmt.Errorf("executor %s not found", id)
	}
	e := execs[idx]

	return util.PrintOutput(cmd.OutOrStdout(), e, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "Executor ID:\t%s\n", util.Deref(e.Id))
		_, _ = fmt.Fprintf(tw, "Active:\t%v\n", util.Deref(e.IsActive))
		_, _ = fmt.Fprintf(tw, "Host:\t%s\n", util.Deref(e.HostPort))
		_, _ = fmt.Fprintf(tw, "Cores:\t%d\n", util.Deref(e.TotalCores))
		_, _ = fmt.Fprintf(tw, "Max Memory:\t%s\n", util.DerefBytes(e.MaxMemory))
		_, _ = fmt.Fprintf(tw, "Memory Used:\t%s\n", util.DerefBytes(e.MemoryUsed))
		_, _ = fmt.Fprintf(tw, "Disk Used:\t%s\n", util.DerefBytes(e.DiskUsed))
		_, _ = fmt.Fprintf(tw, "Tasks:\t%d (active: %d, failed: %d, completed: %d)\n",
			util.Deref(e.TotalTasks), util.Deref(e.ActiveTasks), util.Deref(e.FailedTasks), util.Deref(e.CompletedTasks))
		_, _ = fmt.Fprintf(tw, "Task Time:\t%s\n", util.FormatMs(e.TotalDuration))
		_, _ = fmt.Fprintf(tw, "GC Time:\t%s\n", util.FormatMs(e.TotalGCTime))
		_, _ = fmt.Fprintf(tw, "Input:\t%s\n", util.DerefBytes(e.TotalInputBytes))
		_, _ = fmt.Fprintf(tw, "Shuffle Read:\t%s\n", util.DerefBytes(e.TotalShuffleRead))
		_, _ = fmt.Fprintf(tw, "Shuffle Write:\t%s\n", util.DerefBytes(e.TotalShuffleWrite))
		_, _ = fmt.Fprintf(tw, "RDD Blocks:\t%d\n", util.Deref(e.RddBlocks))
		if e.RemoveReason != nil {
			_, _ = fmt.Fprintf(tw, "Remove Reason:\t%s\n", *e.RemoveReason)
		}
		return tw.Flush()
	})
}

func formatSparkTimeShort(s *string) string {
	if s == nil {
		return ""
	}
	t, err := util.ParseSparkTime(*s)
	if err != nil {
		return *s
	}
	return t.Format("15:04:05")
}

func peakMetric(e client.Executor, fn func(*client.PeakMemoryMetrics) *int64) int64 {
	if e.PeakMemoryMetrics == nil {
		return 0
	}
	return util.Deref(fn(e.PeakMemoryMetrics))
}

func listExecutorsSummary(cmd *cobra.Command, c client.ClientWithResponsesInterface, limit int, sortBy string) error {
	execs, err := fetchExecutors(cmd.Context(), c, true)
	if err != nil {
		return err
	}

	if sortBy == "" {
		sortBy = "summary"
	}
	sortExecutors(execs, sortBy)

	execs, total := util.ApplyLimit(execs, limit)

	return util.PrintOutput(cmd.OutOrStdout(), execs, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "ID\tACTIVE\tADDED\tREMOVED\tTASKS\tPEAK_RSS\tPEAK_HEAP\tPEAK_DIRECT\tPEAK_OFFHEAP\tGC_TIME\tREMOVE_REASON")
		for _, e := range execs {
			reason := ""
			if e.RemoveReason != nil {
				reason = strings.TrimSpace(*e.RemoveReason)
				if i := strings.IndexByte(reason, '\n'); i != -1 {
					reason = reason[:i]
				}
			}
			_, _ = fmt.Fprintf(tw, "%s\t%v\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
				util.Deref(e.Id),
				util.Deref(e.IsActive),
				formatSparkTimeShort(e.AddTime),
				formatSparkTimeShort(e.RemoveTime),
				util.Deref(e.TotalTasks),
				util.FormatBytes(peakMetric(e, func(p *client.PeakMemoryMetrics) *int64 { return p.ProcessTreeJVMRSSMemory })),
				util.FormatBytes(peakMetric(e, func(p *client.PeakMemoryMetrics) *int64 { return p.JVMHeapMemory })),
				util.FormatBytes(peakMetric(e, func(p *client.PeakMemoryMetrics) *int64 { return p.DirectPoolMemory })),
				util.FormatBytes(peakMetric(e, func(p *client.PeakMemoryMetrics) *int64 { return p.JVMOffHeapMemory })),
				util.FormatMs(e.TotalGCTime),
				reason,
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		util.PrintLimitFooter(w, limit, total, "executors")
		return nil
	})
}

func listExecutorsTimeline(cmd *cobra.Command, c client.ClientWithResponsesInterface) error {
	execs, err := fetchExecutors(cmd.Context(), c, true)
	if err != nil {
		return err
	}

	var events []rawEvent

	for _, e := range execs {
		id := util.Deref(e.Id)
		if id == "driver" {
			continue
		}
		if e.AddTime != nil {
			if t, err := util.ParseSparkTime(*e.AddTime); err == nil {
				events = append(events, rawEvent{time: t, id: id, kind: "started"})
			}
		}
		if e.RemoveTime != nil {
			if t, err := util.ParseSparkTime(*e.RemoveTime); err == nil {
				events = append(events, rawEvent{time: t, id: id, kind: "removed"})
			}
		}
	}

	sort.Slice(events, func(i, j int) bool { return events[i].time.Before(events[j].time) })

	// group events within 5 minutes with same kind
	const window = 5 * time.Minute
	var grouped []timelineEvent
	for i := 0; i < len(events); {
		j := i + 1
		for j < len(events) &&
			events[j].kind == events[i].kind &&
			events[j].time.Sub(events[i].time) <= window {
			j++
		}
		ids := make([]string, j-i)
		for k := i; k < j; k++ {
			ids[k-i] = events[k].id
		}
		grouped = append(grouped, timelineEvent{
			Time: events[i].time,
			Kind: events[i].kind,
			IDs:  ids,
		})
		i = j
	}

	return util.PrintOutput(cmd.OutOrStdout(), grouped, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "TIME\tEVENT\tEXECUTORS\tCOUNT")
		for _, g := range grouped {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n",
				g.Time.Format("15:04"),
				g.Kind,
				formatIDRange(g.IDs),
				len(g.IDs),
			)
		}
		return tw.Flush()
	})
}

func formatIDRange(ids []string) string {
	if len(ids) <= 5 {
		return strings.Join(ids, ",")
	}
	return ids[0] + "-" + ids[len(ids)-1]
}

package cmd

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

func newSQLCmd() *cobra.Command {
	var status string
	var limit int
	var sortBy string
	var showInitialPlan bool
	var showPlan bool
	var showSummary bool

	cmd := &cobra.Command{
		Use:     "sql [executionId]",
		Short:   "List or get SQL executions for an application",
		PreRunE: requireAppID,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			if len(args) == 1 {
				id, err := strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("invalid execution ID: %s", args[0])
				}
				if showInitialPlan {
					showPlan = true
				}
				return getSQLExecution(cmd, c, id, showPlan, showInitialPlan, showSummary)
			}
			if showSummary || showInitialPlan {
				return fmt.Errorf("--summary and --initial-plan require an execution ID")
			}
			return listSQLExecutions(cmd, c, status, limit, sortBy)
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (completed|running|failed)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max number of executions to return (0 for all)")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (duration|id)")
	cmd.Flags().BoolVar(&showPlan, "plan", false, "Include query plan and node metrics")
	cmd.Flags().BoolVar(&showInitialPlan, "initial-plan", false, "Include initial plans (implies --plan)")
	cmd.Flags().BoolVar(&showSummary, "summary", false, "Include job summaries + aggregate stage metrics")
	return cmd
}

type sqlSummaryExec struct {
	ID             *int                       `json:"id,omitempty" yaml:"id,omitempty"`
	Status         *client.SQLExecutionStatus `json:"status,omitempty" yaml:"status,omitempty"`
	Description    *string                    `json:"description,omitempty" yaml:"description,omitempty"`
	SubmissionTime *string                    `json:"submissionTime,omitempty" yaml:"submissionTime,omitempty"`
	Duration       *int64                     `json:"duration,omitempty" yaml:"duration,omitempty"`
	SuccessJobIds  *[]int                     `json:"successJobIds,omitempty" yaml:"successJobIds,omitempty"`
	FailedJobIds   *[]int                     `json:"failedJobIds,omitempty" yaml:"failedJobIds,omitempty"`
	RunningJobIds  *[]int                     `json:"runningJobIds,omitempty" yaml:"runningJobIds,omitempty"`
}

type sqlDetail struct {
	Execution sqlSummaryExec    `json:"execution" yaml:"execution"`
	Jobs      []client.Job      `json:"jobs,omitempty" yaml:"jobs,omitempty"`
	Stages    *stageAggregation `json:"stageMetrics,omitempty" yaml:"stageMetrics,omitempty"`
}

var sqlStatusPriority = map[string]int{
	"FAILED":    0,
	"RUNNING":   1,
	"COMPLETED": 2,
}

func sqlDuration(s client.SQLExecution) time.Duration {
	return time.Duration(util.Deref(s.Duration)) * time.Millisecond
}

func sortSQLExecutions(execs []client.SQLExecution, sortBy string) {
	if sortBy != "" {
		slices.SortFunc(execs, func(a, b client.SQLExecution) int {
			switch sortBy {
			case "duration":
				return -cmp.Compare(sqlDuration(a), sqlDuration(b))
			case "id":
				return -cmp.Compare(util.Deref(a.Id), util.Deref(b.Id))
			}
			return 0
		})
		return
	}
	// default: failed first, then by duration desc
	slices.SortFunc(execs, func(a, b client.SQLExecution) int {
		pa := sqlStatusPriority[string(util.Deref(a.Status))]
		pb := sqlStatusPriority[string(util.Deref(b.Status))]
		if c := cmp.Compare(pa, pb); c != 0 {
			return c
		}
		return -cmp.Compare(sqlDuration(a), sqlDuration(b))
	})
}

func listSQLExecutions(cmd *cobra.Command, c client.ClientWithResponsesInterface, status string, limit int, sortBy string) error {
	details := false
	allResults := math.MaxInt32
	params := &client.ListSQLExecutionsParams{Details: &details, Length: &allResults}
	resp, err := c.ListSQLExecutionsWithResponse(cmd.Context(), appID, params)
	if err != nil {
		return err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}

	execs := *body
	if status != "" {
		upper := strings.ToUpper(status)
		execs = slices.DeleteFunc(execs, func(e client.SQLExecution) bool {
			return string(util.Deref(e.Status)) != upper
		})
	}

	sortSQLExecutions(execs, sortBy)

	execs, total := util.ApplyLimit(execs, limit)

	return util.PrintOutput(cmd.OutOrStdout(), execs, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "ID\tSTATUS\tDESCRIPTION\tDURATION\tFAILED_JOBS\tSUCCESS_JOBS\tRUNNING_JOBS")
		for _, e := range execs {
			desc := util.Deref(e.Description)
			if len(desc) > 80 {
				desc = desc[:77] + "..."
			}
			_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
				util.Deref(e.Id),
				util.Deref(e.Status),
				desc,
				sqlDuration(e).Truncate(time.Millisecond),
				util.FormatIntSlice(e.FailedJobIds),
				util.FormatIntSlice(e.SuccessJobIds),
				util.FormatIntSlice(e.RunningJobIds),
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		util.PrintLimitFooter(w, limit, total, "executions")
		return nil
	})
}

// stripInitialPlans removes "== Initial Plan ==" sections from an AQE plan description,
// keeping only the "== Final Plan ==" / "== Current Plan ==" sections.
func stripInitialPlans(plan string) string {
	var b strings.Builder
	lines := strings.Split(plan, "\n")
	i := 0
	for i < len(lines) {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, "+- == Initial Plan ==") {
			// Find the indentation depth of this marker.
			markerIndent := len(lines[i]) - len(strings.TrimLeft(lines[i], " "))
			i++
			// Skip all subsequent lines indented deeper than the marker.
			for i < len(lines) {
				lineIndent := len(lines[i]) - len(strings.TrimLeft(lines[i], " "))
				if lines[i] == "" || lineIndent > markerIndent {
					i++
				} else {
					break
				}
			}
			continue
		}
		b.WriteString(lines[i])
		b.WriteByte('\n')
		i++
	}
	return strings.TrimRight(b.String(), "\n")
}

// formatNodeMetrics formats the node-level metrics as a readable section.
func formatNodeMetrics(nodes *[]client.SQLPlanNode) string {
	if nodes == nil {
		return ""
	}
	var b strings.Builder
	for _, n := range *nodes {
		if n.Metrics == nil || len(*n.Metrics) == 0 {
			continue
		}
		_, _ = fmt.Fprintf(&b, "(%d) %s\n", util.Deref(n.NodeId), util.Deref(n.NodeName))
		for _, m := range *n.Metrics {
			v := strings.Join(strings.Fields(util.Deref(m.Value)), " ")
			_, _ = fmt.Fprintf(&b, "  %s: %s\n", util.Deref(m.Name), v)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// collectJobIds returns all job IDs (success + failed + running) from a SQL execution.
func collectJobIds(e *client.SQLExecution) []int {
	var ids []int
	for _, p := range []*[]int{e.SuccessJobIds, e.FailedJobIds, e.RunningJobIds} {
		if p != nil {
			ids = append(ids, *p...)
		}
	}
	return ids
}

// fetchSQLJobs fetches all jobs and returns those matching the given IDs, sorted failed-first then by duration desc.
func fetchSQLJobs(ctx context.Context, c client.ClientWithResponsesInterface, ids []int) ([]client.Job, error) {
	resp, err := c.ListJobsWithResponse(ctx, appID, &client.ListJobsParams{})
	if err != nil {
		return nil, err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return nil, err
	}
	want := make(map[int]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	var matched []client.Job
	for _, j := range *body {
		if want[util.Deref(j.JobId)] {
			matched = append(matched, j)
		}
	}
	sortJobs(matched, "")
	return matched, nil
}

func formatSQLJobs(w io.Writer, jobs []client.Job) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ID\tSTATUS\tDURATION\tTASKS\tFAILED\tSTAGES")
	for _, j := range jobs {
		stages := util.FormatIntSlice(j.StageIds)
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%d\t%s\n",
			util.Deref(j.JobId),
			util.Deref(j.Status),
			jobDuration(j).Truncate(time.Millisecond),
			util.Deref(j.NumTasks),
			util.Deref(j.NumFailedTasks),
			stages,
		)
	}
	_ = tw.Flush()
}

func getSQLExecution(cmd *cobra.Command, c client.ClientWithResponsesInterface, id int, showPlan bool, showInitialPlan bool, showSummary bool) error {
	params := &client.GetSQLExecutionParams{}
	resp, err := c.GetSQLExecutionWithResponse(cmd.Context(), appID, id, params)
	if err != nil {
		return err
	}
	e, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}

	var jobs []client.Job
	var stages *stageAggregation
	if showSummary {
		ids := collectJobIds(e)
		if len(ids) > 0 {
			jobs, err = fetchSQLJobs(cmd.Context(), c, ids)
			if err != nil {
				return err
			}
			stagesResp, err := c.ListStagesWithResponse(cmd.Context(), appID, &client.ListStagesParams{})
			if err != nil {
				return err
			}
			if stagesResp.JSON200 != nil {
				agg := aggStages(*stagesResp.JSON200, stageIDsFromJobs(jobs))
				stages = &agg
			}
		}
	}

	var output any = e
	if showSummary {
		output = sqlDetail{
			Execution: sqlSummaryExec{
				ID: e.Id, Status: e.Status, Description: e.Description,
				SubmissionTime: e.SubmissionTime, Duration: e.Duration,
				SuccessJobIds: e.SuccessJobIds, FailedJobIds: e.FailedJobIds,
				RunningJobIds: e.RunningJobIds,
			},
			Jobs:   jobs,
			Stages: stages,
		}
	}

	return util.PrintOutput(cmd.OutOrStdout(), output, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "Execution ID:\t%d\n", util.Deref(e.Id))
		_, _ = fmt.Fprintf(tw, "Status:\t%s\n", util.Deref(e.Status))
		_, _ = fmt.Fprintf(tw, "Description:\t%s\n", util.Deref(e.Description))
		_, _ = fmt.Fprintf(tw, "Submitted:\t%s\n", util.Deref(e.SubmissionTime))
		_, _ = fmt.Fprintf(tw, "Duration:\t%s\n", sqlDuration(*e).Truncate(time.Millisecond))
		_, _ = fmt.Fprintf(tw, "Success Jobs:\t%s\n", util.FormatIntSlice(e.SuccessJobIds))
		_, _ = fmt.Fprintf(tw, "Failed Jobs:\t%s\n", util.FormatIntSlice(e.FailedJobIds))
		_, _ = fmt.Fprintf(tw, "Running Jobs:\t%s\n", util.FormatIntSlice(e.RunningJobIds))
		_ = tw.Flush()
		if showPlan && e.PlanDescription != nil && *e.PlanDescription != "" {
			_, _ = fmt.Fprintf(tw, "\nPlan:\n")
			_ = tw.Flush()
			plan := *e.PlanDescription
			if !showInitialPlan {
				plan = stripInitialPlans(plan)
			}
			_, _ = fmt.Fprintln(w, plan)
		}
		if showPlan {
			if metrics := formatNodeMetrics(e.Nodes); metrics != "" {
				_, _ = fmt.Fprintf(w, "\nMetrics:\n")
				_, _ = fmt.Fprint(w, metrics)
			}
		}
		if len(jobs) > 0 {
			_, _ = fmt.Fprintf(w, "\nJobs (%d):\n", len(jobs))
			formatSQLJobs(w, jobs)
		}
		if stages != nil {
			_, _ = fmt.Fprintf(w, "\nAggregate Stage Metrics:\n")
			_, _ = fmt.Fprintf(w, "  Stages:        %d\n", stages.Count)
			_, _ = fmt.Fprintf(w, "  Tasks:         %d\n", stages.Tasks)
			_, _ = fmt.Fprintf(w, "  Input:         %s\n", util.FormatBytes(stages.InputBytes))
			_, _ = fmt.Fprintf(w, "  Shuffle Read:  %s\n", util.FormatBytes(stages.ShuffleRead))
			_, _ = fmt.Fprintf(w, "  Shuffle Write: %s\n", util.FormatBytes(stages.ShuffleWrite))
			_, _ = fmt.Fprintf(w, "  Spill (Disk):  %s\n", util.FormatBytes(stages.SpillDisk))
			_, _ = fmt.Fprintf(w, "  GC Time:       %s\n", util.FormatMsVal(stages.GCTime))
		}
		return tw.Flush()
	})
}

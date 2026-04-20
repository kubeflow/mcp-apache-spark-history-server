package cmd

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

func newStagesCmd() *cobra.Command {
	var status string
	var limit int
	var sortBy string
	var errors bool

	cmd := &cobra.Command{
		Use:     "stages [stageId]",
		Short:   "List or get stages for an application",
		PreRunE: requireAppID,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			if len(args) == 1 {
				stageId, err := strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("invalid stage ID: %s", args[0])
				}
				if errors {
					return getStageErrors(cmd, c, stageId)
				}
				return getStage(cmd, c, stageId)
			}
			params := &client.ListStagesParams{}
			if status != "" {
				s := client.ListStagesParamsStatus(status)
				params.Status = &s
			}
			return listStages(cmd, c, params, limit, sortBy)
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (active|complete|pending|failed)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max number of stages to return (0 for all)")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (failed-tasks|duration|id)")
	cmd.Flags().BoolVar(&errors, "errors", false, "Show only failed tasks with error messages (requires stageId)")
	return cmd
}

type stageRow struct {
	ID           int    `col:"ID"`
	Attempt      int    `col:"ATTEMPT"`
	Status       string `col:"STATUS"`
	Description  string `col:"DESCRIPTION"`
	Tasks        int    `col:"TASKS"`
	Failed       int    `col:"FAILED"`
	Duration     string `col:"DURATION"`
	Input        string `col:"INPUT"`
	ShuffleRead  string `col:"SHUFFLE_READ"`
	ShuffleWrite string `col:"SHUFFLE_WRITE"`
}

type taskErrorRow struct {
	Task     int64  `col:"TASK"`
	Attempt  int    `col:"ATTEMPT"`
	Executor string `col:"EXECUTOR"`
	Status   string `col:"STATUS"`
	Error    string `col:"ERROR"`
}

type stageDetailOutput struct {
	Stage       client.StageData           `json:"stage"`
	TaskSummary *client.TaskMetricsSummary `json:"taskSummary,omitempty"`
}

var stageStatusPriority = map[string]int{
	"FAILED":   0,
	"COMPLETE": 1,
	"ACTIVE":   2,
	"PENDING":  3,
	"SKIPPED":  4,
}

func stageDuration(s client.StageData) time.Duration {
	return util.SparkDuration(s.SubmissionTime, s.CompletionTime)
}

func sortStages(stages []client.StageData, sortBy string) {
	if sortBy != "" {
		slices.SortFunc(stages, func(a, b client.StageData) int {
			switch sortBy {
			case "failed-tasks":
				return -cmp.Compare(util.Deref(a.NumFailedTasks), util.Deref(b.NumFailedTasks))
			case "duration":
				return -cmp.Compare(stageDuration(a), stageDuration(b))
			case "id":
				return -cmp.Compare(util.Deref(a.StageId), util.Deref(b.StageId))
			}
			return 0
		})
		return
	}
	// default: failed status first, then by duration desc
	slices.SortFunc(stages, func(a, b client.StageData) int {
		pa := stageStatusPriority[string(util.Deref(a.Status))]
		pb := stageStatusPriority[string(util.Deref(b.Status))]
		if c := cmp.Compare(pa, pb); c != 0 {
			return c
		}
		return -cmp.Compare(stageDuration(a), stageDuration(b))
	})
}

func stageDesc(s client.StageData) string {
	if d := util.Deref(s.Description); d != "" {
		return d
	}
	return util.Deref(s.Name)
}

func listStages(cmd *cobra.Command, c client.ClientWithResponsesInterface, params *client.ListStagesParams, limit int, sortBy string) error {
	resp, err := c.ListStagesWithResponse(cmd.Context(), appID, params)
	if err != nil {
		return err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}

	stages := *body
	sortStages(stages, sortBy)

	stages, total := util.ApplyLimit(stages, limit)

	return util.PrintOutput(cmd.OutOrStdout(), stages, outputFmt, func(w io.Writer) error {
		rows := make([]stageRow, len(stages))
		for i, s := range stages {
			rows[i] = stageRow{
				util.Deref(s.StageId), util.Deref(s.AttemptId), string(util.Deref(s.Status)), stageDesc(s),
				util.Deref(s.NumTasks), util.Deref(s.NumFailedTasks),
				stageDuration(s).Truncate(time.Millisecond).String(),
				util.DerefBytes(s.InputBytes), util.DerefBytes(s.ShuffleReadBytes), util.DerefBytes(s.ShuffleWriteBytes),
			}
		}
		if err := util.PrintTable(w, rows); err != nil {
			return err
		}
		util.PrintLimitFooter(w, limit, total, "stages")
		return nil
	})
}

func getStage(cmd *cobra.Command, c client.ClientWithResponsesInterface, stageId int) error {
	params := &client.ListStageAttemptsParams{}
	resp, err := c.ListStageAttemptsWithResponse(cmd.Context(), appID, stageId, params)
	if err != nil {
		return err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}

	attempts := *body
	if len(attempts) == 0 {
		return fmt.Errorf("no attempts found for stage %d", stageId)
	}
	s := attempts[len(attempts)-1]
	attemptId := util.Deref(s.AttemptId)

	// fetch task quantile summary
	quantiles := "0.25,0.5,0.75,1.0"
	tsParams := &client.GetTaskSummaryParams{Quantiles: &quantiles}
	tsResp, err := c.GetTaskSummaryWithResponse(cmd.Context(), appID, stageId, attemptId, tsParams)
	var taskMetricsSummary *client.TaskMetricsSummary
	if err == nil && tsResp.JSON200 != nil {
		taskMetricsSummary = tsResp.JSON200
	}

	out := stageDetailOutput{Stage: s, TaskSummary: taskMetricsSummary}

	return util.PrintOutput(cmd.OutOrStdout(), out, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "Stage ID:\t%d\n", util.Deref(s.StageId))
		_, _ = fmt.Fprintf(tw, "Attempt:\t%d\n", util.Deref(s.AttemptId))
		_, _ = fmt.Fprintf(tw, "Status:\t%s\n", util.Deref(s.Status))
		_, _ = fmt.Fprintf(tw, "Description:\t%s\n", stageDesc(s))
		_, _ = fmt.Fprintf(tw, "Name:\t%s\n", util.Deref(s.Name))
		_, _ = fmt.Fprintf(tw, "Duration:\t%s\n", stageDuration(s).Truncate(time.Millisecond))
		_, _ = fmt.Fprintf(tw, "Tasks:\t%d (failed: %d, killed: %d)\n",
			util.Deref(s.NumTasks), util.Deref(s.NumFailedTasks), util.Deref(s.NumKilledTasks))
		_, _ = fmt.Fprintf(tw, "Input:\t%s (%d records)\n",
			util.DerefBytes(s.InputBytes), util.Deref(s.InputRecords))
		_, _ = fmt.Fprintf(tw, "Output:\t%s (%d records)\n",
			util.DerefBytes(s.OutputBytes), util.Deref(s.OutputRecords))
		_, _ = fmt.Fprintf(tw, "Shuffle Read:\t%s (%d records)\n",
			util.DerefBytes(s.ShuffleReadBytes), util.Deref(s.ShuffleReadRecords))
		_, _ = fmt.Fprintf(tw, "Shuffle Write:\t%s (%d records)\n",
			util.DerefBytes(s.ShuffleWriteBytes), util.Deref(s.ShuffleWriteRecords))
		_, _ = fmt.Fprintf(tw, "Memory Spilled:\t%s\n", util.DerefBytes(s.MemoryBytesSpilled))
		_, _ = fmt.Fprintf(tw, "Disk Spilled:\t%s\n", util.DerefBytes(s.DiskBytesSpilled))
		_, _ = fmt.Fprintf(tw, "GC Time:\t%dms\n", util.Deref(s.JvmGcTime))
		_, _ = fmt.Fprintf(tw, "Pool:\t%s\n", util.Deref(s.SchedulingPool))
		if err := tw.Flush(); err != nil {
			return err
		}

		if taskMetricsSummary != nil {
			_, _ = fmt.Fprintf(w, "\nTask Quantiles (p25 / p50 / p75 / max):\n")
			tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
			fmtMs := func(f float32) string { return util.FormatMsVal(int64(f)) }
			fmtB := func(f float32) string { return util.FormatBytes(int64(f)) }
			printQuantileRow(tw, "Duration", taskMetricsSummary.Duration, fmtMs)
			printQuantileRow(tw, "GC Time", taskMetricsSummary.JvmGcTime, fmtMs)
			printQuantileRow(tw, "Scheduler Delay", taskMetricsSummary.SchedulerDelay, fmtMs)
			printQuantileRow(tw, "Peak Exec Memory", taskMetricsSummary.PeakExecutionMemory, fmtB)
			if m := taskMetricsSummary.InputMetrics; m != nil {
				printQuantileRow(tw, "Input", m.BytesRead, fmtB)
			}
			if m := taskMetricsSummary.OutputMetrics; m != nil {
				printQuantileRow(tw, "Output", m.BytesWritten, fmtB)
			}
			if m := taskMetricsSummary.ShuffleReadMetrics; m != nil {
				printQuantileRow(tw, "Shuffle Read", m.ReadBytes, fmtB)
			}
			if m := taskMetricsSummary.ShuffleWriteMetrics; m != nil {
				printQuantileRow(tw, "Shuffle Write", m.WriteBytes, fmtB)
			}
			printQuantileRow(tw, "Disk Spill", taskMetricsSummary.DiskBytesSpilled, fmtB)
			printQuantileRow(tw, "Memory Spill", taskMetricsSummary.MemoryBytesSpilled, fmtB)
			return tw.Flush()
		}
		return nil
	})
}

func printQuantileRow(w io.Writer, label string, vals *[]float32, fmtFn func(float32) string) {
	v := util.Deref(vals)
	if len(v) != 4 {
		return
	}
	_, _ = fmt.Fprintf(w, "  %s:\t%s\t/ %s\t/ %s\t/ %s\n", label, fmtFn(v[0]), fmtFn(v[1]), fmtFn(v[2]), fmtFn(v[3]))
}

func getStageErrors(cmd *cobra.Command, c client.ClientWithResponsesInterface, stageId int) error {
	// get latest attempt ID
	params := &client.ListStageAttemptsParams{}
	resp, err := c.ListStageAttemptsWithResponse(cmd.Context(), appID, stageId, params)
	if err != nil {
		return err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}
	attempts := *body
	if len(attempts) == 0 {
		return fmt.Errorf("no attempts found for stage %d", stageId)
	}
	attemptId := util.Deref(attempts[len(attempts)-1].AttemptId)

	// fetch failed tasks
	status := client.ListTasksParamsStatus("failed")
	taskParams := &client.ListTasksParams{Status: &status}
	taskResp, err := c.ListTasksWithResponse(cmd.Context(), appID, stageId, attemptId, taskParams)
	if err != nil {
		return err
	}
	taskBody, err := util.CheckResponse(taskResp.JSON200, taskResp.HTTPResponse.Status)
	if err != nil {
		return err
	}

	tasks := *taskBody
	return util.PrintOutput(cmd.OutOrStdout(), tasks, outputFmt, func(w io.Writer) error {
		if len(tasks) == 0 {
			_, _ = fmt.Fprintln(w, "No failed tasks.")
			return nil
		}
		rows := make([]taskErrorRow, len(tasks))
		for i, t := range tasks {
			errMsg := util.Deref(t.ErrorMessage)
			if j := strings.IndexByte(errMsg, '\n'); j >= 0 {
				errMsg = errMsg[:j]
			}
			rows[i] = taskErrorRow{util.Deref(t.TaskId), util.Deref(t.Attempt), util.Deref(t.ExecutorId), util.Deref(t.Status), errMsg}
		}
		return util.PrintTable(w, rows)
	})
}

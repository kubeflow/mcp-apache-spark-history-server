package cmd

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"text/tabwriter"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

// LogsResult is the emitted shape for `shs logs`.
// Keys stdout / stderr / spark.log are Spark REST API conventions; surfaced as-is.
type LogsResult struct {
	ExecutorId     string            `json:"executorId"`
	TaskId         *int64            `json:"taskId,omitempty"`
	StageId        *int              `json:"stageId,omitempty"`
	StageAttemptId *int              `json:"stageAttemptId,omitempty"`
	Host           string            `json:"host,omitempty"`
	Logs           map[string]string `json:"logs"`
}

func newLogsCmd() *cobra.Command {
	var executorID string
	var taskID int64
	var stageID int
	var stageAttemptID int

	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Get stdout/stderr/spark.log URLs for an executor or task",
		Long: `Get stdout/stderr/spark.log URLs for an executor or a task.

Exactly one of --executor or --task must be provided.

When --task is set without --stage, all stages are scanned for the matching
taskId. This can be slow on large applications; prefer passing --stage when
known. A Spark taskId is globally unique across an application, so at most
one stage will match.`,
		Example: `  # By executor ID
  shs logs -a APP --attempt 1 --executor 1

  # By task ID (auto-scans stages)
  shs logs -a APP --attempt 1 --task 145

  # By task ID with known stage (fastest)
  shs logs -a APP --attempt 1 --task 145 --stage 12`,
		PreRunE: requireAppID,
		RunE: func(cmd *cobra.Command, args []string) error {
			hasExec := executorID != ""
			hasTask := cmd.Flags().Changed("task")
			if hasExec == hasTask {
				return fmt.Errorf("exactly one of --executor or --task is required")
			}
			if !hasTask && cmd.Flags().Changed("stage") {
				return fmt.Errorf("--stage is only valid with --task")
			}
			if !hasTask && cmd.Flags().Changed("stage-attempt") {
				return fmt.Errorf("--stage-attempt is only valid with --task")
			}

			c, err := newClient()
			if err != nil {
				return err
			}

			var result *LogsResult
			if hasExec {
				result, err = resolveByExecutor(cmd.Context(), c, executorID)
			} else {
				var stagePtr *int
				if cmd.Flags().Changed("stage") {
					stagePtr = &stageID
				}
				var saPtr *int
				if cmd.Flags().Changed("stage-attempt") {
					saPtr = &stageAttemptID
				}
				result, err = resolveByTask(cmd.Context(), c, taskID, stagePtr, saPtr)
			}
			if err != nil {
				return err
			}

			return util.PrintOutput(cmd.OutOrStdout(), result, outputFmt, func(w io.Writer) error {
				tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
				_, _ = fmt.Fprintf(tw, "EXECUTOR:\t%s\n", result.ExecutorId)
				if result.TaskId != nil {
					_, _ = fmt.Fprintf(tw, "TASK:\t%d\n", *result.TaskId)
				}
				if result.StageId != nil {
					saStr := ""
					if result.StageAttemptId != nil {
						saStr = fmt.Sprintf("/%d", *result.StageAttemptId)
					}
					_, _ = fmt.Fprintf(tw, "STAGE:\t%d%s\n", *result.StageId, saStr)
				}
				if result.Host != "" {
					_, _ = fmt.Fprintf(tw, "HOST:\t%s\n", result.Host)
				}
				preferred := []string{"spark.log", "stderr", "stdout"}
				seen := map[string]bool{}
				for _, k := range preferred {
					if v, ok := result.Logs[k]; ok {
						_, _ = fmt.Fprintf(tw, "%s:\t%s\n", k, v)
						seen[k] = true
					}
				}
				extra := make([]string, 0, len(result.Logs))
				for k := range result.Logs {
					if !seen[k] {
						extra = append(extra, k)
					}
				}
				sort.Strings(extra)
				for _, k := range extra {
					_, _ = fmt.Fprintf(tw, "%s:\t%s\n", k, result.Logs[k])
				}
				return tw.Flush()
			})
		},
	}

	cmd.Flags().StringVarP(&executorID, "executor", "e", "", "Executor ID (mutually exclusive with --task)")
	cmd.Flags().Int64VarP(&taskID, "task", "t", 0, "Task ID (mutually exclusive with --executor)")
	cmd.Flags().IntVar(&stageID, "stage", 0, "Stage ID (optional; narrows --task search)")
	cmd.Flags().IntVar(&stageAttemptID, "stage-attempt", 0, "Stage attempt ID (optional; defaults to latest attempt)")
	return cmd
}

func resolveByExecutor(ctx context.Context, c client.ClientWithResponsesInterface, id string) (*LogsResult, error) {
	resp, err := c.ListAllExecutorsWithResponse(ctx, appID)
	if err != nil {
		return nil, err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return nil, err
	}
	execs := util.Deref(body)
	idx := slices.IndexFunc(execs, func(e client.Executor) bool {
		return util.Deref(e.Id) == id
	})
	if idx == -1 {
		return nil, fmt.Errorf("executor %q not found", id)
	}
	e := execs[idx]
	logs := util.Deref(e.ExecutorLogs)
	if len(logs) == 0 {
		return nil, fmt.Errorf("executor %q has no log URLs (not yet assigned, or already cleaned up)", id)
	}
	return &LogsResult{
		ExecutorId: util.Deref(e.Id),
		Host:       util.Deref(e.HostPort),
		Logs:       logs,
	}, nil
}

func resolveByTask(ctx context.Context, c client.ClientWithResponsesInterface, taskID int64, stageID *int, stageAttemptID *int) (*LogsResult, error) {
	if stageID != nil {
		return findTaskInStage(ctx, c, taskID, *stageID, stageAttemptID)
	}
	// No stage hint: scan all stages. taskId is globally unique in a Spark app,
	// so at most one stage will contain it.
	stagesResp, err := c.ListStagesWithResponse(ctx, appID, &client.ListStagesParams{})
	if err != nil {
		return nil, err
	}
	stagesBody, err := util.CheckResponse(stagesResp.JSON200, stagesResp.HTTPResponse.Status)
	if err != nil {
		return nil, err
	}
	seen := map[int]bool{}
	for _, s := range util.Deref(stagesBody) {
		sid := util.Deref(s.StageId)
		if seen[sid] {
			continue
		}
		seen[sid] = true
		res, err := findTaskInStage(ctx, c, taskID, sid, nil)
		if err == nil {
			return res, nil
		}
		if !isTaskNotFound(err) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("task %d not found in any stage of application %s", taskID, appID)
}

// errTaskNotFound is a sentinel used internally when a given stage does not
// contain the requested taskId; resolveByTask keeps iterating on this error.
type errTaskNotFound struct{ taskID int64 }

func (e errTaskNotFound) Error() string {
	return fmt.Sprintf("task %d not found in stage", e.taskID)
}

func isTaskNotFound(err error) bool {
	_, ok := err.(errTaskNotFound)
	return ok
}

func findTaskInStage(ctx context.Context, c client.ClientWithResponsesInterface, taskID int64, stageID int, stageAttempt *int) (*LogsResult, error) {
	attemptID, err := resolveStageAttempt(ctx, c, stageID, stageAttempt)
	if err != nil {
		return nil, err
	}

	const pageSize = 1000
	offset := 0
	for {
		length := pageSize
		params := &client.ListTasksParams{
			Offset: &offset,
			Length: &length,
		}
		resp, err := c.ListTasksWithResponse(ctx, appID, stageID, attemptID, params)
		if err != nil {
			return nil, err
		}
		body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
		if err != nil {
			return nil, err
		}
		tasks := util.Deref(body)
		if len(tasks) == 0 {
			return nil, errTaskNotFound{taskID: taskID}
		}
		for i := range tasks {
			t := &tasks[i]
			if util.Deref(t.TaskId) == taskID {
				logs := util.Deref(t.ExecutorLogs)
				if len(logs) == 0 {
					return nil, fmt.Errorf("task %d found in stage %d/%d but has no log URLs (running or cleaned up)", taskID, stageID, attemptID)
				}
				stageCopy := stageID
				attemptCopy := attemptID
				return &LogsResult{
					ExecutorId:     util.Deref(t.ExecutorId),
					TaskId:         t.TaskId,
					StageId:        &stageCopy,
					StageAttemptId: &attemptCopy,
					Host:           util.Deref(t.Host),
					Logs:           logs,
				}, nil
			}
		}
		if len(tasks) < pageSize {
			return nil, errTaskNotFound{taskID: taskID}
		}
		offset += pageSize
	}
}

func resolveStageAttempt(ctx context.Context, c client.ClientWithResponsesInterface, stageID int, explicit *int) (int, error) {
	if explicit != nil {
		return *explicit, nil
	}
	resp, err := c.ListStageAttemptsWithResponse(ctx, appID, stageID, &client.ListStageAttemptsParams{})
	if err != nil {
		return 0, err
	}
	if resp.HTTPResponse.StatusCode == 404 {
		return 0, fmt.Errorf("stage %d not found", stageID)
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return 0, err
	}
	attempts := util.Deref(body)
	if len(attempts) == 0 {
		return 0, fmt.Errorf("no attempts found for stage %d", stageID)
	}
	latest := 0
	for _, a := range attempts {
		if id := util.Deref(a.AttemptId); id > latest {
			latest = id
		}
	}
	return latest, nil
}

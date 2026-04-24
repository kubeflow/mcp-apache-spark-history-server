package cmd

import (
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

type stageSide struct {
	App           string                     `json:"app"`
	StageID       int                        `json:"stageId"`
	AttemptID     int                        `json:"attemptId"`
	Status        string                     `json:"status"`
	Description   string                     `json:"description"`
	DurationMs    int64                      `json:"durationMs"`
	Tasks         int                        `json:"tasks"`
	FailedTasks   int                        `json:"failedTasks"`
	InputBytes    int64                      `json:"inputBytes"`
	OutputBytes   int64                      `json:"outputBytes"`
	ShuffleRead   int64                      `json:"shuffleRead"`
	ShuffleWrite  int64                      `json:"shuffleWrite"`
	SpillDisk     int64                      `json:"spillDisk"`
	SpillMemory   int64                      `json:"spillMemory"`
	GCTimeMs      int64                      `json:"gcTimeMs"`
	TaskQuantiles *client.TaskMetricsSummary `json:"taskQuantiles,omitempty"`
}

func newCompareStagesCmd() *cobra.Command {
	var appA, appB string
	var serverA, serverB string

	cmd := &cobra.Command{
		Use:   "stages STAGE_A STAGE_B",
		Short: "Compare two stages across applications: metrics and task quantiles",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("two stage IDs are required")
			}
			idA, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid stage ID A: %s", args[0])
			}
			idB, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("invalid stage ID B: %s", args[1])
			}
			return runCompareStages(cmd, serverA, appA, idA, serverB, appB, idB)
		},
	}

	cmd.Flags().StringVar(&appA, "app-a", "", "First application ID (required)")
	cmd.Flags().StringVar(&appB, "app-b", "", "Second application ID (required)")
	cmd.Flags().StringVar(&serverA, "server-a", "", "Server name for app A (overrides --server)")
	cmd.Flags().StringVar(&serverB, "server-b", "", "Server name for app B (overrides --server)")
	_ = cmd.MarkFlagRequired("app-a")
	_ = cmd.MarkFlagRequired("app-b")
	return cmd
}

func fetchStageSide(cmd *cobra.Command, c client.ClientWithResponsesInterface, app string, stageId int) (*stageSide, error) {
	resp, err := c.ListStageAttemptsWithResponse(cmd.Context(), app, stageId, &client.ListStageAttemptsParams{})
	if err != nil {
		return nil, err
	}
	if resp.HTTPResponse.StatusCode == 404 {
		return nil, fmt.Errorf("stage %d not found in app %s (may have been skipped by AQE)", stageId, app)
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return nil, err
	}
	attempts := *body
	if len(attempts) == 0 {
		return nil, fmt.Errorf("no attempts found for stage %d in app %s", stageId, app)
	}
	s := attempts[len(attempts)-1]
	attemptId := util.Deref(s.AttemptId)

	quantiles := "0.25,0.5,0.75,1.0"
	tsParams := &client.GetTaskSummaryParams{Quantiles: &quantiles}
	tsResp, err := c.GetTaskSummaryWithResponse(cmd.Context(), app, stageId, attemptId, tsParams)
	var taskQuantiles *client.TaskMetricsSummary
	if err == nil && tsResp.JSON200 != nil {
		taskQuantiles = tsResp.JSON200
	}

	dur := stageDuration(s)
	return &stageSide{
		App:           app,
		StageID:       util.Deref(s.StageId),
		AttemptID:     attemptId,
		Status:        string(util.Deref(s.Status)),
		Description:   stageDesc(s),
		DurationMs:    dur.Milliseconds(),
		Tasks:         util.Deref(s.NumTasks),
		FailedTasks:   util.Deref(s.NumFailedTasks),
		InputBytes:    util.Deref(s.InputBytes),
		OutputBytes:   util.Deref(s.OutputBytes),
		ShuffleRead:   util.Deref(s.ShuffleReadBytes),
		ShuffleWrite:  util.Deref(s.ShuffleWriteBytes),
		SpillDisk:     util.Deref(s.DiskBytesSpilled),
		SpillMemory:   util.Deref(s.MemoryBytesSpilled),
		GCTimeMs:      util.Deref(s.JvmGcTime),
		TaskQuantiles: taskQuantiles,
	}, nil
}

func runCompareStages(cmd *cobra.Command, serverA, appA string, idA int, serverB, appB string, idB int) error {
	cA, cB, err := getClients(serverA, serverB)
	if err != nil {
		return err
	}

	sA, err := fetchStageSide(cmd, cA, appA, idA)
	if err != nil {
		return err
	}
	sB, err := fetchStageSide(cmd, cB, appB, idB)
	if err != nil {
		return err
	}

	r := struct {
		A stageSide `json:"a"`
		B stageSide `json:"b"`
	}{*sA, *sB}

	return util.PrintOutput(cmd.OutOrStdout(), r, outputFmt, func(w io.Writer) error {
		_, _ = fmt.Fprintf(w, "Stage A:  %s  Stage %d (attempt %d)\n", appA, sA.StageID, sA.AttemptID)
		_, _ = fmt.Fprintf(w, "Stage B:  %s  Stage %d (attempt %d)\n", appB, sB.StageID, sB.AttemptID)
		_, _ = fmt.Fprintf(w, "Desc:     A=%s  B=%s\n", sA.Description, sB.Description)
		_, _ = fmt.Fprintf(w, "Status:   A=%s  B=%s\n", sA.Status, sB.Status)

		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "\n\tA\tB\tDelta\n")

		durA := time.Duration(sA.DurationMs) * time.Millisecond
		durB := time.Duration(sB.DurationMs) * time.Millisecond
		_, _ = fmt.Fprintf(tw, "Duration:\t%s\t%s\t%s\n", durA.Truncate(time.Millisecond), durB.Truncate(time.Millisecond), fmtDelta(durB-durA))
		printIntRow(tw, "Tasks", sA.Tasks, sB.Tasks)
		printIntRow(tw, "Failed Tasks", sA.FailedTasks, sB.FailedTasks)
		printBytesRow(tw, "Input", sA.InputBytes, sB.InputBytes)
		printBytesRow(tw, "Output", sA.OutputBytes, sB.OutputBytes)
		printBytesRow(tw, "Shuffle Read", sA.ShuffleRead, sB.ShuffleRead)
		printBytesRow(tw, "Shuffle Write", sA.ShuffleWrite, sB.ShuffleWrite)
		printBytesRow(tw, "Spill (Disk)", sA.SpillDisk, sB.SpillDisk)
		printBytesRow(tw, "Spill (Memory)", sA.SpillMemory, sB.SpillMemory)
		_, _ = fmt.Fprintf(tw, "GC Time:\t%s\t%s\t%s\n",
			util.FormatMsVal(sA.GCTimeMs), util.FormatMsVal(sB.GCTimeMs),
			fmtDelta(durMs(sB.GCTimeMs-sA.GCTimeMs)))
		if err := tw.Flush(); err != nil {
			return err
		}

		if sA.TaskQuantiles != nil || sB.TaskQuantiles != nil {
			_, _ = fmt.Fprintf(w, "\nTask Quantiles:\n")
			tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
			_, _ = fmt.Fprintf(tw, "\tp25\tp50\tp75\tmax\n")
			fmtMs := func(f float32) string { return util.FormatMsVal(int64(f)) }
			fmtB := func(f float32) string { return util.FormatBytes(int64(f)) }
			deltaMs := func(f float32) string { return fmtDelta(time.Duration(int64(f)) * time.Millisecond) }
			deltaB := func(f float32) string { return fmtDeltaBytes(int64(f)) }

			qA, qB := sA.TaskQuantiles, sB.TaskQuantiles
			printQuantileCompare(tw, "Duration", topQuantile(qA, func(q *client.TaskMetricsSummary) *[]float32 { return q.Duration }), topQuantile(qB, func(q *client.TaskMetricsSummary) *[]float32 { return q.Duration }), fmtMs, deltaMs)
			printQuantileCompare(tw, "GC Time", topQuantile(qA, func(q *client.TaskMetricsSummary) *[]float32 { return q.JvmGcTime }), topQuantile(qB, func(q *client.TaskMetricsSummary) *[]float32 { return q.JvmGcTime }), fmtMs, deltaMs)
			printQuantileCompare(tw, "Scheduler Delay", topQuantile(qA, func(q *client.TaskMetricsSummary) *[]float32 { return q.SchedulerDelay }), topQuantile(qB, func(q *client.TaskMetricsSummary) *[]float32 { return q.SchedulerDelay }), fmtMs, deltaMs)
			printQuantileCompare(tw, "Peak Exec Memory", topQuantile(qA, func(q *client.TaskMetricsSummary) *[]float32 { return q.PeakExecutionMemory }), topQuantile(qB, func(q *client.TaskMetricsSummary) *[]float32 { return q.PeakExecutionMemory }), fmtB, deltaB)
			printQuantileCompare(tw, "Input", inputQuantile(qA), inputQuantile(qB), fmtB, deltaB)
			printQuantileCompare(tw, "Output", outputQuantile(qA), outputQuantile(qB), fmtB, deltaB)
			printQuantileCompare(tw, "Shuffle Read", shuffleReadQuantile(qA), shuffleReadQuantile(qB), fmtB, deltaB)
			printQuantileCompare(tw, "Shuffle Write", shuffleWriteQuantile(qA), shuffleWriteQuantile(qB), fmtB, deltaB)
			printQuantileCompare(tw, "Disk Spill", topQuantile(qA, func(q *client.TaskMetricsSummary) *[]float32 { return q.DiskBytesSpilled }), topQuantile(qB, func(q *client.TaskMetricsSummary) *[]float32 { return q.DiskBytesSpilled }), fmtB, deltaB)
			printQuantileCompare(tw, "Memory Spill", topQuantile(qA, func(q *client.TaskMetricsSummary) *[]float32 { return q.MemoryBytesSpilled }), topQuantile(qB, func(q *client.TaskMetricsSummary) *[]float32 { return q.MemoryBytesSpilled }), fmtB, deltaB)
			return tw.Flush()
		}
		return nil
	})
}

func topQuantile(q *client.TaskMetricsSummary, get func(*client.TaskMetricsSummary) *[]float32) []float32 {
	if q == nil {
		return nil
	}
	return util.Deref(get(q))
}

func inputQuantile(q *client.TaskMetricsSummary) []float32 {
	if q == nil || q.InputMetrics == nil {
		return nil
	}
	return util.Deref(q.InputMetrics.BytesRead)
}

func outputQuantile(q *client.TaskMetricsSummary) []float32 {
	if q == nil || q.OutputMetrics == nil {
		return nil
	}
	return util.Deref(q.OutputMetrics.BytesWritten)
}

func shuffleReadQuantile(q *client.TaskMetricsSummary) []float32 {
	if q == nil || q.ShuffleReadMetrics == nil {
		return nil
	}
	return util.Deref(q.ShuffleReadMetrics.ReadBytes)
}

func shuffleWriteQuantile(q *client.TaskMetricsSummary) []float32 {
	if q == nil || q.ShuffleWriteMetrics == nil {
		return nil
	}
	return util.Deref(q.ShuffleWriteMetrics.WriteBytes)
}

func printQuantileCompare(tw io.Writer, label string, vA, vB []float32, fmtFn func(float32) string, deltaFn func(float32) string) {
	// skip only when neither side has valid quantiles
	if len(vA) != 4 && len(vB) != 4 {
		return
	}
	fmtVals := func(v []float32) string {
		if len(v) != 4 {
			return "-"
		}
		return fmt.Sprintf("%s\t%s\t%s\t%s", fmtFn(v[0]), fmtFn(v[1]), fmtFn(v[2]), fmtFn(v[3]))
	}
	_, _ = fmt.Fprintf(tw, "  %s:\t\t\t\t\n", label)
	_, _ = fmt.Fprintf(tw, "    A:\t%s\n", fmtVals(vA))
	_, _ = fmt.Fprintf(tw, "    B:\t%s\n", fmtVals(vB))
	if len(vA) == 4 && len(vB) == 4 {
		_, _ = fmt.Fprintf(tw, "    Delta:\t%s\t%s\t%s\t%s\n",
			deltaFn(vB[0]-vA[0]), deltaFn(vB[1]-vA[1]), deltaFn(vB[2]-vA[2]), deltaFn(vB[3]-vA[3]))
	}
}

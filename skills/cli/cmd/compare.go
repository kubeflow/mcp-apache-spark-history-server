package cmd

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

func newCompareCmd() *cobra.Command {
	var appA, appB string
	var serverA, serverB string

	cmd := &cobra.Command{
		Use:   "compare EXEC_ID_A EXEC_ID_B",
		Short: "Compare two SQL executions across applications",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			idA, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid execution ID A: %s", args[0])
			}
			idB, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("invalid execution ID B: %s", args[1])
			}
			if appA == "" || appB == "" {
				return fmt.Errorf("both -a and -b flags are required")
			}
			return runCompare(cmd, serverA, appA, idA, serverB, appB, idB)
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

type side struct {
	App          string `json:"app"`
	SQLID        int    `json:"sqlId"`
	Description  string `json:"description"`
	Status       string `json:"status"`
	DurationMs   int64  `json:"durationMs"`
	Jobs         int    `json:"jobs"`
	Stages       int    `json:"stages"`
	Tasks        int    `json:"tasks"`
	StageTimeMs  int64  `json:"stageTimeMs"`
	InputBytes   int64  `json:"inputBytes"`
	ShuffleRead  int64  `json:"shuffleRead"`
	ShuffleWrite int64  `json:"shuffleWrite"`
	SpillDisk    int64  `json:"spillDisk"`
	GCTimeMs     int64  `json:"gcTimeMs"`
}

func fetchSQLAndJobs(ctx context.Context, c client.ClientWithResponsesInterface, app string, sqlID int) (*client.SQLExecution, []client.Job, error) {
	sqlResp, err := c.GetSQLExecutionWithResponse(ctx, app, sqlID, &client.GetSQLExecutionParams{})
	if err != nil {
		return nil, nil, err
	}
	if sqlResp.JSON200 == nil {
		return nil, nil, fmt.Errorf("app %s sql %d: %s", app, sqlID, sqlResp.HTTPResponse.Status)
	}
	sql := sqlResp.JSON200

	jobResp, err := c.ListJobsWithResponse(ctx, app, &client.ListJobsParams{})
	if err != nil {
		return nil, nil, err
	}
	if jobResp.JSON200 == nil {
		return nil, nil, fmt.Errorf("app %s jobs: %s", app, jobResp.HTTPResponse.Status)
	}

	// filter jobs to this SQL execution
	want := map[int]bool{}
	for _, p := range []*[]int{sql.SuccessJobIds, sql.FailedJobIds, sql.RunningJobIds} {
		if p != nil {
			for _, id := range *p {
				want[id] = true
			}
		}
	}
	var jobs []client.Job
	for _, j := range *jobResp.JSON200 {
		if want[util.Deref(j.JobId)] {
			jobs = append(jobs, j)
		}
	}
	return sql, jobs, nil
}

func getClients(serverA, serverB string) (client.ClientWithResponsesInterface, client.ClientWithResponsesInterface, error) {
	clientA, err := util.NewSHSClient(configPath, util.WithTimeout(timeout), util.WithServer(serverA))
	if err != nil {
		return nil, nil, err
	}
	clientB, err := util.NewSHSClient(configPath, util.WithTimeout(timeout), util.WithServer(serverB))
	if err != nil {
		return nil, nil, err
	}
	return clientA, clientB, nil
}

func runCompare(cmd *cobra.Command, serverA, appA string, idA int, serverB, appB string, idB int) error {
	cA, cB, err := getClients(serverA, serverB)
	if err != nil {
		return err
	}
	sqlA, jobsA, err := fetchSQLAndJobs(cmd.Context(), cA, appA, idA)
	if err != nil {
		return err
	}
	sqlB, jobsB, err := fetchSQLAndJobs(cmd.Context(), cB, appB, idB)
	if err != nil {
		return err
	}

	// fetch stages
	stagesRespA, err := cA.ListStagesWithResponse(cmd.Context(), appA, &client.ListStagesParams{})
	if err != nil {
		return err
	}
	if stagesRespA.JSON200 == nil {
		return fmt.Errorf("app %s stages: %s", appA, stagesRespA.HTTPResponse.Status)
	}
	stagesRespB, err := cB.ListStagesWithResponse(cmd.Context(), appB, &client.ListStagesParams{})
	if err != nil {
		return err
	}
	if stagesRespB.JSON200 == nil {
		return fmt.Errorf("app %s stages: %s", appB, stagesRespB.HTTPResponse.Status)
	}

	aggA := aggStages(*stagesRespA.JSON200, stageIDsFromJobs(jobsA))
	aggB := aggStages(*stagesRespB.JSON200, stageIDsFromJobs(jobsB))

	mkSide := func(app string, sql *client.SQLExecution, jobs []client.Job, agg stageAggregation) side {
		return side{
			App: app, SQLID: util.Deref(sql.Id),
			Description: util.Deref(sql.Description), Status: string(util.Deref(sql.Status)),
			DurationMs: util.Deref(sql.Duration), Jobs: len(jobs),
			Stages: agg.Count, Tasks: agg.Tasks, StageTimeMs: agg.Duration.Milliseconds(),
			InputBytes: agg.InputBytes, ShuffleRead: agg.ShuffleRead,
			ShuffleWrite: agg.ShuffleWrite, SpillDisk: agg.SpillDisk, GCTimeMs: agg.GCTime,
		}
	}
	r := struct {
		A side `json:"a"`
		B side `json:"b"`
	}{mkSide(appA, sqlA, jobsA, aggA), mkSide(appB, sqlB, jobsB, aggB)}

	return util.PrintOutput(cmd.OutOrStdout(), r, outputFmt, func(w io.Writer) error {
		_, _ = fmt.Fprintf(w, "App A:  %s  (SQL %d)\n", appA, util.Deref(sqlA.Id))
		_, _ = fmt.Fprintf(w, "App B:  %s  (SQL %d)\n", appB, util.Deref(sqlB.Id))
		_, _ = fmt.Fprintf(w, "Query:  A=%s  B=%s\n", util.Deref(sqlA.Description), util.Deref(sqlB.Description))
		_, _ = fmt.Fprintf(w, "Status: A=%s  B=%s\n", util.Deref(sqlA.Status), util.Deref(sqlB.Status))

		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "\n\tA\tB\tDelta\n")

		durA, durB := sqlDuration(*sqlA), sqlDuration(*sqlB)
		_, _ = fmt.Fprintf(tw, "Duration:\t%s\t%s\t%s\n", durA.Truncate(time.Millisecond), durB.Truncate(time.Millisecond), fmtDelta(durB-durA))
		_, _ = fmt.Fprintf(tw, "Jobs:\t%d\t%d\t%+d\n", len(jobsA), len(jobsB), len(jobsB)-len(jobsA))
		_, _ = fmt.Fprintf(tw, "Stages:\t%d\t%d\t%+d\n", aggA.Count, aggB.Count, aggB.Count-aggA.Count)
		_, _ = fmt.Fprintf(tw, "Tasks:\t%d\t%d\t%+d\n", aggA.Tasks, aggB.Tasks, aggB.Tasks-aggA.Tasks)
		_, _ = fmt.Fprintf(tw, "Stage Time:\t%s\t%s\t%s\n", aggA.Duration.Truncate(time.Millisecond), aggB.Duration.Truncate(time.Millisecond), fmtDelta(aggB.Duration-aggA.Duration))
		_, _ = fmt.Fprintf(tw, "Input:\t%s\t%s\t%s\n", util.FormatBytes(aggA.InputBytes), util.FormatBytes(aggB.InputBytes), fmtDeltaBytes(aggB.InputBytes-aggA.InputBytes))
		_, _ = fmt.Fprintf(tw, "Shuffle Read:\t%s\t%s\t%s\n", util.FormatBytes(aggA.ShuffleRead), util.FormatBytes(aggB.ShuffleRead), fmtDeltaBytes(aggB.ShuffleRead-aggA.ShuffleRead))
		_, _ = fmt.Fprintf(tw, "Shuffle Write:\t%s\t%s\t%s\n", util.FormatBytes(aggA.ShuffleWrite), util.FormatBytes(aggB.ShuffleWrite), fmtDeltaBytes(aggB.ShuffleWrite-aggA.ShuffleWrite))
		_, _ = fmt.Fprintf(tw, "Spill (Disk):\t%s\t%s\t%s\n", util.FormatBytes(aggA.SpillDisk), util.FormatBytes(aggB.SpillDisk), fmtDeltaBytes(aggB.SpillDisk-aggA.SpillDisk))
		_, _ = fmt.Fprintf(tw, "GC Time:\t%s\t%s\t%s\n", util.FormatMsVal(aggA.GCTime), util.FormatMsVal(aggB.GCTime), fmtDelta(time.Duration(aggB.GCTime-aggA.GCTime)*time.Millisecond))

		return tw.Flush()
	})
}

func fmtDelta(d time.Duration) string {
	if d >= 0 {
		return "+" + d.Truncate(time.Millisecond).String()
	}
	return d.Truncate(time.Millisecond).String()
}

func fmtDeltaBytes(b int64) string {
	if b >= 0 {
		return "+" + util.FormatBytes(b)
	}
	return "-" + util.FormatBytes(-b)
}

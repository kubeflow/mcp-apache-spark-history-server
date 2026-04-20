package cmd

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

func newCompareSQLCmd() *cobra.Command {
	var appA, appB string
	var serverA, serverB string
	var showEnv, showPlans bool

	cmd := &cobra.Command{
		Use:   "sql [EXEC_ID_A EXEC_ID_B]",
		Short: "Compare two SQL executions, environments, or plans across applications",
		RunE: func(cmd *cobra.Command, args []string) error {
			if appA == "" || appB == "" {
				return fmt.Errorf("both --app-a and --app-b flags are required")
			}

			if showEnv {
				return runCompareEnv(cmd, serverA, appA, serverB, appB)
			}

			if len(args) != 2 {
				return fmt.Errorf("two execution IDs are required (unless using --env)")
			}
			idA, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid execution ID A: %s", args[0])
			}
			idB, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("invalid execution ID B: %s", args[1])
			}

			if showPlans {
				return runComparePlans(cmd, serverA, appA, idA, serverB, appB, idB)
			}

			return runCompare(cmd, serverA, appA, idA, serverB, appB, idB)
		},
	}

	cmd.Flags().StringVar(&appA, "app-a", "", "First application ID (required)")
	cmd.Flags().StringVar(&appB, "app-b", "", "Second application ID (required)")
	cmd.Flags().StringVar(&serverA, "server-a", "", "Server name for app A (overrides --server)")
	cmd.Flags().StringVar(&serverB, "server-b", "", "Server name for app B (overrides --server)")
	cmd.Flags().BoolVar(&showEnv, "env", false, "Compare environment configurations")
	cmd.Flags().BoolVar(&showPlans, "plans", false, "Compare SQL execution plan structure")
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

type envDiff struct {
	Key  string `json:"key"`
	ValA string `json:"a"`
	ValB string `json:"b"`
}

type envDiffResult struct {
	AppA  string    `json:"appA"`
	AppB  string    `json:"appB"`
	Diffs []envDiff `json:"different,omitempty"`
	OnlyA []envDiff `json:"onlyInA,omitempty"`
	OnlyB []envDiff `json:"onlyInB,omitempty"`
}

type nodeDiff struct {
	NodeType string `json:"nodeType"`
	CountA   int    `json:"a"`
	CountB   int    `json:"b"`
}

type nodeDiffResult struct {
	AppA  string `json:"appA"`
	AppB  string `json:"appB"`
	ExecA int    `json:"execIdA"`
	ExecB int    `json:"execIdB"`
	Nodes struct {
		A int `json:"a"`
		B int `json:"b"`
	} `json:"nodeCount"`
	Edges struct {
		A int `json:"a"`
		B int `json:"b"`
	} `json:"edgeCount"`
	Diffs []nodeDiff `json:"nodeTypeDiffs,omitempty"`
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

func runCompareEnv(cmd *cobra.Command, serverA, appA, serverB, appB string) error {
	cA, cB, err := getClients(serverA, serverB)
	if err != nil {
		return err
	}
	envRespA, err := cA.GetEnvironmentWithResponse(cmd.Context(), appA)
	if err != nil {
		return err
	}
	envA, err := util.CheckResponse(envRespA.JSON200, envRespA.HTTPResponse.Status)
	if err != nil {
		return err
	}
	envRespB, err := cB.GetEnvironmentWithResponse(cmd.Context(), appB)
	if err != nil {
		return err
	}
	envB, err := util.CheckResponse(envRespB.JSON200, envRespB.HTTPResponse.Status)
	if err != nil {
		return err
	}

	propsA := propsToMap(envA.SparkProperties)
	propsB := propsToMap(envB.SparkProperties)

	var diffs []envDiff
	var onlyA, onlyB []envDiff

	// collect all keys
	allKeys := map[string]bool{}
	for k := range propsA {
		allKeys[k] = true
	}
	for k := range propsB {
		allKeys[k] = true
	}

	for k := range allKeys {
		vA, inA := propsA[k]
		vB, inB := propsB[k]
		if inA && inB {
			if vA != vB {
				diffs = append(diffs, envDiff{k, vA, vB})
			}
		} else if inA {
			onlyA = append(onlyA, envDiff{k, vA, ""})
		} else {
			onlyB = append(onlyB, envDiff{Key: k, ValB: vB})
		}
	}

	sortEnvDiffs := func(s []envDiff) {
		slices.SortFunc(s, func(a, b envDiff) int { return cmp.Compare(a.Key, b.Key) })
	}
	sortEnvDiffs(diffs)
	sortEnvDiffs(onlyA)
	sortEnvDiffs(onlyB)

	result := envDiffResult{
		AppA:  appA,
		AppB:  appB,
		Diffs: diffs,
		OnlyA: onlyA,
		OnlyB: onlyB,
	}

	return util.PrintOutput(cmd.OutOrStdout(), result, outputFmt, func(w io.Writer) error {
		_, _ = fmt.Fprintf(w, "App A: %s\nApp B: %s\n", appA, appB)

		if envA.Runtime != nil && envB.Runtime != nil {
			_, _ = fmt.Fprintf(w, "\n=== Runtime ===\n")
			tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
			_, _ = fmt.Fprintf(tw, "\tA\tB\n")
			_, _ = fmt.Fprintf(tw, "Java:\t%s\t%s\n", util.Deref(envA.Runtime.JavaVersion), util.Deref(envB.Runtime.JavaVersion))
			_, _ = fmt.Fprintf(tw, "Scala:\t%s\t%s\n", util.Deref(envA.Runtime.ScalaVersion), util.Deref(envB.Runtime.ScalaVersion))
			_ = tw.Flush()
		}

		if len(diffs) > 0 {
			_, _ = fmt.Fprintf(w, "\n=== Different Spark Properties (%d) ===\n", len(diffs))
			tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "PROPERTY\tA\tB")
			for _, d := range diffs {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", d.Key, d.ValA, d.ValB)
			}
			_ = tw.Flush()
		}

		if len(onlyA) > 0 {
			_, _ = fmt.Fprintf(w, "\n=== Only in A (%d) ===\n", len(onlyA))
			tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "PROPERTY\tVALUE")
			for _, d := range onlyA {
				_, _ = fmt.Fprintf(tw, "%s\t%s\n", d.Key, d.ValA)
			}
			_ = tw.Flush()
		}

		if len(onlyB) > 0 {
			_, _ = fmt.Fprintf(w, "\n=== Only in B (%d) ===\n", len(onlyB))
			tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "PROPERTY\tVALUE")
			for _, d := range onlyB {
				_, _ = fmt.Fprintf(tw, "%s\t%s\n", d.Key, d.ValB)
			}
			_ = tw.Flush()
		}

		if len(diffs) == 0 && len(onlyA) == 0 && len(onlyB) == 0 {
			_, _ = fmt.Fprintln(w, "\nSpark properties are identical.")
		}

		return nil
	})
}

func propsToMap(props *[][]string) map[string]string {
	m := map[string]string{}
	if props == nil {
		return m
	}
	for _, pair := range *props {
		if len(pair) >= 2 {
			m[pair[0]] = pair[1]
		}
	}
	return m
}

func runComparePlans(cmd *cobra.Command, serverA, appA string, idA int, serverB, appB string, idB int) error {
	cA, cB, err := getClients(serverA, serverB)
	if err != nil {
		return err
	}

	sqlRespA, err := cA.GetSQLExecutionWithResponse(cmd.Context(), appA, idA, &client.GetSQLExecutionParams{})
	if err != nil {
		return err
	}
	sqlA, err := util.CheckResponse(sqlRespA.JSON200, sqlRespA.HTTPResponse.Status)
	if err != nil {
		return err
	}
	sqlRespB, err := cB.GetSQLExecutionWithResponse(cmd.Context(), appB, idB, &client.GetSQLExecutionParams{})
	if err != nil {
		return err
	}
	sqlB, err := util.CheckResponse(sqlRespB.JSON200, sqlRespB.HTTPResponse.Status)
	if err != nil {
		return err
	}

	nodesA := countNodeTypes(sqlA.Nodes)
	nodesB := countNodeTypes(sqlB.Nodes)

	allTypes := map[string]bool{}
	for k := range nodesA {
		allTypes[k] = true
	}
	for k := range nodesB {
		allTypes[k] = true
	}

	var diffs []nodeDiff
	for t := range allTypes {
		a, b := nodesA[t], nodesB[t]
		if a != b {
			diffs = append(diffs, nodeDiff{t, a, b})
		}
	}

	edgesA, edgesB := 0, 0
	if sqlA.Edges != nil {
		edgesA = len(*sqlA.Edges)
	}
	if sqlB.Edges != nil {
		edgesB = len(*sqlB.Edges)
	}
	totalA, totalB := 0, 0
	if sqlA.Nodes != nil {
		totalA = len(*sqlA.Nodes)
	}
	if sqlB.Nodes != nil {
		totalB = len(*sqlB.Nodes)
	}

	slices.SortFunc(diffs, func(a, b nodeDiff) int { return cmp.Compare(a.NodeType, b.NodeType) })

	result := nodeDiffResult{
		AppA: appA, AppB: appB,
		ExecA: idA, ExecB: idB,
		Diffs: diffs,
	}
	result.Nodes.A, result.Nodes.B = totalA, totalB
	result.Edges.A, result.Edges.B = edgesA, edgesB

	return util.PrintOutput(cmd.OutOrStdout(), result, outputFmt, func(w io.Writer) error {
		_, _ = fmt.Fprintf(w, "App A: %s  (SQL %d)\n", appA, idA)
		_, _ = fmt.Fprintf(w, "App B: %s  (SQL %d)\n", appB, idB)
		_, _ = fmt.Fprintf(w, "Query: A=%s  B=%s\n", util.Deref(sqlA.Description), util.Deref(sqlB.Description))

		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "\n\tA\tB\tDelta\n")
		_, _ = fmt.Fprintf(tw, "Nodes:\t%d\t%d\t%+d\n", totalA, totalB, totalB-totalA)
		_, _ = fmt.Fprintf(tw, "Edges:\t%d\t%d\t%+d\n", edgesA, edgesB, edgesB-edgesA)
		_ = tw.Flush()

		if len(diffs) > 0 {
			_, _ = fmt.Fprintf(w, "\n=== Node Type Differences ===\n")
			tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "NODE_TYPE\tA\tB\tDELTA")
			for _, d := range diffs {
				_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%+d\n", d.NodeType, d.CountA, d.CountB, d.CountB-d.CountA)
			}
			_ = tw.Flush()
		} else {
			_, _ = fmt.Fprintln(w, "\nPlan node types are identical.")
		}

		return nil
	})
}

func countNodeTypes(nodes *[]client.SQLPlanNode) map[string]int {
	m := map[string]int{}
	if nodes == nil {
		return m
	}
	for _, n := range *nodes {
		m[util.Deref(n.NodeName)]++
	}
	return m
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

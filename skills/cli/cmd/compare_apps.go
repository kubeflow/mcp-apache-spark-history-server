package cmd

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
	"github.com/spf13/cobra"
)

type appSide struct {
	App          string `json:"app"`
	Executors    int    `json:"executors"`
	DeadExecs    int    `json:"deadExecutors"`
	TotalCores   int    `json:"totalCores"`
	MaxMemory    int64  `json:"maxMemory"`
	Jobs         int    `json:"jobs"`
	FailedJobs   int    `json:"failedJobs"`
	Stages       int    `json:"stages"`
	FailedStages int    `json:"failedStages"`
	Tasks        int    `json:"tasks"`
	FailedTasks  int    `json:"failedTasks"`
	InputBytes   int64  `json:"inputBytes"`
	ShuffleRead  int64  `json:"shuffleRead"`
	ShuffleWrite int64  `json:"shuffleWrite"`
	SpillDisk    int64  `json:"spillDisk"`
	GCTimeMs     int64  `json:"gcTimeMs"`
}

func newCompareAppsCmd() *cobra.Command {
	var appA, appB string
	var serverA, serverB string

	cmd := &cobra.Command{
		Use:   "apps",
		Short: "Compare app-level performance: executors, jobs, stages, and aggregate metrics",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCompareApps(cmd, serverA, appA, serverB, appB)
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

func runCompareApps(cmd *cobra.Command, serverA, appA, serverB, appB string) error {
	cA, cB, err := getClients(serverA, serverB)
	if err != nil {
		return err
	}

	sideA, err := collectAppSide(cmd.Context(), cA, appA)
	if err != nil {
		return err
	}
	sideB, err := collectAppSide(cmd.Context(), cB, appB)
	if err != nil {
		return err
	}

	r := struct {
		A appSide `json:"a"`
		B appSide `json:"b"`
	}{*sideA, *sideB}

	return util.PrintOutput(cmd.OutOrStdout(), r, outputFmt, func(w io.Writer) error {
		_, _ = fmt.Fprintf(w, "App A:  %s\nApp B:  %s\n", appA, appB)

		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "\n\tA\tB\tDelta\n")
		printIntRow(tw, "Executors", sideA.Executors, sideB.Executors)
		printIntRow(tw, "Dead Executors", sideA.DeadExecs, sideB.DeadExecs)
		printIntRow(tw, "Total Cores", sideA.TotalCores, sideB.TotalCores)
		printBytesRow(tw, "Max Memory", sideA.MaxMemory, sideB.MaxMemory)
		printIntRow(tw, "Jobs", sideA.Jobs, sideB.Jobs)
		printIntRow(tw, "Failed Jobs", sideA.FailedJobs, sideB.FailedJobs)
		printIntRow(tw, "Stages", sideA.Stages, sideB.Stages)
		printIntRow(tw, "Failed Stages", sideA.FailedStages, sideB.FailedStages)
		printIntRow(tw, "Tasks", sideA.Tasks, sideB.Tasks)
		printIntRow(tw, "Failed Tasks", sideA.FailedTasks, sideB.FailedTasks)
		printBytesRow(tw, "Input", sideA.InputBytes, sideB.InputBytes)
		printBytesRow(tw, "Shuffle Read", sideA.ShuffleRead, sideB.ShuffleRead)
		printBytesRow(tw, "Shuffle Write", sideA.ShuffleWrite, sideB.ShuffleWrite)
		printBytesRow(tw, "Spill (Disk)", sideA.SpillDisk, sideB.SpillDisk)
		_, _ = fmt.Fprintf(tw, "GC Time:\t%s\t%s\t%s\n",
			util.FormatMsVal(sideA.GCTimeMs), util.FormatMsVal(sideB.GCTimeMs),
			fmtDelta(durMs(sideB.GCTimeMs-sideA.GCTimeMs)))
		return tw.Flush()
	})
}

func collectAppSide(ctx context.Context, c client.ClientWithResponsesInterface, app string) (*appSide, error) {
	// executors (all, including dead)
	execResp, err := c.ListAllExecutorsWithResponse(ctx, app)
	if err != nil {
		return nil, err
	}
	execs, err := util.CheckResponse(execResp.JSON200, execResp.HTTPResponse.Status)
	if err != nil {
		return nil, err
	}

	s := &appSide{App: app}
	for _, e := range *execs {
		if util.Deref(e.Id) == "driver" {
			continue
		}
		s.Executors++
		if !util.Deref(e.IsActive) {
			s.DeadExecs++
		}
		s.TotalCores += util.Deref(e.TotalCores)
		s.MaxMemory += util.Deref(e.MaxMemory)
	}

	// jobs
	jobResp, err := c.ListJobsWithResponse(ctx, app, &client.ListJobsParams{})
	if err != nil {
		return nil, err
	}
	jobs, err := util.CheckResponse(jobResp.JSON200, jobResp.HTTPResponse.Status)
	if err != nil {
		return nil, err
	}
	s.Jobs = len(*jobs)
	for _, j := range *jobs {
		if util.Deref(j.Status) == "FAILED" {
			s.FailedJobs++
		}
		s.FailedTasks += util.Deref(j.NumFailedTasks)
		s.Tasks += util.Deref(j.NumTasks)
	}

	// stages
	stagesResp, err := c.ListStagesWithResponse(ctx, app, &client.ListStagesParams{})
	if err != nil {
		return nil, err
	}
	stages, err := util.CheckResponse(stagesResp.JSON200, stagesResp.HTTPResponse.Status)
	if err != nil {
		return nil, err
	}
	s.Stages = len(*stages)
	for _, st := range *stages {
		if util.Deref(st.Status) == "FAILED" {
			s.FailedStages++
		}
		s.InputBytes += util.Deref(st.InputBytes)
		s.ShuffleRead += util.Deref(st.ShuffleReadBytes)
		s.ShuffleWrite += util.Deref(st.ShuffleWriteBytes)
		s.SpillDisk += util.Deref(st.DiskBytesSpilled)
		s.GCTimeMs += util.Deref(st.JvmGcTime)
	}

	return s, nil
}

func printIntRow(tw io.Writer, label string, a, b int) {
	_, _ = fmt.Fprintf(tw, "%s:\t%d\t%d\t%+d\n", label, a, b, b-a)
}

func printBytesRow(tw io.Writer, label string, a, b int64) {
	_, _ = fmt.Fprintf(tw, "%s:\t%s\t%s\t%s\n", label, util.FormatBytes(a), util.FormatBytes(b), fmtDeltaBytes(b-a))
}

func durMs(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

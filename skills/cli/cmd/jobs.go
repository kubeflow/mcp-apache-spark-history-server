package cmd

import (
	"cmp"
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

func newJobsCmd() *cobra.Command {
	var status string
	var limit int
	var group string
	var sortBy string

	cmd := &cobra.Command{
		Use:     "jobs [jobId]",
		Short:   "List or get jobs for an application",
		PreRunE: requireAppID,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newClient()
			if err != nil {
				return err
			}
			if len(args) == 1 {
				jobId, err := strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("invalid job ID: %s", args[0])
				}
				return getJob(cmd, c, jobId)
			}
			params := &client.ListJobsParams{}
			if status != "" {
				s := client.ListJobsParamsStatus(status)
				params.Status = &s
			}
			return listJobs(cmd, c, params, limit, group, sortBy)
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (running|succeeded|failed|unknown)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max number of jobs to return (0 for all)")
	cmd.Flags().StringVar(&group, "group", "", "Filter by job group")
	cmd.Flags().StringVar(&sortBy, "sort", "", "Sort by field (failed-tasks|duration|id)")
	return cmd
}

var statusPriority = map[string]int{
	"FAILED":    0,
	"RUNNING":   1,
	"UNKNOWN":   2,
	"SUCCEEDED": 3,
}

func jobDuration(j client.Job) time.Duration {
	return util.SparkDuration(j.SubmissionTime, j.CompletionTime)
}

func sortJobs(jobs []client.Job, sortBy string) {
	if sortBy != "" {
		slices.SortFunc(jobs, func(a, b client.Job) int {
			switch sortBy {
			case "failed-tasks":
				return -cmp.Compare(util.Deref(a.NumFailedTasks), util.Deref(b.NumFailedTasks))
			case "duration":
				return -cmp.Compare(jobDuration(a), jobDuration(b))
			case "id":
				return -cmp.Compare(util.Deref(a.JobId), util.Deref(b.JobId))
			}
			return 0
		})
		return
	}
	// default: failed status first, then by duration desc
	slices.SortFunc(jobs, func(a, b client.Job) int {
		pa := statusPriority[string(util.Deref(a.Status))]
		pb := statusPriority[string(util.Deref(b.Status))]
		if c := cmp.Compare(pa, pb); c != 0 {
			return c
		}
		return -cmp.Compare(jobDuration(a), jobDuration(b))
	})
}

func listJobs(cmd *cobra.Command, c client.ClientWithResponsesInterface, params *client.ListJobsParams, limit int, group string, sortBy string) error {
	resp, err := c.ListJobsWithResponse(cmd.Context(), appID, params)
	if err != nil {
		return err
	}
	body, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}

	jobs := *body

	if group != "" {
		filtered := jobs[:0]
		for _, j := range jobs {
			if j.JobGroup != nil && *j.JobGroup == group {
				filtered = append(filtered, j)
			}
		}
		jobs = filtered
	}

	sortJobs(jobs, sortBy)

	jobs, total := util.ApplyLimit(jobs, limit)

	return util.PrintOutput(cmd.OutOrStdout(), jobs, outputFmt, func(w io.Writer) error {
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "ID\tSTATUS\tDESCRIPTION\tDURATION\tTASKS\tFAILED_TASKS\tSTAGES")
		for _, j := range jobs {
			desc := util.Deref(j.Description)
			if desc == "" {
				desc = util.Deref(j.Name)
			}
			stages := util.FormatIntSlice(j.StageIds)
			_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d\t%d\t%s\n",
				util.Deref(j.JobId),
				util.Deref(j.Status),
				desc,
				jobDuration(j).Truncate(time.Millisecond),
				util.Deref(j.NumTasks),
				util.Deref(j.NumFailedTasks),
				stages,
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		util.PrintLimitFooter(w, limit, total, "jobs")
		return nil
	})
}

func getJob(cmd *cobra.Command, c client.ClientWithResponsesInterface, jobId int) error {
	resp, err := c.GetJobWithResponse(cmd.Context(), appID, jobId)
	if err != nil {
		return err
	}
	j, err := util.CheckResponse(resp.JSON200, resp.HTTPResponse.Status)
	if err != nil {
		return err
	}

	return util.PrintOutput(cmd.OutOrStdout(), j, outputFmt, func(w io.Writer) error {
		desc := util.Deref(j.Description)
		if desc == "" {
			desc = util.Deref(j.Name)
		}
		stages := util.FormatIntSlice(j.StageIds)
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "Job ID:\t%d\n", util.Deref(j.JobId))
		_, _ = fmt.Fprintf(tw, "Status:\t%s\n", util.Deref(j.Status))
		_, _ = fmt.Fprintf(tw, "Description:\t%s\n", desc)
		_, _ = fmt.Fprintf(tw, "Duration:\t%s\n", jobDuration(*j).Truncate(time.Millisecond))
		_, _ = fmt.Fprintf(tw, "Tasks:\t%d (failed: %d, killed: %d, skipped: %d)\n",
			util.Deref(j.NumTasks), util.Deref(j.NumFailedTasks), util.Deref(j.NumKilledTasks), util.Deref(j.NumSkippedTasks))
		_, _ = fmt.Fprintf(tw, "Stages:\t%s (failed: %d, skipped: %d)\n",
			stages, util.Deref(j.NumFailedStages), util.Deref(j.NumSkippedStages))
		if j.JobGroup != nil {
			_, _ = fmt.Fprintf(tw, "Group:\t%s\n", *j.JobGroup)
		}
		return tw.Flush()
	})
}

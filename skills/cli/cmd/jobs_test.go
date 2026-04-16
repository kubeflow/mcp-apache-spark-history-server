package cmd

import (
	"testing"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
)

func TestJobDuration(t *testing.T) {
	tests := []struct {
		name string
		job  client.Job
		want time.Duration
	}{
		{
			name: "normal",
			job: client.Job{
				SubmissionTime: util.Ptr("2025-08-05T00:27:50.920GMT"),
				CompletionTime: util.Ptr("2025-08-05T00:28:04.673GMT"),
			},
			want: 13*time.Second + 753*time.Millisecond,
		},
		{
			name: "nil submission",
			job:  client.Job{CompletionTime: util.Ptr("2025-08-05T00:28:04.673GMT")},
			want: 0,
		},
		{
			name: "nil completion",
			job:  client.Job{SubmissionTime: util.Ptr("2025-08-05T00:27:50.920GMT")},
			want: 0,
		},
		{
			name: "bad format",
			job: client.Job{
				SubmissionTime: util.Ptr("not-a-date"),
				CompletionTime: util.Ptr("2025-08-05T00:28:04.673GMT"),
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := jobDuration(tt.job)
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSortJobs_Default(t *testing.T) {
	failed := client.JobStatus("FAILED")
	succeeded := client.JobStatus("SUCCEEDED")
	running := client.JobStatus("RUNNING")

	jobs := []client.Job{
		{JobId: util.Ptr(1), Status: &succeeded, SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:10.000GMT")},
		{JobId: util.Ptr(2), Status: &failed, SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:05.000GMT")},
		{JobId: util.Ptr(3), Status: &running},
		{JobId: util.Ptr(4), Status: &succeeded, SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:20.000GMT")},
	}

	sortJobs(jobs, "")

	// failed first, then running, then succeeded by duration desc
	wantOrder := []int{2, 3, 4, 1}
	for i, want := range wantOrder {
		if got := util.Deref(jobs[i].JobId); got != want {
			t.Errorf("position %d: got job %d, want %d", i, got, want)
		}
	}
}

func TestSortJobs_ByFailedTasks(t *testing.T) {
	s := client.JobStatus("SUCCEEDED")
	jobs := []client.Job{
		{JobId: util.Ptr(1), Status: &s, NumFailedTasks: util.Ptr(0)},
		{JobId: util.Ptr(2), Status: &s, NumFailedTasks: util.Ptr(5)},
		{JobId: util.Ptr(3), Status: &s, NumFailedTasks: util.Ptr(2)},
	}

	sortJobs(jobs, "failed-tasks")

	wantOrder := []int{2, 3, 1}
	for i, want := range wantOrder {
		if got := util.Deref(jobs[i].JobId); got != want {
			t.Errorf("position %d: got job %d, want %d", i, got, want)
		}
	}
}

func TestSortJobs_ByDuration(t *testing.T) {
	s := client.JobStatus("SUCCEEDED")
	jobs := []client.Job{
		{JobId: util.Ptr(1), Status: &s, SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:05.000GMT")},
		{JobId: util.Ptr(2), Status: &s, SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:30.000GMT")},
		{JobId: util.Ptr(3), Status: &s, SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:10.000GMT")},
	}

	sortJobs(jobs, "duration")

	wantOrder := []int{2, 3, 1}
	for i, want := range wantOrder {
		if got := util.Deref(jobs[i].JobId); got != want {
			t.Errorf("position %d: got job %d, want %d", i, got, want)
		}
	}
}

func TestSortJobs_ById(t *testing.T) {
	s := client.JobStatus("SUCCEEDED")
	jobs := []client.Job{
		{JobId: util.Ptr(10), Status: &s},
		{JobId: util.Ptr(30), Status: &s},
		{JobId: util.Ptr(20), Status: &s},
	}

	sortJobs(jobs, "id")

	wantOrder := []int{30, 20, 10}
	for i, want := range wantOrder {
		if got := util.Deref(jobs[i].JobId); got != want {
			t.Errorf("position %d: got job %d, want %d", i, got, want)
		}
	}
}

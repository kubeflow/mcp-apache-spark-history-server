package cmd

import (
	"testing"
	"time"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
)

func TestStageDuration(t *testing.T) {
	tests := []struct {
		name  string
		stage client.StageData
		want  time.Duration
	}{
		{
			name: "normal",
			stage: client.StageData{
				SubmissionTime: util.Ptr("2025-08-05T00:27:50.920GMT"),
				CompletionTime: util.Ptr("2025-08-05T00:28:04.673GMT"),
			},
			want: 13*time.Second + 753*time.Millisecond,
		},
		{
			name:  "nil submission",
			stage: client.StageData{CompletionTime: util.Ptr("2025-08-05T00:28:04.673GMT")},
			want:  0,
		},
		{
			name:  "nil completion",
			stage: client.StageData{SubmissionTime: util.Ptr("2025-08-05T00:27:50.920GMT")},
			want:  0,
		},
		{
			name: "bad format",
			stage: client.StageData{
				SubmissionTime: util.Ptr("not-a-date"),
				CompletionTime: util.Ptr("2025-08-05T00:28:04.673GMT"),
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stageDuration(tt.stage); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStageDesc(t *testing.T) {
	tests := []struct {
		name  string
		stage client.StageData
		want  string
	}{
		{
			name:  "description set",
			stage: client.StageData{Description: util.Ptr("my desc"), Name: util.Ptr("my name")},
			want:  "my desc",
		},
		{
			name:  "falls back to name",
			stage: client.StageData{Name: util.Ptr("my name")},
			want:  "my name",
		},
		{
			name:  "empty description falls back to name",
			stage: client.StageData{Description: util.Ptr(""), Name: util.Ptr("my name")},
			want:  "my name",
		},
		{
			name:  "both nil",
			stage: client.StageData{},
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stageDesc(tt.stage); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func stageStatus(s string) *client.StageDataStatus {
	v := client.StageDataStatus(s)
	return &v
}

func TestSortStages_Default(t *testing.T) {
	stages := []client.StageData{
		{StageId: util.Ptr(1), Status: stageStatus("COMPLETE"), SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:10.000GMT")},
		{StageId: util.Ptr(2), Status: stageStatus("FAILED"), SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:05.000GMT")},
		{StageId: util.Ptr(3), Status: stageStatus("ACTIVE")},
		{StageId: util.Ptr(4), Status: stageStatus("PENDING")},
		{StageId: util.Ptr(5), Status: stageStatus("SKIPPED")},
		{StageId: util.Ptr(6), Status: stageStatus("COMPLETE"), SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:20.000GMT")},
	}

	sortStages(stages, "")

	// failed first, then complete (duration desc), then active, pending, skipped
	wantOrder := []int{2, 6, 1, 3, 4, 5}
	for i, want := range wantOrder {
		if got := util.Deref(stages[i].StageId); got != want {
			t.Errorf("position %d: got stage %d, want %d", i, got, want)
		}
	}
}

func TestSortStages_ByFailedTasks(t *testing.T) {
	s := stageStatus("COMPLETE")
	stages := []client.StageData{
		{StageId: util.Ptr(1), Status: s, NumFailedTasks: util.Ptr(0)},
		{StageId: util.Ptr(2), Status: s, NumFailedTasks: util.Ptr(5)},
		{StageId: util.Ptr(3), Status: s, NumFailedTasks: util.Ptr(2)},
	}

	sortStages(stages, "failed-tasks")

	wantOrder := []int{2, 3, 1}
	for i, want := range wantOrder {
		if got := util.Deref(stages[i].StageId); got != want {
			t.Errorf("position %d: got stage %d, want %d", i, got, want)
		}
	}
}

func TestSortStages_ByDuration(t *testing.T) {
	s := stageStatus("COMPLETE")
	stages := []client.StageData{
		{StageId: util.Ptr(1), Status: s, SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:05.000GMT")},
		{StageId: util.Ptr(2), Status: s, SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:30.000GMT")},
		{StageId: util.Ptr(3), Status: s, SubmissionTime: util.Ptr("2025-08-05T00:00:00.000GMT"), CompletionTime: util.Ptr("2025-08-05T00:00:10.000GMT")},
	}

	sortStages(stages, "duration")

	wantOrder := []int{2, 3, 1}
	for i, want := range wantOrder {
		if got := util.Deref(stages[i].StageId); got != want {
			t.Errorf("position %d: got stage %d, want %d", i, got, want)
		}
	}
}

func TestSortStages_ById(t *testing.T) {
	s := stageStatus("COMPLETE")
	stages := []client.StageData{
		{StageId: util.Ptr(10), Status: s},
		{StageId: util.Ptr(30), Status: s},
		{StageId: util.Ptr(20), Status: s},
	}

	sortStages(stages, "id")

	wantOrder := []int{30, 20, 10}
	for i, want := range wantOrder {
		if got := util.Deref(stages[i].StageId); got != want {
			t.Errorf("position %d: got stage %d, want %d", i, got, want)
		}
	}
}

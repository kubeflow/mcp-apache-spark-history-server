package cmd

import (
	"testing"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
)

func TestSortExecutors_Default(t *testing.T) {
	execs := []client.Executor{
		{Id: util.Ptr("1"), IsActive: util.Ptr(false), TotalDuration: util.Ptr(int64(5000))},
		{Id: util.Ptr("2"), IsActive: util.Ptr(true), TotalDuration: util.Ptr(int64(1000))},
		{Id: util.Ptr("3"), IsActive: util.Ptr(true), TotalDuration: util.Ptr(int64(3000))},
		{Id: util.Ptr("4"), IsActive: util.Ptr(false), TotalDuration: util.Ptr(int64(2000))},
	}

	sortExecutors(execs, "")

	// active first (duration desc), then dead (duration desc)
	wantOrder := []string{"3", "2", "1", "4"}
	for i, want := range wantOrder {
		if got := util.Deref(execs[i].Id); got != want {
			t.Errorf("position %d: got executor %q, want %q", i, got, want)
		}
	}
}

func TestSortExecutors_Summary(t *testing.T) {
	execs := []client.Executor{
		{Id: util.Ptr("1"), IsActive: util.Ptr(true), TotalDuration: util.Ptr(int64(5000))},
		{Id: util.Ptr("2"), IsActive: util.Ptr(false), TotalDuration: util.Ptr(int64(1000))},
		{Id: util.Ptr("3"), IsActive: util.Ptr(false), TotalDuration: util.Ptr(int64(3000))},
		{Id: util.Ptr("4"), IsActive: util.Ptr(true), TotalDuration: util.Ptr(int64(2000))},
	}

	sortExecutors(execs, "summary")

	// dead first (duration desc), then active (duration desc)
	wantOrder := []string{"3", "2", "1", "4"}
	for i, want := range wantOrder {
		if got := util.Deref(execs[i].Id); got != want {
			t.Errorf("position %d: got executor %q, want %q", i, got, want)
		}
	}
}

func TestSortExecutors_ByFields(t *testing.T) {
	makeExecs := func() []client.Executor {
		return []client.Executor{
			{Id: util.Ptr("2"), FailedTasks: util.Ptr(1), TotalDuration: util.Ptr(int64(100)), TotalGCTime: util.Ptr(int64(30))},
			{Id: util.Ptr("3"), FailedTasks: util.Ptr(5), TotalDuration: util.Ptr(int64(500)), TotalGCTime: util.Ptr(int64(10))},
			{Id: util.Ptr("1"), FailedTasks: util.Ptr(3), TotalDuration: util.Ptr(int64(250)), TotalGCTime: util.Ptr(int64(50))},
		}
	}

	tests := []struct {
		name      string
		sortBy    string
		wantOrder []string
	}{
		{name: "failed-tasks", sortBy: "failed-tasks", wantOrder: []string{"3", "1", "2"}},
		{name: "duration", sortBy: "duration", wantOrder: []string{"3", "1", "2"}},
		{name: "gc", sortBy: "gc", wantOrder: []string{"1", "2", "3"}},
		{name: "id", sortBy: "id", wantOrder: []string{"1", "2", "3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execs := makeExecs()
			sortExecutors(execs, tt.sortBy)
			for i, want := range tt.wantOrder {
				if got := util.Deref(execs[i].Id); got != want {
					t.Errorf("position %d: got executor %q, want %q", i, got, want)
				}
			}
		})
	}
}

func TestFormatSparkTimeShort(t *testing.T) {
	tests := []struct {
		name string
		in   *string
		want string
	}{
		{
			name: "nil input",
			in:   nil,
			want: "",
		},
		{
			name: "valid spark timestamp",
			in:   util.Ptr("2025-08-05T14:27:50.920GMT"),
			want: "14:27:50",
		},
		{
			name: "invalid format returns original",
			in:   util.Ptr("not-a-timestamp"),
			want: "not-a-timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatSparkTimeShort(tt.in); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPeakMetric(t *testing.T) {
	getHeap := func(p *client.PeakMemoryMetrics) *int64 { return p.JVMHeapMemory }

	tests := []struct {
		name string
		exec client.Executor
		want int64
	}{
		{
			name: "nil peak memory metrics",
			exec: client.Executor{},
			want: 0,
		},
		{
			name: "nil metric field",
			exec: client.Executor{PeakMemoryMetrics: &client.PeakMemoryMetrics{}},
			want: 0,
		},
		{
			name: "non-nil metric field",
			exec: client.Executor{
				PeakMemoryMetrics: &client.PeakMemoryMetrics{
					JVMHeapMemory: util.Ptr(int64(1024 * 1024)),
				},
			},
			want: 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := peakMetric(tt.exec, getHeap); got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

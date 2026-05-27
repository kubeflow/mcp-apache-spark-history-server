package cmd

import (
	"testing"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
)

func strPtr(s string) *string { return &s }
func int64Ptr(i int64) *int64 { return &i }

func sampleThreads() []client.ThreadStackTrace {
	return []client.ThreadStackTrace{
		{
			ThreadId:    int64Ptr(1),
			ThreadName:  strPtr("main"),
			ThreadState: strPtr("RUNNABLE"),
		},
		{
			ThreadId:          int64Ptr(2),
			ThreadName:        strPtr("dispatcher-event-loop-0"),
			ThreadState:       strPtr("WAITING"),
			BlockedByThreadId: int64Ptr(1),
		},
		{
			ThreadId:    int64Ptr(3),
			ThreadName:  strPtr("Executor task launch worker for task 42"),
			ThreadState: strPtr("BLOCKED"),
			LockName:    strPtr("java.util.concurrent.locks.ReentrantLock"),
		},
		{
			ThreadId:    int64Ptr(4),
			ThreadName:  strPtr("DispatcherWatcher"),
			ThreadState: strPtr("TIMED_WAITING"),
		},
	}
}

func TestFilterThreads(t *testing.T) {
	tests := []struct {
		name        string
		state       string
		nameFilter  string
		blockedOnly bool
		wantIDs     []int64
	}{
		{
			name:    "no filter returns all",
			wantIDs: []int64{1, 2, 3, 4},
		},
		{
			name:    "state RUNNABLE",
			state:   "RUNNABLE",
			wantIDs: []int64{1},
		},
		{
			name:    "state lowercase still matches",
			state:   "blocked",
			wantIDs: []int64{3},
		},
		{
			name:       "name substring case-insensitive",
			nameFilter: "DISPATCHER",
			wantIDs:    []int64{2, 4},
		},
		{
			name:        "blocked-only catches BlockedByThreadId",
			blockedOnly: true,
			wantIDs:     []int64{2, 3},
		},
		{
			name:       "combine state + name",
			state:      "WAITING",
			nameFilter: "dispatcher",
			wantIDs:    []int64{2},
		},
		{
			name:       "no match returns empty",
			nameFilter: "nonexistent",
			wantIDs:    []int64{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterThreads(sampleThreads(), tt.state, tt.nameFilter, tt.blockedOnly)
			gotIDs := make([]int64, len(got))
			for i, th := range got {
				gotIDs[i] = *th.ThreadId
			}
			if len(gotIDs) != len(tt.wantIDs) {
				t.Fatalf("got IDs %v, want %v", gotIDs, tt.wantIDs)
			}
			for i, id := range tt.wantIDs {
				if gotIDs[i] != id {
					t.Errorf("index %d: got %d, want %d", i, gotIDs[i], id)
				}
			}
		})
	}
}

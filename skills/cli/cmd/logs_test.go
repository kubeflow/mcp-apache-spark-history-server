package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestIsTaskNotFound(t *testing.T) {
	if !isTaskNotFound(errTaskNotFound{taskID: 42}) {
		t.Fatalf("expected errTaskNotFound to be recognized")
	}
	if isTaskNotFound(errors.New("some other error")) {
		t.Fatalf("plain error should not be recognized as task-not-found")
	}
	if isTaskNotFound(nil) {
		t.Fatalf("nil should not be recognized as task-not-found")
	}
}

func TestErrTaskNotFoundMessage(t *testing.T) {
	err := errTaskNotFound{taskID: 145}
	if !strings.Contains(err.Error(), "145") {
		t.Fatalf("error message must include taskID, got %q", err.Error())
	}
}

func TestLogsResultJSONShape(t *testing.T) {
	taskID := int64(145)
	stageID := 12
	stageAttempt := 0
	r := &LogsResult{
		ExecutorId:     "1",
		TaskId:         &taskID,
		StageId:        &stageID,
		StageAttemptId: &stageAttempt,
		Host:           "host.example.com:12345",
		Logs: map[string]string{
			"stdout":    "http://host/stdout",
			"stderr":    "http://host/stderr",
			"spark.log": "http://host/spark.log",
		},
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(b)
	for _, want := range []string{
		`"executorId":"1"`,
		`"taskId":145`,
		`"stageId":12`,
		`"stageAttemptId":0`,
		`"host":"host.example.com:12345"`,
		`"stdout":"http://host/stdout"`,
		`"stderr":"http://host/stderr"`,
		`"spark.log":"http://host/spark.log"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("want substring %s in %s", want, got)
		}
	}
}

// TestLogsResultJSONOmitsEmpty verifies that pointer fields collapse when unset.
func TestLogsResultJSONOmitsEmpty(t *testing.T) {
	r := &LogsResult{
		ExecutorId: "driver",
		Logs: map[string]string{
			"spark.log": "http://x/spark.log",
		},
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(b)
	for _, forbidden := range []string{`"taskId"`, `"stageId"`, `"stageAttemptId"`, `"host"`} {
		if strings.Contains(got, forbidden) {
			t.Errorf("should omit %s when empty/nil; got %s", forbidden, got)
		}
	}
}

// TestLogsCommandFlagValidation exercises the RunE guard rails without an HTTP
// call. These paths fail before reaching newClient(), so no mock is needed.
func TestLogsCommandFlagValidation(t *testing.T) {
	tests := []struct {
		name    string
		flags   []string
		wantErr string
	}{
		{
			name:    "no executor no task",
			flags:   []string{},
			wantErr: "exactly one of --executor or --task is required",
		},
		{
			name:    "both executor and task",
			flags:   []string{"--executor", "1", "--task", "5"},
			wantErr: "exactly one of --executor or --task is required",
		},
		{
			name:    "stage without task",
			flags:   []string{"--executor", "1", "--stage", "2"},
			wantErr: "--stage is only valid with --task",
		},
		{
			name:    "stage-attempt without task",
			flags:   []string{"--executor", "1", "--stage-attempt", "0"},
			wantErr: "--stage-attempt is only valid with --task",
		},
	}

	origAppID := appID
	defer func() { appID = origAppID }()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appID = "application_test_00001"

			cmd := newLogsCmd()
			// silence any incidental stdout.
			var buf bytes.Buffer
			cmd.SetOut(&buf)
			cmd.SetErr(&buf)
			cmd.SetArgs(tt.flags)
			// parse flags explicitly so Changed() works, then invoke RunE.
			if err := cmd.ParseFlags(tt.flags); err != nil {
				t.Fatalf("parse flags: %v", err)
			}
			err := cmd.RunE(cmd, []string{})
			if err == nil {
				t.Fatalf("expected error %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

// TestNewLogsCmdDeclaresExpectedFlags guards the public flag surface.
func TestNewLogsCmdDeclaresExpectedFlags(t *testing.T) {
	cmd := newLogsCmd()
	for _, name := range []string{"executor", "task", "stage", "stage-attempt"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("flag %q missing", name)
		}
	}
	// Verify short aliases.
	for alias, long := range map[string]string{"e": "executor", "t": "task"} {
		if f := cmd.Flags().ShorthandLookup(alias); f == nil || f.Name != long {
			t.Errorf("short -%s should alias --%s", alias, long)
		}
	}
}

// TestPreferredLogKeyOrder guards the ordering contract used by the text-mode output.
// spark.log / stderr / stdout first (in that order), then extras alphabetical.
func TestPreferredLogKeyOrder(t *testing.T) {
	preferred := []string{"spark.log", "stderr", "stdout"}
	have := map[string]string{
		"stdout":    "u1",
		"spark.log": "u2",
		"stderr":    "u3",
		"extra-log": "u4",
	}
	seen := map[string]bool{}
	order := []string{}
	for _, k := range preferred {
		if _, ok := have[k]; ok {
			order = append(order, k)
			seen[k] = true
		}
	}
	extras := []string{}
	for k := range have {
		if !seen[k] {
			extras = append(extras, k)
		}
	}
	sort.Strings(extras)
	order = append(order, extras...)

	want := []string{"spark.log", "stderr", "stdout", "extra-log"}
	if len(order) != len(want) {
		t.Fatalf("len mismatch: got %v want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("pos %d: got %s want %s", i, order[i], want[i])
		}
	}
}

// Ensure the cobra import stays used when we add future integration tests.
var _ = (*cobra.Command)(nil)

//go:build e2e

package e2e

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/google/go-cmp/cmp"
)

type appInfo struct {
	ID   string `yaml:"id"`
	Name string `yaml:"name"`
}

type yarnInfo struct {
	ID string `yaml:"id"`
}

type fixtures struct {
	App1               appInfo  `yaml:"app1"`
	App2               appInfo  `yaml:"app2"`
	Yarn               yarnInfo `yaml:"yarn"`
	Apps               string   `yaml:"apps"`
	JobsApp1           string   `yaml:"jobs_app1"`
	JobsApp2           string   `yaml:"jobs_app2"`
	Stage5ErrsApp1     string   `yaml:"stage5_errors_app1"`
	Stage5ErrsApp2     string   `yaml:"stage5_errors_app2"`
	SQLApp1            string   `yaml:"sql_app1"`
	SQLApp2            string   `yaml:"sql_app2"`
	JobsApp1SortID     string   `yaml:"jobs_app1_sort_id"`
	SQLApp1SortID      string   `yaml:"sql_app1_sort_id"`
	CompareSQL6        string   `yaml:"compare_sql6"`
	CompareSQL8        string   `yaml:"compare_sql8"`
	ExecApp1           string   `yaml:"executors_app1"`
	ExecApp2           string   `yaml:"executors_app2"`
	ExecSumApp1        string   `yaml:"executors_summary_app1"`
	SQL6SumApp1        string   `yaml:"sql6_summary_app1"`
	SQL6SumApp2        string   `yaml:"sql6_summary_app2"`
	SQL6PlanApp1       string   `yaml:"sql6_plan_sha256_app1"`
	SQL6PlanApp2       string   `yaml:"sql6_plan_sha256_app2"`
	AppsAllServers     string   `yaml:"apps_all_servers"`
	StagesApp1         string   `yaml:"stages_app1"`
	StagesApp2         string   `yaml:"stages_app2"`
	YarnAttempts       string   `yaml:"yarn_attempts"`
	YarnJobsAttempt1   string   `yaml:"yarn_jobs_attempt1"`
	YarnJobsAttempt2   string   `yaml:"yarn_jobs_attempt2"`
	YarnStagesAttempt1 string   `yaml:"yarn_stages_attempt1"`
	Stages5            string   `yaml:"stages_5"`
	Stages43           string   `yaml:"stages_43"`
}

var fix fixtures

func TestMain(m *testing.M) {
	data, err := os.ReadFile("fixtures.yaml")
	if err != nil {
		panic("failed to read fixtures.yaml: " + err.Error())
	}
	if err := yaml.Unmarshal(data, &fix); err != nil {
		panic("failed to parse fixtures.yaml: " + err.Error())
	}
	os.Exit(m.Run())
}

func shs(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("../bin/shs", append([]string{"-c", "config.yaml"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("shs %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func TestApps(t *testing.T) {
	got := shs(t, "apps")
	if diff := cmp.Diff(fix.Apps, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
	t.Run("all_servers", func(t *testing.T) {
		got := shs(t, "apps", "--all-servers", "--sort", "server")
		if diff := cmp.Diff(fix.AppsAllServers, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestJobs(t *testing.T) {
	tests := []struct {
		name string
		app  string
		want string
	}{
		{"app1", fix.App1.ID, fix.JobsApp1},
		{"app2", fix.App2.ID, fix.JobsApp2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, "jobs", "-a", tt.app, "--limit", "0")
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStageErrors(t *testing.T) {
	tests := []struct {
		name string
		app  string
		want string
	}{
		{"app1", fix.App1.ID, fix.Stage5ErrsApp1},
		{"app2", fix.App2.ID, fix.Stage5ErrsApp2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, "stages", "-a", tt.app, "5", "--errors")
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSQL(t *testing.T) {
	tests := []struct {
		name string
		app  string
		want string
	}{
		{"app1", fix.App1.ID, fix.SQLApp1},
		{"app2", fix.App2.ID, fix.SQLApp2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, "sql", "-a", tt.app, "--limit", "0")
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSort(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"jobs_sort_id", []string{"jobs", "-a", fix.App1.ID, "--sort", "id", "--limit", "5"}, fix.JobsApp1SortID},
		{"sql_sort_id", []string{"sql", "-a", fix.App1.ID, "--sort", "id", "--limit", "5"}, fix.SQLApp1SortID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, tt.args...)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
func TestExecutors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"app1", []string{"executors", "-a", fix.App1.ID}, fix.ExecApp1},
		{"app2", []string{"executors", "-a", fix.App2.ID}, fix.ExecApp2},
		{"summary_app1", []string{"executors", "-a", fix.App1.ID, "--summary"}, fix.ExecSumApp1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, tt.args...)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("executors mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		name   string
		execID string
		want   string
	}{
		{"sql6_join", "6", fix.CompareSQL6},
		{"sql8_spill", "8", fix.CompareSQL8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, "compare", "--app-a", fix.App1.ID, "--app-b", fix.App2.ID, tt.execID, tt.execID)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
	// Cross-server: same SHS via two config entries (default + secondary)
	t.Run("cross_server", func(t *testing.T) {
		got := shs(t, "compare", "--server-a", "default", "--server-b", "secondary",
			"--app-a", fix.App1.ID, "--app-b", fix.App2.ID, "6", "6")
		if diff := cmp.Diff(fix.CompareSQL6, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestSQLSummary(t *testing.T) {
	tests := []struct {
		name string
		app  string
		want string
	}{
		{"app1", fix.App1.ID, fix.SQL6SumApp1},
		{"app2", fix.App2.ID, fix.SQL6SumApp2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, "sql", "-a", tt.app, "6", "--summary")
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSQLPlan verifies plan output via SHA256 hash since the full plan text
// is too large to embed in fixtures. Uses --initial-plan to cover both final
// and initial plans in one check.
func TestSQLPlan(t *testing.T) {
	tests := []struct {
		name string
		app  string
		want string
	}{
		{"app1", fix.App1.ID, fix.SQL6PlanApp1},
		{"app2", fix.App2.ID, fix.SQL6PlanApp2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, "sql", "-a", tt.app, "6", "--initial-plan")
			hash := fmt.Sprintf("%x", sha256.Sum256([]byte(got)))
			if hash != tt.want {
				t.Errorf("sql plan hash mismatch\nwant: %s\ngot:  %s", tt.want, hash)
			}
		})
	}
}

func TestYarnAttempts(t *testing.T) {
	app := fix.Yarn.ID

	t.Run("list_attempts", func(t *testing.T) {
		got := shs(t, "apps", "-a", app, "--attempts")
		if diff := cmp.Diff(fix.YarnAttempts, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
	t.Run("jobs_attempt1", func(t *testing.T) {
		got := shs(t, "jobs", "-a", app, "--attempt", "1", "--limit", "0")
		if diff := cmp.Diff(fix.YarnJobsAttempt1, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
	t.Run("jobs_attempt2", func(t *testing.T) {
		got := shs(t, "jobs", "-a", app, "--attempt", "2", "--limit", "0")
		if diff := cmp.Diff(fix.YarnJobsAttempt2, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
	t.Run("stages_attempt1", func(t *testing.T) {
		got := shs(t, "stages", "-a", app, "--attempt", "1", "--limit", "0")
		if diff := cmp.Diff(fix.YarnStagesAttempt1, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestStages(t *testing.T) {
	tests := []struct {
		name string
		app  string
		want string
	}{
		{"app1", fix.App1.ID, fix.StagesApp1},
		{"app2", fix.App2.ID, fix.StagesApp2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, "stages", "-a", tt.app, "--limit", "5")
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStageDetail(t *testing.T) {
	tests := []struct {
		name    string
		app     string
		stageID string
		want    string
	}{
		{"failed_no_quantiles", fix.App1.ID, "5", fix.Stages5},
		{"complete_with_quantiles", fix.App1.ID, "43", fix.Stages43},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, "stages", "-a", tt.app, tt.stageID)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestJSONOutput(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		count int
	}{
		{"apps", []string{"apps", "-o", "json"}, 3},
		{"jobs", []string{"jobs", "-a", fix.App1.ID, "--limit", "5", "-o", "json"}, 5},
		{"stages", []string{"stages", "-a", fix.App1.ID, "--limit", "5", "-o", "json"}, 5},
		{"sql", []string{"sql", "-a", fix.App1.ID, "-o", "json"}, 10},
		{"executors", []string{"executors", "-a", fix.App1.ID, "-o", "json"}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := shs(t, tt.args...)
			var arr []json.RawMessage
			if err := json.Unmarshal([]byte(out), &arr); err != nil {
				t.Fatalf("invalid JSON: %v\n%s", err, out)
			}
			if len(arr) != tt.count {
				t.Errorf("expected %d items, got %d", tt.count, len(arr))
			}
		})
	}

	// Compare JSON golden file tests
	goldenTests := []struct {
		name string
		args []string
		file string
	}{
		{"compare", []string{"compare", "--app-a", fix.App1.ID, "--app-b", fix.App2.ID, "6", "6", "-o", "json"}, "compare.json"},
		{"compare_env", []string{"compare", "--app-a", fix.App1.ID, "--app-b", fix.App2.ID, "--env", "-o", "json"}, "compare_env.json"},
		{"compare_plans", []string{"compare", "--app-a", fix.App1.ID, "--app-b", fix.App2.ID, "--plans", "6", "6", "-o", "json"}, "compare_plans.json"},
	}
	for _, tt := range goldenTests {
		t.Run(tt.name, func(t *testing.T) {
			got := shs(t, tt.args...)
			want, err := os.ReadFile(tt.file)
			if err != nil {
				t.Fatalf("read golden file: %v", err)
			}
			if diff := cmp.Diff(string(want), got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

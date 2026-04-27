package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
)

func TestSampleConfig_RoundTrips(t *testing.T) {
	out, err := sampleConfig()
	if err != nil {
		t.Fatalf("sampleConfig: %v", err)
	}

	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, out, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(p)
	if err != nil {
		t.Fatalf("Load round-trip failed: %v", err)
	}

	if cfg.Servers["local"].URL != config.DefaultServerUrl {
		t.Errorf("local url = %q, want %q", cfg.Servers["local"].URL, config.DefaultServerUrl)
	}
	if !cfg.Servers["local"].Default {
		t.Error("local.default should be true")
	}
	if cfg.Servers["production"].Auth == nil || cfg.Servers["production"].Auth.Token != "your-token-here" {
		t.Error("production.auth.token not round-tripped")
	}
	if !cfg.Servers["production"].VerifySSL {
		t.Error("production.verify_ssl should be true")
	}
}

func TestSampleConfig_ContainsComments(t *testing.T) {
	out, err := sampleConfig()
	if err != nil {
		t.Fatalf("sampleConfig: %v", err)
	}

	yaml := string(out)
	for _, want := range []string{
		"# minimal config for a local Spark History Server",
		"# mark as default server",
		"# remote server with auth and SSL",
		"# optional: for EMR-based clusters",
		"# use token or username/password",
		"# bearer token",
	} {
		if !strings.Contains(yaml, want) {
			t.Errorf("missing comment: %q", want)
		}
	}
}

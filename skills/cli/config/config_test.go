package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoad_BasicYAML(t *testing.T) {
	p := writeTestConfig(t, `
servers:
  local:
    default: true
    url: "http://localhost:18080"
  prod:
    url: "https://spark.example.com"
    verify_ssl: true
    auth:
      username: admin
      password: secret
      token: abc
    emr_cluster_arn: "arn:aws:emr:us-east-1:123456789012:cluster/j-ABC"
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(cfg.Servers))
	}

	local := cfg.Servers["local"]
	if !local.Default {
		t.Error("expected local.default = true")
	}
	if local.URL != "http://localhost:18080" {
		t.Errorf("expected local url, got %s", local.URL)
	}

	prod := cfg.Servers["prod"]
	if prod.VerifySSL == nil || !*prod.VerifySSL {
		t.Error("expected prod.verify_ssl = true")
	}
	if prod.Auth == nil {
		t.Fatal("expected prod.auth to be set")
	}
	if prod.Auth.Username != "admin" || prod.Auth.Password != "secret" || prod.Auth.Token != "abc" {
		t.Errorf("unexpected auth: %+v", prod.Auth)
	}
	if prod.EMRClusterARN != "arn:aws:emr:us-east-1:123456789012:cluster/j-ABC" {
		t.Errorf("unexpected emr_cluster_arn: %s", prod.EMRClusterARN)
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	p := writeTestConfig(t, `
servers:
  local:
    url: "http://localhost:18080"
    auth:
      token: original
`)
	t.Setenv("SHS_CLI__SERVERS__LOCAL__URL", "http://overridden:9999")
	t.Setenv("SHS_CLI__SERVERS__LOCAL__AUTH__TOKEN", "env-token")

	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}

	local := cfg.Servers["local"]
	if local.URL != "http://overridden:9999" {
		t.Errorf("expected env override for url, got %s", local.URL)
	}
	if local.Auth == nil || local.Auth.Token != "env-token" {
		t.Errorf("expected env override for auth.token, got %+v", local.Auth)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	p := writeTestConfig(t, `{{{invalid`)
	_, err := Load(p)
	if err == nil {
		t.Fatal("expected error for invalid yaml")
	}
}

func TestLoad_EmptyServers(t *testing.T) {
	p := writeTestConfig(t, `servers:`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Servers) != 0 {
		t.Errorf("expected 0 servers, got %d", len(cfg.Servers))
	}
}

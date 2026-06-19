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
	if !prod.VerifySSL {
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
	if len(cfg.Servers) != 1 {
		t.Errorf("expected 1 servers, got %d", len(cfg.Servers))
	}
}

// --- config path resolution cascade ---

// isolateConfigEnv clears config-related env vars and points HOME/XDG at a
// clean temp area, then switches to an empty working directory.
func isolateConfigEnv(t *testing.T) {
	t.Helper()
	t.Setenv(EnvConfig, "")
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", t.TempDir())
	t.Chdir(t.TempDir())
}

func TestResolve_ExplicitEnvWins(t *testing.T) {
	isolateConfigEnv(t)
	if err := os.WriteFile(DefaultConfigName, []byte("servers:\n"), 0644); err != nil {
		t.Fatal(err)
	}
	explicit := filepath.Join(t.TempDir(), "explicit.yaml")
	if err := os.WriteFile(explicit, []byte("servers:\n"), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvConfig, explicit)

	path, exp := resolvePath("")
	if path != explicit || !exp {
		t.Fatalf("expected explicit env path, got (%q, %v)", path, exp)
	}
}

func TestResolve_FlagWinsOverEnv(t *testing.T) {
	isolateConfigEnv(t)
	t.Setenv(EnvConfig, "/from/env.yaml")
	path, exp := resolvePath("/from/flag.yaml")
	if path != "/from/flag.yaml" || !exp {
		t.Fatalf("expected flag path, got (%q, %v)", path, exp)
	}
}

func TestResolve_CwdBeforeUserConfig(t *testing.T) {
	isolateConfigEnv(t)
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	userDir := filepath.Join(xdg, AppConfigDir)
	if err := os.MkdirAll(userDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(userDir, DefaultConfigName), []byte("servers:\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(DefaultConfigName, []byte("servers:\n"), 0644); err != nil {
		t.Fatal(err)
	}
	path, exp := resolvePath("")
	if path != DefaultConfigName || exp {
		t.Fatalf("expected cwd config (non-explicit), got (%q, %v)", path, exp)
	}
}

func TestResolve_UserConfigWhenNoCwd(t *testing.T) {
	isolateConfigEnv(t)
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	userDir := filepath.Join(xdg, AppConfigDir)
	if err := os.MkdirAll(userDir, 0755); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(userDir, DefaultConfigName)
	if err := os.WriteFile(want, []byte("servers:\n"), 0644); err != nil {
		t.Fatal(err)
	}
	path, exp := resolvePath("")
	if path != want || exp {
		t.Fatalf("expected user config (non-explicit), got (%q, %v)", path, exp)
	}
}

func TestResolve_NothingFound(t *testing.T) {
	isolateConfigEnv(t)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	path, exp := resolvePath("")
	if path != "" || exp {
		t.Fatalf("expected no path, got (%q, %v)", path, exp)
	}
}

func TestLoad_ExplicitMissingFailsFast(t *testing.T) {
	isolateConfigEnv(t)
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("expected error for explicit missing config file")
	}
}

func TestLoad_DefaultsWhenNoFile(t *testing.T) {
	isolateConfigEnv(t)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Servers) != 1 {
		t.Fatalf("expected 1 default server, got %d", len(cfg.Servers))
	}
	if cfg.Servers["local"].URL != DefaultServerUrl {
		t.Errorf("expected default local url, got %s", cfg.Servers["local"].URL)
	}
}

func TestLoad_DiscoversCwdConfig(t *testing.T) {
	isolateConfigEnv(t)
	if err := os.WriteFile(DefaultConfigName,
		[]byte("servers:\n  fromcwd:\n    url: http://cwd:18080\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Servers["fromcwd"]; !ok {
		t.Fatalf("expected server discovered from cwd config, got %+v", cfg.Servers)
	}
}

// TestLoad_IgnoresUnknownMCPFields documents forward-compatibility: a config
// shared with the MCP server may carry fields the CLI does not model; they
// must be ignored rather than cause an error.
func TestLoad_IgnoresUnknownMCPFields(t *testing.T) {
	isolateConfigEnv(t)
	p := writeTestConfig(t, `
servers:
  prod:
    default: true
    url: "https://spark.example.com"
    verify_ssl: true
    use_proxy: true
    timeout: 45
    include_plan_description: true
    auth:
      token: tok
mcp:
  transports:
    - streamable-http
  port: "18888"
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("expected unknown MCP fields to be ignored, got error: %v", err)
	}
	prod := cfg.Servers["prod"]
	if prod.URL != "https://spark.example.com" || !prod.VerifySSL || !prod.Default {
		t.Errorf("unexpected prod server: %+v", prod)
	}
	if prod.Auth == nil || prod.Auth.Token != "tok" {
		t.Errorf("expected auth.token preserved, got %+v", prod.Auth)
	}
}

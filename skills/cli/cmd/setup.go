package cmd

import (
	"os"
	"os/exec"
	"path/filepath"
	"text/template"

	goyaml "github.com/goccy/go-yaml"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/config"
	"github.com/spf13/cobra"
)

func shsBin() string {
	if _, err := exec.LookPath("shs"); err == nil {
		return "shs"
	}
	if exe, err := os.Executable(); err == nil {
		if resolved, err := filepath.EvalSymlinks(exe); err == nil {
			return resolved
		}
	}
	return "shs"
}

var skillTmpl = template.Must(template.New("skill").Parse(`---
name: spark-history
description: >
  Debug and analyze Apache Spark jobs using the shs CLI.
  Use when investigating Spark application failures, slow queries,
  data skew, executor issues, shuffle/spill problems, or comparing
  job runs across environments.
---

` + "!`\"{{.Bin}}\" prime`" + `
`))

func newSetupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Print agent skill files to stdout",
	}
	cmd.AddCommand(newSetupSkillCmd())
	cmd.AddCommand(newSetupConfigCmd())
	return cmd
}

func newSetupConfigCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "config",
		Short: "Print a sample config.yaml to get started",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			out, err := sampleConfig()
			if err != nil {
				return err
			}
			_, err = cmd.OutOrStdout().Write(out)
			return err
		},
	}
}

// sampleConfig marshals a representative Config struct to YAML so the output
// automatically reflects any fields added to the structs — no template to
// keep in sync. Comments are attached via goccy/go-yaml's CommentMap; a
// missing comment on a new field is harmless, whereas a missing field in a
// hardcoded template is a silent bug.
func sampleConfig() ([]byte, error) {
	cfg := config.Config{
		Servers: map[string]config.Server{
			"local": {
				Default: true,
				URL:     config.DefaultServerUrl,
			},
			"production": {
				URL:       "https://spark-history.example.com",
				VerifySSL: true,
				Auth: &config.Auth{
					Token: "your-token-here",
				},
				EMRClusterARN: "arn:aws:emr:us-east-1:123456789012:cluster/j-EXAMPLE",
			},
		},
	}

	comments := goyaml.CommentMap{
		"$.servers.local":                      []*goyaml.Comment{goyaml.HeadComment(" minimal config for a local Spark History Server")},
		"$.servers.local.default":              []*goyaml.Comment{goyaml.LineComment(" mark as default server")},
		"$.servers.production":                 []*goyaml.Comment{goyaml.HeadComment(" remote server with auth and SSL")},
		"$.servers.production.emr_cluster_arn": []*goyaml.Comment{goyaml.LineComment(" optional: for EMR-based clusters")},
		"$.servers.production.auth":            []*goyaml.Comment{goyaml.HeadComment(" use token or username/password")},
		"$.servers.production.auth.token":      []*goyaml.Comment{goyaml.LineComment(" bearer token")},
	}

	return goyaml.MarshalWithOptions(cfg, goyaml.WithComment(comments))
}

func newSetupSkillCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "skill",
		Short: "Print skill file for coding agents (redirect to your agent's skill path)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return skillTmpl.Execute(cmd.OutOrStdout(), struct{ Bin string }{shsBin()})
		},
	}
}

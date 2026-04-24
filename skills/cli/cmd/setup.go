package cmd

import (
	"os"
	"os/exec"
	"path/filepath"
	"text/template"

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
	return cmd
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

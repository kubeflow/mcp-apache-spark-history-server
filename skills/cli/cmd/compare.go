package cmd

import (
	"github.com/spf13/cobra"
)

func newCompareCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare applications or SQL executions",
	}
	cmd.AddCommand(newCompareSQLCmd())
	cmd.AddCommand(newCompareAppsCmd())
	cmd.AddCommand(newCompareStagesCmd())
	return cmd
}

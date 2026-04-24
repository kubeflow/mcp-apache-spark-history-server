package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

//nolint:unused
func newLogsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "logs",
		Short: "Download logs for an application",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("TODO: GET /applications/%s/logs\n", appID)
			return nil
		},
	}
}

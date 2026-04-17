package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

//nolint:unused
func newStorageCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "storage",
		Short: "List RDD storage info for an application",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("TODO: GET /applications/%s/storage/rdd\n", appID)
			return nil
		},
	}
}

package streamweaver

import "github.com/spf13/cobra"

func NewStartAllCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Start all StreamWeaver services",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}

	return cmd
}

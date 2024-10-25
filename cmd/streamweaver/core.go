package streamweaver

import "github.com/spf13/cobra"

func NewStartCoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "core",
		Short: "Start the StreamWeaver core service",
		Run: func(cmd *cobra.Command, args []string) {
			err := cmd.Help()
			if err != nil {
				panic(err)
			}
		},
	}

	return cmd
}

package streamweaver

import "github.com/spf13/cobra"

var (
	ConfigFile string
)

func NewBaseCommand(subCommands []*cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "streamweaver",
		Short: "StreamWeaver broker is a distributed message broker using Redis Streams",
		Run: func(cmd *cobra.Command, args []string) {
			err := cmd.Help()
			if err != nil {
				panic(err)
			}
		},
	}

	for _, subCmd := range subCommands {
		cmd.AddCommand(subCmd)
	}

	cmd.PersistentFlags().StringVarP(&ConfigFile, "config", "c", DEFAULT_CONFIG_FILE_PATH, "Path to the configuration file")

	return cmd
}

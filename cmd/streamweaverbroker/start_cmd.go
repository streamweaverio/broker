package streamweaverbroker

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/streamweaverio/broker/cmd/streamweaverbroker/config"
)

func NewStartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the StreamWeaver broker",
		Run: func(cmd *cobra.Command, args []string) {
			// Get configuration
			configFile, _ := cmd.Flags().GetString("config")
			cfg, err := config.ReadConfiguration(configFile)
			if err != nil {
				fmt.Errorf("error reading configuration: %s", err)
				os.Exit(1)
			}

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

			select {
			case <-quit:
				// Graceful shutdown
				os.Exit(0)
			default:
				// Start the broker
			}
		},
	}
}

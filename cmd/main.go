package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/streamweaverio/broker/cmd/streamweaverbroker"
)

func main() {
	startCmd := streamweaverbroker.NewStartCmd()
	simulateCmd := streamweaverbroker.NewSimulateCmd()
	rootCmd := streamweaverbroker.NewBaseCommand([]*cobra.Command{startCmd, simulateCmd})

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

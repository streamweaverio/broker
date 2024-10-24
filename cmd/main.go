package main

import (
	"fmt"
	"os"
	"streamweaver/core/cmd/streamweaver"

	"github.com/spf13/cobra"
)

func main() {
	startAllCmd := streamweaver.NewStartAllCommand()
	startCoreCmd := streamweaver.NewStartCoreCommand()
	rootCmd := streamweaver.NewBaseCommand([]*cobra.Command{startAllCmd, startCoreCmd})

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

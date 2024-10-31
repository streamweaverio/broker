package main

import (
	"fmt"
	"os"
	"streamweaver/core/cmd/streamweaver"

	"github.com/spf13/cobra"
)

func main() {
	startCmd := streamweaver.NewStartCmd()
	rootCmd := streamweaver.NewBaseCommand([]*cobra.Command{startCmd})

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

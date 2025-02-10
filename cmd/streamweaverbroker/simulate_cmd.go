package streamweaverbroker

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/streamweaverio/broker/internal/config"
	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/simulator"
	"go.uber.org/zap"
)

var (
	BrokerUrl string
	Frequency int
)

func NewSimulateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "simulate",
		Short: "Simulate a client sending data to the broker",
		Run: func(cmd *cobra.Command, args []string) {
			brokerUrl, _ := cmd.Flags().GetString("url")
			frequency, _ := cmd.Flags().GetInt("frequency")

			configFile, _ := cmd.Flags().GetString("config")
			cfg, err := config.ReadConfiguration(configFile)
			if err != nil {
				fmt.Printf("Error reading configuration: %v\n", err)
				os.Exit(1)
			}

			logger, err := logging.NewLogger(&logging.LoggerOptions{
				LogLevel:      cfg.Logging.LogLevel,
				LogOutput:     cfg.Logging.LogOutput,
				LogFormat:     cfg.Logging.LogFormat,
				LogFilePrefix: cfg.Logging.LogFilePrefix,
				LogDirectory:  cfg.Logging.LogDirectory,
			})
			if err != nil {
				fmt.Printf("Error creating logger: %v\n", err)
				os.Exit(1)
			}

			client, err := simulator.NewClient(&simulator.SimulatorClientOptions{
				BrokerUrl: brokerUrl,
				Frequency: frequency,
			}, logger)
			if err != nil {
				logger.Error("Error creating client simulator", zap.Error(err))
				os.Exit(1)
			}

			err = client.CreateStream(cmd.Context(), "orders", 300000)
			if err != nil {
				logger.Error("Error creating stream", zap.Error(err))
				os.Exit(1)
			}

			err = client.Start()
			if err != nil {
				logger.Error("Error starting client simulator", zap.Error(err))
				os.Exit(1)
			}
		},
	}

	cmd.Flags().StringVarP(&BrokerUrl, "url", "u", "localhost:3002", "Broker URL")
	cmd.Flags().IntVarP(&Frequency, "frequency", "f", 5, "Message production frequency in seconds")

	return cmd
}

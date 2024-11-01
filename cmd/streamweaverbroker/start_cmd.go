package streamweaverbroker

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/streamweaverio/broker/internal/broker"
	"github.com/streamweaverio/broker/internal/config"
	"github.com/streamweaverio/broker/pkg/process"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const processPIDFile = "streamweaverbroker.pid"

func NewStartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the StreamWeaver broker",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			configFile, _ := cmd.Flags().GetString("config")
			cfg, err := config.ReadConfiguration(configFile)
			if err != nil {
				fmt.Printf("Error reading configuration: %v\n", err)
				os.Exit(1)
			}

			logger, err := logging.New(&logging.Options{
				Level:      cfg.Logging.LogLevel,
				Output:     cfg.Logging.LogOutput,
				Format:     cfg.Logging.LogFormat,
				FilePrefix: cfg.Logging.LogFilePrefix,
				Directory:  cfg.Logging.LogDirectory,
			})
			if err != nil {
				fmt.Printf("Error creating logger: %v\n", err)
				os.Exit(1)
			}

			grpcServer := grpc.NewServer()
			rpcHandler := broker.NewRPCHandler(logger)
			b := broker.New(&broker.Options{
				Ctx:    ctx,
				Port:   cfg.Port,
				Logger: logger,
				Server: grpcServer,
				RPC:    rpcHandler,
			})

			if err := process.CreatePIDFile(processPIDFile, os.Getpid()); err != nil {
				logger.Fatal("Error creating PID file", zap.Error(err))
				os.Exit(1)
			}

			go func() {
				if err := b.Start(); err != nil {
					logger.Fatal("Error starting broker", zap.Error(err))
					cancel()
				}
			}()

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
			<-quit

			b.Stop()
		},
	}
}

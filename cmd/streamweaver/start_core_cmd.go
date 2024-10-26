package streamweaver

import (
	"streamweaver/core/cmd/streamweaver/core"
	"streamweaver/core/pkg/config"
	"streamweaver/core/pkg/logging"
	"streamweaver/core/pkg/redis"

	"github.com/spf13/cobra"
)

func NewStartCoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "core",
		Short: "Start the StreamWeaver core service",
		Run: func(cmd *cobra.Command, args []string) {
			// Get configuration
			configFile, _ := cmd.Flags().GetString("config")
			cfg, err := config.ReadConfiguration(configFile)
			if err != nil {
				panic(err)
			}

			// Logger
			logger, err := logging.NewLogger(&logging.LoggerOptions{
				LogLevel:      cfg.Logging.LogLevel,
				LogOutput:     cfg.Logging.LogOutput,
				LogFormat:     cfg.Logging.LogFormat,
				LogFilePrefix: cfg.Logging.LogFilePrefix,
				LogDirectory:  cfg.Logging.LogDirectory,
				MaxFileSize:   cfg.Logging.MaxFileSize,
			})
			if err != nil {
				panic(err)
			}

			// Redis Client
			redisClient, err := redis.NewClient(&redis.ClientOptions{
				Context:  cmd.Context(),
				Host:     cfg.Redis.Hosts[0].Host,
				Port:     cfg.Redis.Hosts[0].Port,
				DB:       cfg.Redis.DB,
				Password: cfg.Redis.Password,
			}, logger)
			if err != nil {
				panic(err)
			}

			core := core.New(&core.StreamWeaverCoreOptions{
				RedisClient: redisClient,
			}, logger)

			core.Start()
		},
	}

	return cmd
}

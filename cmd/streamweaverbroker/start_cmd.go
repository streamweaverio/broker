package streamweaverbroker

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/streamweaverio/broker/internal/archiver"
	"github.com/streamweaverio/broker/internal/broker"
	"github.com/streamweaverio/broker/internal/config"
	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/redis"
	"github.com/streamweaverio/broker/internal/retention"
	"github.com/streamweaverio/broker/internal/s3"
	"github.com/streamweaverio/broker/internal/storage"
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

			// Create redis cluster client
			redisClient, err := redis.NewClusterClient(&redis.ClusterClientOptions{
				Ctx:            ctx,
				Nodes:          MakeRedisNodeAddresses(cfg.Redis.Hosts),
				Password:       cfg.Redis.Password,
				DB:             cfg.Redis.DB,
				MaxPingRetries: 10,
			}, logger)
			if err != nil {
				logger.Fatal("Error creating Redis cluster client", zap.Error(err))
				os.Exit(1)
			}

			metadataService := redis.NewStreamMetadataService(ctx, redisClient, logger)

			redisStreamService := redis.NewRedisStreamService(&redis.RedisStreamServiceOptions{
				Ctx:                    ctx,
				MetadataService:        metadataService,
				RedisClient:            redisClient,
				GlobalRetentionOptions: cfg.Retention,
			}, logger)

			grpcServer := grpc.NewServer()
			// RPC Handler for broker
			rpcHandler := broker.NewRPCHandler(redisStreamService, logger)

			// Create storage from storage config
			storageDriver, err := GetStorage(cfg, logger)
			if err != nil {
				logger.Fatal("error creating storage", zap.Error(err))
				os.Exit(1)
			}

			// Create archiver instance with storage driver
			archiver := archiver.New(&archiver.ArchiverOptions{
				Storage: storageDriver,
			}, logger)

			// Retention Manager
			retentionManager, err := retention.NewRetentionManager(&retention.RetentionManagerOptions{
				Interval: 30,
			}, logger)
			if err != nil {
				logger.Fatal("error creating retention manager", zap.Error(err))
				os.Exit(1)
			}

			// Register retention policies
			// Time Retention Policy (default)
			timeRetentionPolicy := retention.NewTimeRetentionPolicy(&retention.TimeRetentionPolicyOpts{
				CancelCtx:             ctx,
				StreamMetadataservice: metadataService,
				Streamservice:         redisStreamService,
				RegistryKey:           redis.STREAM_REGISTRY_KEY,
				Archiver:              archiver,
			}, logger)
			retentionManager.RegisterPolicy(&retention.RetentionPolicy{Name: "time", Rule: timeRetentionPolicy})

			// Create broker
			b := broker.New(&broker.Options{
				Ctx:    ctx,
				Port:   cfg.Port,
				Logger: logger,
				Server: grpcServer,
				RPC:    rpcHandler,
			})

			if err := process.CreatePIDFile(processPIDFile, os.Getpid()); err != nil {
				logger.Fatal("error creating PID file", zap.Error(err))
				os.Exit(1)
			}

			go func() {
				if err := b.Start(); err != nil {
					logger.Fatal("error starting broker", zap.Error(err))
					cancel()
				}
			}()

			go func() {
				if err := retentionManager.Start(); err != nil {
					logger.Fatal("error starting retention manager", zap.Error(err))
					cancel()
				}
			}()

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
			<-quit

			b.Stop()
			retentionManager.Stop()

			if err := process.RemovePIDFile(processPIDFile); err != nil {
				logger.Error("error removing PID file", zap.Error(err))
				os.Exit(1)
			}
		},
	}
}

// Create a list of redis node addresses
func MakeRedisNodeAddresses(hosts []*config.RedisHostConfig) []string {
	var nodes []string
	for _, host := range hosts {
		nodes = append(nodes, fmt.Sprintf("%s:%d", host.Host, host.Port))
	}
	return nodes
}

// Creates a storage manager
func GetStorage(cfg *config.StreamWeaverConfig, logger logging.LoggerContract) (storage.Storage, error) {
	if cfg.Storage.Provider == "s3" {
		client, err := s3.NewClient(&s3.S3ClientOptions{
			AccessKeyId:     cfg.Storage.S3.AccessKeyId,
			AccessKeySecret: cfg.Storage.S3.SecretAccessKey,
			Region:          cfg.Storage.S3.Region,
		})
		if err != nil {
			return nil, err
		}

		return storage.NewS3Storage(&storage.S3StorageOptions{
			Client:     client,
			BucketName: cfg.Storage.S3.Bucket,
		}, logger)
	} else if cfg.Storage.Provider == "local" {
		return storage.NewLocalFilesystemDriver(cfg.Storage.Local.Directory)
	}

	return nil, fmt.Errorf("unknown storage provider: %s", cfg.Storage.Provider)
}

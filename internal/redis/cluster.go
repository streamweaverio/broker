package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const DEFAULT_PING_ATTEMPTS = 10
const DEFAULT_PING_BACKOFF_LIMIT = 60

type ClusterClientOptions struct {
	Ctx              context.Context
	Nodes            []string
	Password         string
	DB               int
	MaxPingRetries   int
	PingBackoffLimit int
}

func NewClusterClient(opts *ClusterClientOptions, logger *zap.Logger) (*redis.ClusterClient, error) {
	var lastError error = nil
	pingAttemps := 0
	pingBackoff := backoff.NewExponentialBackOff()

	if len(opts.Nodes) < 1 {
		return nil, NotEnoughNodesError()
	}

	if opts.MaxPingRetries == 0 {
		opts.MaxPingRetries = DEFAULT_PING_ATTEMPTS
	}

	if opts.PingBackoffLimit == 0 {
		opts.PingBackoffLimit = DEFAULT_PING_BACKOFF_LIMIT
	}

	pingBackoff.MaxElapsedTime = time.Duration(opts.PingBackoffLimit) * time.Second

	logger.Info("Connecting to Redis cluster", zap.Strings("nodes", opts.Nodes))

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: opts.Nodes,
		NewClient: func(opt *redis.Options) *redis.Client {
			if opts.Password != "" {
				opt.Password = opts.Password
			}
			opt.DB = opts.DB
			return redis.NewClient(opt)
		},
	})

	for {
		ping, err := client.Ping(opts.Ctx).Result()
		if err == nil && ping == "PONG" {
			pingBackoff.Reset()
			break
		}

		pingAttemps++
		lastError = err
		nextRetryTime := time.Now().Add(pingBackoff.NextBackOff())
		logger.Error("failed to connect to Redis cluster", zap.Error(err), zap.Time("next_retry_at", nextRetryTime))

		if pingAttemps >= opts.MaxPingRetries {
			lastError = fmt.Errorf("failed to connect to Redis cluster after %d attempts", opts.MaxPingRetries)
			break
		}

		logger.Info("Retrying connection to Redis cluster", zap.Int("attempt", pingAttemps))
		time.Sleep(pingBackoff.NextBackOff())
	}

	if lastError != nil {
		return nil, lastError
	}

	logger.Info("Connected to Redis cluster")

	return client, nil
}

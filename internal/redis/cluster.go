package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/pkg/utils"
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

type ClusterNodeInfo struct {
	ID          string   `json:"id"`
	Address     string   `json:"address"`
	Hostname    string   `json:"hostname"`
	Flags       []string `json:"flags"`
	Master      string   `json:"master"`
	PingSent    int64    `json:"ping_sent"`
	PingRecv    int64    `json:"ping_recv"`
	ConfigEpoch int64    `json:"config_epoch"`
	LinkState   string   `json:"link_state"`
	Slot        string   `json:"slot"`
}

type ClusterInfo struct {
	Nodes []*ClusterNodeInfo
}

func GetClusterInfo(context context.Context, client *redis.ClusterClient) (*ClusterInfo, error) {
	clusterInfo := &ClusterInfo{}

	// Run the CLUSTER NODES command
	result, err := client.Do(context, "CLUSTER", "NODES").Result()
	if err != nil {
		return nil, err
	}

	nodesStr, ok := result.(string)
	if !ok {
		return nil, errors.New("unexpected type for CLUSTER NODES result")
	}

	// Parse each line of CLUSTER NODES output
	for _, line := range strings.Split(nodesStr, "\n") {
		if line == "" {
			continue
		}

		fields := strings.Split(line, " ")
		if len(fields) < 8 {
			return nil, fmt.Errorf("invalid CLUSTER NODES line format: %s", line)
		}

		// Parse the slot information
		slot := ""
		for _, field := range fields[8:] {
			if strings.Contains(field, "-") || strings.Contains(field, "[") {
				slot = field
				break
			}
		}

		nodeInfo := &ClusterNodeInfo{
			ID:          fields[0],
			Address:     fields[1],
			Hostname:    strings.Split(fields[1], ":")[0],
			Flags:       strings.Split(fields[2], ","),
			Master:      fields[3],
			PingSent:    utils.ParseInt64(fields[4]),
			PingRecv:    utils.ParseInt64(fields[5]),
			ConfigEpoch: utils.ParseInt64(fields[6]),
			LinkState:   fields[7],
			Slot:        slot,
		}

		clusterInfo.Nodes = append(clusterInfo.Nodes, nodeInfo)
	}

	return clusterInfo, nil
}

func NewClusterClient(opts *ClusterClientOptions, logger logging.LoggerContract) (*redis.ClusterClient, error) {
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
		Addrs:    opts.Nodes,
		Password: opts.Password,
		NewClient: func(opt *redis.Options) *redis.Client {
			opt.DB = opts.DB
			client := redis.NewClient(opt)
			return client
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

	logger.Info("Connected to Redis cluster, nodes:")

	return client, nil
}

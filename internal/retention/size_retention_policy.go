package retention

import (
	"context"

	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/redis"
	"go.uber.org/zap"
)

type SizeRetentionPolicy struct {
	Key    string
	Redis  redis.RedisStreamClient
	Logger logging.LoggerContract
}

func NewSizeRetentionPolicy(redis redis.RedisStreamClient, bucketkey string, logger logging.LoggerContract) *SizeRetentionPolicy {
	return &SizeRetentionPolicy{
		Key:    bucketkey,
		Redis:  redis,
		Logger: logger,
	}
}

func (s *SizeRetentionPolicy) Enforce() error {
	s.Logger.Info("Retrieving affected streams...")
	streams, err := s.Redis.SMembers(context.TODO(), s.Key).Result()
	if err != nil {
		return err
	}
	streamCount := len(streams)
	s.Logger.Info("Found streams with policy attached", zap.Int("count", streamCount))
	return nil
}

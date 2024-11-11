package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/redis"
	"github.com/streamweaverio/broker/pkg/utils"
	"go.uber.org/zap"
)

type TimeRetentionPolicy struct {
	Ctx    context.Context
	Key    string
	Redis  redis.RedisStreamClient
	Logger logging.LoggerContract
}

type TimeRetentionPolicyOpts struct {
	Ctx   context.Context
	Redis redis.RedisStreamClient
	Key   string
}

func NewTimeRetentionPolicy(opts *TimeRetentionPolicyOpts, logger logging.LoggerContract) *TimeRetentionPolicy {
	return &TimeRetentionPolicy{
		Ctx:    opts.Ctx,
		Key:    opts.Key,
		Redis:  opts.Redis,
		Logger: logger,
	}
}

func (s *TimeRetentionPolicy) Enforce() error {
	s.Logger.Debug("Retrieving affected streams...", zap.String("policy", "time"))
	streams, err := s.GetStreams()
	if err != nil {
		return err
	}

	streamCount := len(streams)
	s.Logger.Info("Found streams with time retention policy attached", zap.Int("count", streamCount))

	for _, stream := range streams {
		s.Logger.Info("Applying time retention policy to stream...", zap.String("hash", stream))
		err := s.ApplyPolicy(stream)
		if err != nil {
			s.Logger.Error("Failed to apply time retention policy to stream", zap.String("hash", stream), zap.Error(err))
			continue
		}
	}

	return nil
}

func (s *TimeRetentionPolicy) GetStreams() ([]string, error) {
	streams, err := s.Redis.SMembers(s.Ctx, s.Key).Result()
	if err != nil {
		return nil, err
	}

	return streams, nil
}

func (s *TimeRetentionPolicy) GetStreamRetentionPolicy(stream string) (map[string]string, error) {
	result, err := s.Redis.HGetAll(s.Ctx, fmt.Sprintf("%s%s", redis.STREAM_META_DATA_PREFIX, stream)).Result()
	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, fmt.Errorf("stream %s not found", stream)
	}

	maxAge, ok := result["max_age"]
	if !ok {
		return nil, PolicyOptionNotSetError("max_age")
	}

	return map[string]string{
		"max_age":     maxAge,
		"stream_name": result["name"],
	}, nil
}

func (s *TimeRetentionPolicy) CalculateMinID(maxAge string) string {
	maxAgeInSeconds, err := utils.ParseTimeUnitString(maxAge)
	if err != nil {
		return ""
	}

	duration := time.Duration(maxAgeInSeconds) * time.Second
	cutoffTimestamp := time.Now().Add(-duration).UnixMilli()
	redisTimestamp := fmt.Sprintf("%d-0", cutoffTimestamp)

	return redisTimestamp
}

func (s *TimeRetentionPolicy) ApplyPolicy(stream string) error {
	policy, err := s.GetStreamRetentionPolicy(stream)
	if err != nil {
		return err
	}

	s.Logger.Info("Applying time retention policy to stream...", zap.String("name", policy["stream_name"]))
	minID := s.CalculateMinID(policy["max_age"])
	if minID == "" {
		return fmt.Errorf("failed to calculate min ID for stream %s", policy["stream_name"])
	}

	// TODO: Persist messages to storage before trimming
	count, err := s.Redis.XTrimMinID(context.TODO(), policy["stream_name"], minID).Result()
	if err != nil {
		return fmt.Errorf("failed to trim stream %s: %w", policy["stream_name"], err)
	}

	s.Logger.Info("Applied time retention policy to stream", zap.String("name", policy["stream_name"]), zap.Int64("message_affected", count))
	return nil
}

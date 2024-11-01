package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/config"
	"github.com/streamweaverio/broker/internal/logging"
	"go.uber.org/zap"
)

type CreateStreamParameters struct {
	Name            string
	RetentionPolicy string
	MaxSize         int64
	MaxAge          string
}

type RedisStreamServiceContract interface {
	// Create a new Redis stream for prodcuing and consuming messages
	CreateStream(params *CreateStreamParameters) error
}

// Implements RedisStreamServiceContract
type RedisStreamService struct {
	Ctx context.Context
	// TODO: Abstract the Redis client to an interface to allow for easier testing
	Client RedisStreamClient
	Logger logging.LoggerContract
	Opts   *RedisStreamServiceOptions
}

type RedisStreamServiceOptions struct {
	GlobalRetentionOptions *config.RetentionConfig
}

func NewRedisStreamService(ctx context.Context, client RedisStreamClient, logger logging.LoggerContract, opts *RedisStreamServiceOptions) *RedisStreamService {
	return &RedisStreamService{
		Client: client,
		Logger: logger,
		Ctx:    ctx,
		Opts:   opts,
	}
}

func (p *CreateStreamParameters) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("stream name is required")
	}

	return nil
}

func (s *RedisStreamService) CreateStream(params *CreateStreamParameters) error {
	s.Logger.Debug("Creating stream...", zap.String("name", params.Name))

	if params.RetentionPolicy == "" {
		params.RetentionPolicy = s.Opts.GlobalRetentionOptions.Policy
	}

	if params.RetentionPolicy == "time" && params.MaxAge == "" {
		params.MaxAge = s.Opts.GlobalRetentionOptions.MaxAge
	}

	if params.RetentionPolicy == "size" && params.MaxSize == 0 {
		params.MaxSize = s.Opts.GlobalRetentionOptions.MaxSize
	}

	err := params.Validate()
	if err != nil {
		return err
	}

	args := &redis.XAddArgs{
		Stream: params.Name,
		Values: map[string]interface{}{
			"message": "stream created",
		},
	}
	// TODO: Implement retention logic
	id, err := s.Client.XAdd(s.Ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	// Remove the dummy message used to create the stream
	_, err = s.Client.XDel(s.Ctx, params.Name, id).Result()
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	s.Logger.Debug("Stream created", zap.String("name", params.Name))
	return nil
}

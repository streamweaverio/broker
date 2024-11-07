package redis

import (
	"context"
	"fmt"
	"time"

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

type StreamMetadata struct {
	Name            string
	RetentionPolicy string
	MaxSize         int64
	MaxAge          string
	CreatedAt       int64
	UpdatedAt       int64
}

type RedisStreamServiceContract interface {
	// Create a new Redis stream for prodcuing and consuming messages
	CreateStream(params *CreateStreamParameters) error
}

// Implements RedisStreamServiceContract
type RedisStreamService struct {
	Ctx    context.Context
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
	id, err := s.Client.XAdd(s.Ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	err = s.WriteStreamMetadata(&StreamMetadata{
		Name:            params.Name,
		RetentionPolicy: params.RetentionPolicy,
		MaxSize:         params.MaxSize,
		MaxAge:          params.MaxAge,
		CreatedAt:       time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("failed to write stream metadata: %w", err)
	}

	// Add the stream to the retention bucket for the retention policy. This is used by the retention process to manage the stream.
	err = s.AddToRetentionBucket(params.Name, params.RetentionPolicy)
	if err != nil {
		return err
	}

	// Remove the dummy message used to create the stream
	_, err = s.Client.XDel(s.Ctx, params.Name, id).Result()
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	s.Logger.Debug("Stream created", zap.String("name", params.Name))
	return nil
}
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
	Name   string
	MaxAge string
}

type StreamMetadata struct {
	Name      string
	MaxAge    string
	CreatedAt int64
	UpdatedAt int64
}

type RedisStreamServiceContract interface {
	// Create a new Redis stream for prodcuing and consuming messages
	CreateStream(params *CreateStreamParameters) error
}

// Implements RedisStreamServiceContract
type RedisStreamService struct {
	Ctx                    context.Context
	Client                 RedisStreamClient
	StreamMetadataService  StreamMetadataService
	Logger                 logging.LoggerContract
	GlobalRetentionOptions *config.RetentionConfig
}

type RedisStreamServiceOptions struct {
	Ctx                    context.Context
	MetadataService        StreamMetadataService
	RedisClient            RedisStreamClient
	GlobalRetentionOptions *config.RetentionConfig
}

func NewRedisStreamService(opts *RedisStreamServiceOptions, logger logging.LoggerContract) *RedisStreamService {
	return &RedisStreamService{
		Client:                 opts.RedisClient,
		StreamMetadataService:  opts.MetadataService,
		Logger:                 logger,
		Ctx:                    opts.Ctx,
		GlobalRetentionOptions: opts.GlobalRetentionOptions,
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

	if params.MaxAge == "" {
		params.MaxAge = s.GlobalRetentionOptions.MaxAge
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

	err = s.StreamMetadataService.WriteStreamMetadata(&StreamMetadata{
		Name:      params.Name,
		MaxAge:    params.MaxAge,
		CreatedAt: time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("failed to write stream metadata: %w", err)
	}

	// Add the stream to the retention bucket
	err = s.StreamMetadataService.AddToRetentionBucket(params.Name, STREAM_RETENTION_BUCKET_KEY)
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

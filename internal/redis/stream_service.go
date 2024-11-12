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
	Name          string
	CleanupPolicy string
	MaxAge        int64
}

type StreamMetadata struct {
	Name          string
	MaxAge        int64
	CleanupPolicy string
	CreatedAt     int64
	UpdatedAt     int64
}

type RedisStreamService interface {
	// Create a new Redis stream for prodcuing and consuming messages
	CreateStream(params *CreateStreamParameters) error
	DeleteMessagesOlderThan(streamName string, minId string) error
}

// Implements RedisStreamServiceContract
type RedisStreamServiceImpl struct {
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

func NewRedisStreamService(opts *RedisStreamServiceOptions, logger logging.LoggerContract) RedisStreamService {
	return &RedisStreamServiceImpl{
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

func (s *RedisStreamServiceImpl) CreateStream(params *CreateStreamParameters) error {
	s.Logger.Debug("Creating stream...", zap.String("name", params.Name))
	var cleanupPolicyBucket string

	if params.MaxAge == 0 {
		params.MaxAge = s.GlobalRetentionOptions.MaxAge
	}

	if params.CleanupPolicy == "" {
		params.CleanupPolicy = s.GlobalRetentionOptions.CleanupPolicy
	}

	err := params.Validate()
	if err != nil {
		return err
	}

	switch params.CleanupPolicy {
	case "delete":
		cleanupPolicyBucket = STREAM_CLEANUP_BUCKET_DELETE
	case "archive":
		cleanupPolicyBucket = STREAM_CLEANUP_BUCKET_ARCHIVE
	case "delete,archive":
		cleanupPolicyBucket = STREAM_CLEANUP_BUCKET_DELETE_ARCHIVE
	default:
		cleanupPolicyBucket = STREAM_CLEANUP_BUCKET_DELETE
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

	err = s.StreamMetadataService.AddToRegistry(params.Name)
	if err != nil {
		return fmt.Errorf("failed to add stream to registry: %w", err)
	}

	err = s.StreamMetadataService.WriteStreamMetadata(&StreamMetadata{
		Name:          params.Name,
		MaxAge:        params.MaxAge,
		CleanupPolicy: params.CleanupPolicy,
		CreatedAt:     time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("failed to write stream metadata: %w", err)
	}

	err = s.StreamMetadataService.AddToCleanupBucket(params.Name, cleanupPolicyBucket)
	if err != nil {
		return fmt.Errorf("failed to add stream to cleanup bucket: %w", err)
	}

	// Remove the dummy message used to create the stream
	_, err = s.Client.XDel(s.Ctx, params.Name, id).Result()
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	s.Logger.Debug("Stream created", zap.String("name", params.Name))
	return nil
}

func (s *RedisStreamServiceImpl) DeleteMessagesOlderThan(streamName string, minId string) error {
	_, err := s.Client.XTrimMinID(s.Ctx, streamName, minId).Result()
	if err != nil {
		return fmt.Errorf("failed to delete messages from stream %s: %w", streamName, err)
	}
	return nil
}

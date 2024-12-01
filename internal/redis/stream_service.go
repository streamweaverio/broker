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
	// Create a new Redis stream for producing and consuming messages
	CreateStream(params *CreateStreamParameters) error
	// Count messages older than a given ID in a stream
	CountMessagesOlderThan(streamName string, minId string) (int64, error)
	// Delete messages older than a given ID from a stream
	DeleteMessagesOlderThan(streamName string, minId string) error
	// Get messages older than a given ID from a stream
	GetMessagesOlderThan(streamName string, minId string, count int64) ([]redis.XMessage, error)
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

func (s *RedisStreamServiceImpl) CountMessagesOlderThan(streamName string, minId string) (int64, error) {
	info, err := s.Client.XInfoStream(s.Ctx, streamName).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get stream info for %s: %w", streamName, err)
	}

	if minId >= info.FirstEntry.ID {
		return 0, nil
	}

	var count int64
	var lastId = "-" // Start from the beginning of the stream
	batchSize := int64(1000)

	for {
		messages, err := s.Client.XRangeN(s.Ctx, streamName, lastId, minId, batchSize).Result()
		if err != nil {
			return 0, fmt.Errorf("failed to get messages from stream %s: %w", streamName, err)
		}

		if len(messages) == 0 {
			break
		}

		// Count messages older than minId
		for _, msg := range messages {
			if msg.ID >= minId {
				break
			}
			count++
		}

		// If we got less than batch size or last message ID is >= minId, we're done
		if len(messages) < int(batchSize) || messages[len(messages)-1].ID >= minId {
			break
		}

		// Update lastID for next batch
		lastId = messages[len(messages)-1].ID
	}

	return count, nil
}

func (s *RedisStreamServiceImpl) DeleteMessagesOlderThan(streamName string, minId string) error {
	_, err := s.Client.XTrimMinID(s.Ctx, streamName, minId).Result()
	if err != nil {
		return fmt.Errorf("failed to delete messages from stream %s: %w", streamName, err)
	}
	return nil
}

func (s *RedisStreamServiceImpl) GetMessagesOlderThan(streamName string, minId string, count int64) ([]redis.XMessage, error) {
	// Use "-" to start from beginning, and minId as the end (exclusive)
	messages, err := s.Client.XRangeN(s.Ctx, streamName, "-", minId, count).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get messages from stream %s: %w", streamName, err)
	}
	return messages, nil
}

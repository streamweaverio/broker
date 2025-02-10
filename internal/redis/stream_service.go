package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/config"
	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/pkg/utils"
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

type StreamPublishResult struct {
	MessageIds []string
	Published  int
	Failed     int
	Errors     []error
}

type RedisStreamService interface {
	// Create a new Redis stream for producing and consuming messages
	CreateStream(params *CreateStreamParameters) error
	// Count messages older than a given ID in a stream
	CountMessagesOlderThan(streamName string, minId string, batchSize int64) (int64, error)
	// Delete messages older than a given ID from a stream
	DeleteMessagesOlderThan(streamName string, minId string) error
	// Get messages older than a given ID from a stream
	GetMessagesOlderThan(streamName string, minId string, count int64) ([]redis.XMessage, error)
	// Publish messages to a stream
	PublishMessages(streamName string, messages [][]byte) (*StreamPublishResult, error)
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

func (r *StreamPublishResult) IncrementPublished() {
	r.Published++
}

func (r *StreamPublishResult) IncrementFailed() {
	r.Failed++
}

func (r *StreamPublishResult) AddMessageId(id string) {
	r.MessageIds = append(r.MessageIds, id)
}

func (r *StreamPublishResult) AddError(err error) {
	r.Errors = append(r.Errors, err)
}

func (s *RedisStreamServiceImpl) StreamExists(streamName string) (bool, error) {
	_, err := s.Client.XInfoStream(s.Ctx, streamName).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}

		if err.Error() == "ERR no such key" {
			return false, nil
		}
	}
	return true, nil
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

func (s *RedisStreamServiceImpl) CountMessagesOlderThan(streamName string, minId string, batchSize int64) (int64, error) {
	if streamName == "" {
		return 0, fmt.Errorf("stream name cannot be empty")
	}
	if minId == "" {
		return 0, fmt.Errorf("minId cannot be empty")
	}

	info, err := s.Client.XInfoStream(s.Ctx, streamName).Result()
	if err != nil {
		if err == redis.Nil || err.Error() == "ERR no such key" {
			return 0, StreamNotFoundError(streamName)
		}
		return 0, fmt.Errorf("failed to get stream info for %s: %w", streamName, err)
	}

	minIdTimestamp, err := utils.GetTimestampFromStreamMessageID(minId)
	if err != nil {
		return 0, fmt.Errorf("failed to get timestamp from stream message ID: %w", err)
	}

	if info.FirstEntry.ID != "" {
		firstEntryTimestamp, err := utils.GetTimestampFromStreamMessageID(info.FirstEntry.ID)
		if err != nil {
			return 0, fmt.Errorf("failed to get timestamp from stream message ID: %w", err)
		}

		if minIdTimestamp < firstEntryTimestamp {
			return 0, nil
		}
	}

	var count int64
	lastId := "-" // Start from the beginning

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
			msgTimestamp, err := utils.GetTimestampFromStreamMessageID(msg.ID)
			if err != nil {
				return 0, fmt.Errorf("failed to get timestamp from stream message ID: %w", err)
			}

			if msgTimestamp >= minIdTimestamp {
				break
			}
			count++
		}

		// Break conditions
		lastId = messages[len(messages)-1].ID
		lastIdTimestamp, err := utils.GetTimestampFromStreamMessageID(lastId)
		if err != nil {
			return 0, fmt.Errorf("failed to get timestamp from stream message ID: %w", err)
		}

		if int64(len(messages)) < batchSize || lastIdTimestamp >= minIdTimestamp {
			break
		}

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

// Publish messages to a stream
func (s *RedisStreamServiceImpl) PublishMessages(streamName string, messages [][]byte) (*StreamPublishResult, error) {
	result := StreamPublishResult{
		MessageIds: make([]string, 0),
		Published:  0,
		Failed:     0,
		Errors:     make([]error, 0),
	}
	// Check if stream exists
	exists, err := s.StreamExists(streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to check if stream exists: %w", err)
	}

	if !exists {
		return nil, StreamNotFoundError(streamName)
	}

	messagesToPublish := ByteSliceToRedisMessageMapSlice(messages)

	for _, message := range messagesToPublish {
		args := &redis.XAddArgs{
			Stream: streamName,
			Values: message,
		}

		id, err := s.Client.XAdd(s.Ctx, args).Result()
		if err != nil {
			streamPublishError := StreamPublishError(err)
			result.IncrementFailed()
			result.AddError(streamPublishError)
			continue
		}

		result.IncrementPublished()

		result.AddMessageId(id)
	}

	return &result, nil
}

package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/pkg/utils"
	"go.uber.org/zap"
)

type StreamMetadataService interface {
	AddToRegistry(streamName string) error
	AddToCleanupBucket(streamName string, bucketKey string) error
	GetStreamMetadata(streamHash string) (*StreamMetadata, error)
	ListStreams() ([]string, error)
	WriteStreamMetadata(value *StreamMetadata) error
}

type StreamMetadataServiceImpl struct {
	Ctx    context.Context
	Logger logging.LoggerContract
	Client RedisStreamClient
}

func NewStreamMetadataService(ctx context.Context, client RedisStreamClient, logger logging.LoggerContract) StreamMetadataService {
	return &StreamMetadataServiceImpl{
		Ctx:    ctx,
		Logger: logger,
		Client: client,
	}
}

func (s *StreamMetadataServiceImpl) WriteStreamMetadata(value *StreamMetadata) error {
	streamHash := utils.HashString(value.Name)
	key := fmt.Sprintf("%s%s", STREAM_META_DATA_PREFIX, streamHash)
	s.Logger.Debug("Preparing to write stream metadata to Redis...", zap.String("key", key))

	// Retrieve existing metadata
	existingMetadata, err := s.Client.HGetAll(s.Ctx, key).Result()
	if err != nil {
		s.Logger.Error("Failed to retrieve existing stream metadata", zap.String("key", key), zap.Error(err))
		return fmt.Errorf("failed to get stream metadata: %w", err)
	}

	s.Logger.Debug("Fetched existing metadata", zap.String("key", key), zap.Any("existingMetadata", existingMetadata))

	// Update or set metadata fields
	metadata := make(map[string]interface{})
	if value.Name != "" && value.Name != existingMetadata["name"] {
		metadata["name"] = value.Name
	}
	if value.CleanupPolicy != "" && value.CleanupPolicy != existingMetadata["cleanup_policy"] {
		metadata["cleanup_policy"] = value.CleanupPolicy
	}

	metadata["max_age"] = strconv.FormatInt(value.MaxAge, 10)
	metadata["updated_at"] = strconv.FormatInt(time.Now().Unix(), 10)
	if existingMetadata["created_at"] == "" {
		metadata["created_at"] = metadata["updated_at"]
	}

	// Log the final metadata that will be written
	s.Logger.Debug("Final metadata to write/update in Redis", zap.String("key", key), zap.Any("metadata", metadata))

	// Write to Redis if metadata has values
	if len(metadata) > 0 {
		err = s.Client.HSet(s.Ctx, key, metadata).Err()
		if err != nil {
			s.Logger.Error("Failed to write stream metadata to Redis", zap.String("key", key), zap.Any("metadata", metadata), zap.Error(err))
			return fmt.Errorf("failed to update stream metadata: %w", err)
		}
		s.Logger.Debug("Successfully updated stream metadata in Redis", zap.String("key", key), zap.Any("metadata", metadata))
	} else {
		s.Logger.Debug("No metadata changes detected, skipping write", zap.String("key", key))
	}

	return nil
}

// Adds a stream to the bucket for the cleanup policy
func (s *StreamMetadataServiceImpl) AddToCleanupBucket(streamName string, bucketKey string) error {
	streamHash := utils.HashString(streamName)

	_, err := s.Client.SAdd(s.Ctx, bucketKey, streamHash).Result()
	if err != nil {
		return fmt.Errorf("failed to add stream to cleanup bucket: %w", err)
	}

	s.Logger.Debug("Added stream to cleanup bucket.", zap.String("stream", streamName), zap.String("bucket", bucketKey), zap.String("stream_hash", streamHash))

	return nil
}

// Adds a stream to the registry
func (s *StreamMetadataServiceImpl) AddToRegistry(streamName string) error {
	streamHash := utils.HashString(streamName)

	_, err := s.Client.SAdd(s.Ctx, STREAM_REGISTRY_KEY, streamHash).Result()
	if err != nil {
		return fmt.Errorf("failed to add stream to registry: %w", err)
	}

	s.Logger.Debug("Added stream to registry.", zap.String("stream", streamName), zap.String("stream_hash", streamHash))

	return nil
}

// Gets the metadata for a stream
func (s *StreamMetadataServiceImpl) GetStreamMetadata(hash string) (*StreamMetadata, error) {
	key := fmt.Sprintf("%s%s", STREAM_META_DATA_PREFIX, hash)
	s.Logger.Debug("Fetching stream metadata from Redis...", zap.String("hash", hash), zap.String("key", key))

	// Retrieve metadata from Redis
	response := s.Client.HGetAll(s.Ctx, key)
	if response.Err() != nil {
		s.Logger.Error("Failed to get stream metadata from Redis", zap.String("key", key), zap.Error(response.Err()))
		return nil, fmt.Errorf("failed to get stream metadata: %w", response.Err())
	}

	metadata := response.Val()
	s.Logger.Debug("Fetched metadata from Redis", zap.String("key", key), zap.Any("metadata", metadata))

	// Check if metadata exists
	if len(metadata) == 0 {
		s.Logger.Warn("No metadata found for stream", zap.String("key", key))
		return nil, fmt.Errorf("stream %s not found", hash)
	}

	// Parse and log each field in the metadata
	name := metadata["name"]
	cleanupPolicy := metadata["cleanup_policy"]

	maxAge, err := strconv.ParseInt(metadata["max_age"], 10, 64)
	if err != nil {
		s.Logger.Error("Failed to parse max_age", zap.String("key", key), zap.String("max_age", metadata["max_age"]), zap.Error(err))
		return nil, fmt.Errorf("failed to parse max_age: %w", err)
	}

	createdAt, err := strconv.ParseInt(metadata["created_at"], 10, 64)
	if err != nil {
		s.Logger.Error("Failed to parse created_at", zap.String("key", key), zap.String("created_at", metadata["created_at"]), zap.Error(err))
		return nil, fmt.Errorf("failed to parse created_at: %w", err)
	}

	updatedAt, err := strconv.ParseInt(metadata["updated_at"], 10, 64)
	if err != nil {
		s.Logger.Error("Failed to parse updated_at", zap.String("key", key), zap.String("updated_at", metadata["updated_at"]), zap.Error(err))
		return nil, fmt.Errorf("failed to parse updated_at: %w", err)
	}

	s.Logger.Debug("Parsed metadata fields", zap.String("key", key), zap.String("name", name), zap.Int64("max_age", maxAge), zap.String("cleanup_policy", cleanupPolicy), zap.Int64("created_at", createdAt), zap.Int64("updated_at", updatedAt))

	return &StreamMetadata{
		Name:          name,
		MaxAge:        maxAge,
		CleanupPolicy: cleanupPolicy,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}, nil
}

// Lists all streams in the registry
func (s *StreamMetadataServiceImpl) ListStreams() ([]string, error) {
	streams, err := s.Client.SMembers(s.Ctx, STREAM_REGISTRY_KEY).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list streams: %w", err)
	}

	return streams, nil
}

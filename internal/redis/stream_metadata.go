package redis

import (
	"fmt"
	"strconv"
	"time"

	"github.com/streamweaverio/broker/pkg/utils"
	"go.uber.org/zap"
)

func (s *RedisStreamService) WriteStreamMetadata(value *StreamMetadata) error {
	s.Logger.Debug("Writing stream metadata to Redis...", zap.String("name", value.Name))

	streamHash := utils.HashString(value.Name)
	key := fmt.Sprintf("%s%s", STREAM_META_DATA_PREFIX, streamHash)
	// Exiting metadata
	metadata, err := s.Client.HGetAll(s.Ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to get stream metadata: %w", err)
	}

	if metadata != nil {
		if value.Name != "" && value.Name != metadata["name"] {
			metadata["name"] = value.Name
		}

		if value.RetentionPolicy != "" && value.RetentionPolicy != metadata["retention_policy"] {
			metadata["retention_policy"] = value.RetentionPolicy
		}

		if value.MaxSize != 0 && strconv.FormatInt(value.MaxSize, 10) != metadata["max_size"] {
			metadata["max_size"] = strconv.FormatInt(value.MaxSize, 10)
		}

		if value.MaxAge != "" && value.MaxAge != metadata["max_age"] {
			metadata["max_age"] = value.MaxAge
		}

		metadata["updated_at"] = strconv.FormatInt(time.Now().Unix(), 10)

		err := s.Client.HSet(s.Ctx, key, metadata).Err()
		if err != nil {
			return fmt.Errorf("failed to update stream metadata: %w", err)
		}
		s.Logger.Debug("Updated stream metadata in Redis.", zap.String("key", key), zap.Any("metadata", metadata))
		return nil
	}

	err = s.Client.HSet(s.Ctx, key, map[string]interface{}{
		"name":             value.Name,
		"retention_policy": value.RetentionPolicy,
		"max_size":         strconv.FormatInt(value.MaxSize, 10),
		"max_age":          value.MaxAge,
		"created_at":       strconv.FormatInt(value.CreatedAt, 10),
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to write stream metadata: %w", err)
	}
	s.Logger.Debug("Wrote stream metadata to Redis.", zap.String("key", key), zap.Any("metadata", value))

	return nil
}

// Adds a stream to the bucket for the retention policy
func (s *RedisStreamService) AddToRetentionBucket(streamName string, retentionPolicy string) error {
	streamHash := utils.HashString(streamName)
	var key string
	switch retentionPolicy {
	case "size":
		key = STREAM_RETENTION_POLICY_SIZE_BUCKET_KEY
	case "time":
		key = STREAM_RETENTION_POLICY_TIME_BUCKET_KEY
	default:
		return fmt.Errorf("invalid retention policy: %s", retentionPolicy)
	}

	_, err := s.Client.SAdd(s.Ctx, key, streamHash).Result()
	if err != nil {
		return fmt.Errorf("failed to add stream to retention bucket: %w", err)
	}

	s.Logger.Debug("Added stream to retention bucket.", zap.String("stream", streamName), zap.String("bucket", key), zap.String("stream_hash", streamHash))

	return nil
}

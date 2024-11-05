package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/config"
	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/pkg/utils"
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

func (s *RedisStreamService) WriteStreamMetadata(value *StreamMetadata) error {
	s.Logger.Debug("Writing stream metadata to Redis...", zap.String("name", value.Name))

	streamHash := utils.HashString(value.Name)
	key := fmt.Sprintf("%s%d", STREAM_META_DATA_PREFIX, streamHash)
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

	// Remove the dummy message used to create the stream
	_, err = s.Client.XDel(s.Ctx, params.Name, id).Result()
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	s.Logger.Debug("Stream created", zap.String("name", params.Name))
	return nil
}

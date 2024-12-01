package retention

import (
	"context"
	"fmt"

	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/redis"
	"github.com/streamweaverio/broker/internal/storage"
	"github.com/streamweaverio/broker/pkg/utils"
	"go.uber.org/zap"
)

type TimeRetentionPolicy struct {
	Ctx              context.Context
	Metadataservice  redis.StreamMetadataService
	Streamservice    redis.RedisStreamService
	StorageManager   storage.StorageManager
	Logger           logging.LoggerContract
	RegistryKey      string
	MessageBatchSize int64
}

type TimeRetentionPolicyOpts struct {
	Ctx                   context.Context
	StreamMetadataservice redis.StreamMetadataService
	Streamservice         redis.RedisStreamService
	Redis                 redis.RedisStreamClient
	RegistryKey           string
	MessageBatchSize      int64
	StorageManager        storage.StorageManager
}

func NewTimeRetentionPolicy(opts *TimeRetentionPolicyOpts, logger logging.LoggerContract) *TimeRetentionPolicy {
	if opts.MessageBatchSize <= 0 {
		opts.MessageBatchSize = 1000
	}

	return &TimeRetentionPolicy{
		Ctx:              opts.Ctx,
		Metadataservice:  opts.StreamMetadataservice,
		Streamservice:    opts.Streamservice,
		Logger:           logger,
		RegistryKey:      opts.RegistryKey,
		StorageManager:   opts.StorageManager,
		MessageBatchSize: opts.MessageBatchSize,
	}
}

func (s *TimeRetentionPolicy) Enforce() error {
	s.Logger.Debug("Retrieving affected streams...", zap.String("policy", "time"))
	streams, err := s.Metadataservice.ListStreams()
	if err != nil {
		return err
	}

	streamCount := len(streams)
	s.Logger.Info("Found streams with time retention policy attached", zap.Int("count", streamCount))

	for _, stream := range streams {
		s.Logger.Info("Applying time retention policy to stream...", zap.String("hash", stream))
		err := s.ApplyPolicy(stream)
		if err != nil {
			s.Logger.Error("Failed to apply time retention policy to stream", zap.String("stream_hash", stream), zap.Error(err))
			continue
		}
	}
	return nil
}

// Archives messages older than the minID from the stream
func (s *TimeRetentionPolicy) ArchiveMessages(stream string, minID string) error {
	// Count affected messages
	affectedMsgCount, err := s.Streamservice.CountMessagesOlderThan(stream, minID)
	if err != nil {
		return fmt.Errorf("failed to count messages in stream %s: %w", stream, err)
	}

	if affectedMsgCount == 0 {
		s.Logger.Info("No messages to archive", zap.String("stream", stream))
		return nil
	}

	s.Logger.Info("Starting message archival",
		zap.String("stream", stream),
		zap.Int64("affected_messages", affectedMsgCount))

	var processedCount int64
	lastID := "-" // Start from beginning of stream

	for processedCount < affectedMsgCount {
		// Get batch of messages
		messages, err := s.Streamservice.GetMessagesOlderThan(stream, lastID, s.MessageBatchSize)
		if err != nil {
			return fmt.Errorf("failed to get messages from stream %s: %w", stream, err)
		}

		if len(messages) == 0 {
			break // No more messages to process
		}

		// Archive batch
		err = s.StorageManager.ArchiveMessages(s.Ctx, stream, messages)
		if err != nil {
			return fmt.Errorf("failed to archive messages: %w", err)
		}

		processedCount += int64(len(messages))
		lastID = messages[len(messages)-1].ID // Update last processed ID

		s.Logger.Debug("Archived batch of messages",
			zap.String("stream", stream),
			zap.Int("batch_size", len(messages)),
			zap.Int64("total_processed", processedCount),
			zap.Int64("total_affected", affectedMsgCount))

		// Break if we've reached or exceeded minID
		if lastID >= minID {
			break
		}
	}

	s.Logger.Info("Completed message archival",
		zap.String("stream", stream),
		zap.Int64("archived_messages", processedCount))

	return nil
}

// Deletes messages older than the minID from the stream
func (s *TimeRetentionPolicy) DeleteMessages(stream string, minID string) error {
	err := s.Streamservice.DeleteMessagesOlderThan(stream, minID)
	if err != nil {
		return fmt.Errorf("failed to delete messages from stream %s: %w", stream, err)
	}
	return nil
}

// Deletes and archives messages older than the minID from the stream
func (s *TimeRetentionPolicy) DeleteAndArchiveMessages(stream string, minID string) error {
	// Archive messages first
	err := s.ArchiveMessages(stream, minID)
	if err != nil {
		return err
	}
	// Delete messages
	err = s.DeleteMessages(stream, minID)
	if err != nil {
		return err
	}

	return nil
}

// Applies the cleanup policy to the stream
func (s *TimeRetentionPolicy) ApplyCleanupPolicy(stream string, policy string, minID string) error {
	switch policy {
	case "delete":
		s.Logger.Info("Deleting older messages from stream...", zap.String("stream", stream), zap.String("min_id", minID))
		return s.DeleteMessages(stream, minID)
	case "archive":
		s.Logger.Info("Archiving older messages from stream...", zap.String("stream", stream), zap.String("min_id", minID))
		return s.ArchiveMessages(stream, minID)
	case "delete,archive":
		s.Logger.Info("Deleting and archiving older messages from stream...", zap.String("stream", stream), zap.String("min_id", minID))
		return s.DeleteAndArchiveMessages(stream, minID)
	default:
		return fmt.Errorf("unknown cleanup policy: %s", policy)
	}
}

func (s *TimeRetentionPolicy) ApplyPolicy(stream string) error {
	meta, err := s.Metadataservice.GetStreamMetadata(stream)
	if err != nil {
		return err
	}

	s.Logger.Info("Applying time retention policy to stream...", zap.String("name", meta.Name))
	minID, err := utils.CalculateRedisStreamMinID(meta.MaxAge)
	if err != nil {
		return fmt.Errorf("failed to calculate min ID for stream %s: %w", meta.Name, err)
	}

	err = s.ApplyCleanupPolicy(meta.Name, meta.CleanupPolicy, minID)
	if err != nil {
		return err
	}

	return nil
}

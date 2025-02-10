package retention

import (
	"context"
	"fmt"

	"github.com/streamweaverio/broker/internal/archiver"
	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/redis"
	"github.com/streamweaverio/broker/pkg/utils"
	"go.uber.org/zap"
)

type TimeRetentionPolicy struct {
	CancelCtx        context.Context
	Metadataservice  redis.StreamMetadataService
	Streamservice    redis.RedisStreamService
	Archiver         archiver.Archiver
	Logger           logging.LoggerContract
	RegistryKey      string
	MessageBatchSize int64
}

type TimeRetentionPolicyOpts struct {
	// Cancel context to allow for cancellation of the retention policy
	CancelCtx             context.Context
	StreamMetadataservice redis.StreamMetadataService
	Streamservice         redis.RedisStreamService
	Redis                 redis.RedisStreamClient
	RegistryKey           string
	MessageBatchSize      int64
	Archiver              archiver.Archiver
}

func NewTimeRetentionPolicy(opts *TimeRetentionPolicyOpts, logger logging.LoggerContract) *TimeRetentionPolicy {
	if opts.MessageBatchSize <= 0 {
		opts.MessageBatchSize = 1000
	}

	return &TimeRetentionPolicy{
		CancelCtx:        opts.CancelCtx,
		Metadataservice:  opts.StreamMetadataservice,
		Streamservice:    opts.Streamservice,
		Archiver:         opts.Archiver,
		Logger:           logger,
		RegistryKey:      opts.RegistryKey,
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
	s.Logger.Debug("Found streams with time retention policy attached", zap.Int("count", streamCount))

	for _, stream := range streams {
		s.Logger.Debug("Applying time retention policy to stream...", zap.String("hash", stream))
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
	affectedMsgCount, err := s.Streamservice.CountMessagesOlderThan(stream, minID, s.MessageBatchSize)
	if err != nil {
		return fmt.Errorf("failed to count messages in stream %s: %w", stream, err)
	}

	if affectedMsgCount == 0 {
		s.Logger.Info("No messages to archive", zap.String("stream", stream))
		return nil
	}

	s.Logger.Debug(fmt.Sprintf("stream: %s has %d messages to archive", stream, affectedMsgCount))

	if affectedMsgCount < s.MessageBatchSize {
		s.Logger.Debug("Message count is less than batch size, archiving all messages in one go", zap.String("stream", stream), zap.Int64("count", affectedMsgCount))
		// Archive all messages in one go
		messages, err := s.Streamservice.GetMessagesOlderThan(stream, minID, s.MessageBatchSize)
		if err != nil {
			return fmt.Errorf("failed to get messages from stream %s: %w", stream, err)
		}

		err = s.Archiver.Archive(s.CancelCtx, stream, messages)
		if err != nil {
			return fmt.Errorf("failed to archive messages: %w", err)
		}
	} else {
		batchCount := affectedMsgCount / s.MessageBatchSize
		remainder := affectedMsgCount % s.MessageBatchSize

		if remainder > 0 {
			batchCount++
		}

		s.Logger.Debug("Archiving messages in batches",
			zap.String("stream", stream),
			zap.Int64("affected_messages", affectedMsgCount),
			zap.Int64("batch_size", s.MessageBatchSize),
			zap.Int64("batch_count", batchCount))

		var currentMinId = minID

		for i := int64(0); i < batchCount; i++ {
			s.Logger.Debug("Processing message batch", zap.Int64("batch", i+1))
			// Get messages for batch
			messages, err := s.Streamservice.GetMessagesOlderThan(stream, currentMinId, s.MessageBatchSize)
			if err != nil {
				s.Logger.Error("Failed to get messages from stream", zap.String("stream", stream), zap.Error(err))
				continue
			}

			// Set new minID for next batch
			currentMinId = messages[len(messages)-1].ID

			// Send to archiver
			err = s.Archiver.Archive(s.CancelCtx, stream, messages)
			if err != nil {
				s.Logger.Error("Failed to archive messages", zap.String("stream", stream), zap.Error(err))
				continue
			}
		}
	}

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

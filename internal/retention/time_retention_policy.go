package retention

import (
	"context"
	"fmt"

	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/redis"
	"github.com/streamweaverio/broker/pkg/utils"
	"go.uber.org/zap"
)

type TimeRetentionPolicy struct {
	Ctx             context.Context
	Metadataservice redis.StreamMetadataService
	Streamservice   redis.RedisStreamService
	Logger          logging.LoggerContract
	RegistryKey     string
}

type TimeRetentionPolicyOpts struct {
	Ctx                   context.Context
	StreamMetadataservice redis.StreamMetadataService
	Streamservice         redis.RedisStreamService
	Redis                 redis.RedisStreamClient
	RegistryKey           string
}

func NewTimeRetentionPolicy(opts *TimeRetentionPolicyOpts, logger logging.LoggerContract) *TimeRetentionPolicy {
	return &TimeRetentionPolicy{
		Ctx:             opts.Ctx,
		Metadataservice: opts.StreamMetadataservice,
		Streamservice:   opts.Streamservice,
		Logger:          logger,
		RegistryKey:     opts.RegistryKey,
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

func (s *TimeRetentionPolicy) ArchiveMessages(stream string, minID string) error {
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

func (s *TimeRetentionPolicy) DeleteAndArchiveMessages(stream string, minID string) error {
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

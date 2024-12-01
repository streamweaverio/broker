package storage

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/logging"
	"go.uber.org/zap"
)

type StorageManager interface {
	// Register a storage driver
	RegisterDriver(name string, driver StorageProviderDriver) error
	// Archive messages using the registered driver
	ArchiveMessages(ctx context.Context, streamName string, messages []redis.XMessage) error
	// Get archived messages from the storage provider
	GetArchivedMessages(ctx context.Context, streamName string, startID, endID string) ([]redis.XMessage, error)
	// Start the storage manager
	Start() error
	// Stop the storage manager
	Stop(ctx context.Context) error
}

type StorageManagerImpl struct {
	Config   *StorageManagerOpts
	Ctx      context.Context
	Cancel   context.CancelFunc
	Logger   logging.LoggerContract
	Driver   StorageProviderDriver
	TaskChan chan StorageTask
	Workers  sync.WaitGroup
}

type StorageTask struct {
	// Type of storage operation; Write, Read
	Type string
	// Stream name
	StreamName string
	// Messages to store
	Messages []redis.XMessage
	// Result channel to notify the caller
	ResultChan chan error
	// Number of retries for the operation
	RetryCount int
	// Metadata for the operation
	Metadata *ArchiveMetadata
	// Time when the operation was created
	CreatedAt time.Time
}

type ArchiveMetadata struct {
	// Stream name
	StreamName string `json:"stream_name"`
	// Number of messages in the archive
	MessageCount int `json:"message_count"`
	// First message ID in the archive
	StartID string `json:"start_id"`
	// Last message ID in the archive
	EndID string `json:"end_id"`
	// Size of the archived data in bytes
	Size int64 `json:"size"`
	// Time when the archive operation was created
	CreatedAt time.Time `json:"created_at"`
}

type StorageManagerOpts struct {
	// Number of workers to use for storage operations
	WorkerPoolSize int
	// Maximum number of retries for storage operations
	MaxRetries int
	// Maximum number of messages to store in the queue
	QueueSize int
	// Backoff limit for storage operations
	BackoffLimit time.Duration
}

func NewStorageManager(opts *StorageManagerOpts, logger logging.LoggerContract) (StorageManager, error) {
	if opts.WorkerPoolSize <= 0 {
		opts.WorkerPoolSize = 5
	}

	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 3
	}

	if opts.QueueSize <= 0 {
		opts.QueueSize = 1000
	}

	if opts.BackoffLimit <= 0 {
		opts.BackoffLimit = 60 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &StorageManagerImpl{
		Config:   opts,
		Logger:   logger,
		TaskChan: make(chan StorageTask, opts.QueueSize),
		Ctx:      ctx,
		Cancel:   cancel,
	}, nil
}

func (s *StorageManagerImpl) RegisterDriver(name string, driver StorageProviderDriver) error {
	if driver == nil {
		return fmt.Errorf("driver is nil")
	}

	if s.Driver != nil {
		return fmt.Errorf("driver already registered")
	}

	s.Logger.Info("Registering storage driver...", zap.String("name", name))
	s.Driver = driver
	return nil
}

func (s *StorageManagerImpl) ArchiveMessages(ctx context.Context, streamName string, messages []redis.XMessage) error {
	if len(messages) == 0 {
		return nil
	}

	if s.Driver == nil {
		return fmt.Errorf("no storage driver registered")
	}

	resultChan := make(chan error, 1)
	task := StorageTask{
		Type:       "Write",
		StreamName: streamName,
		Messages:   messages,
		ResultChan: resultChan,
		RetryCount: s.Config.MaxRetries,
		Metadata: &ArchiveMetadata{
			StreamName:   streamName,
			StartID:      messages[0].ID,
			EndID:        messages[len(messages)-1].ID,
			MessageCount: len(messages),
			CreatedAt:    time.Now(),
		},
		CreatedAt: time.Now(),
	}

	// Send operation to worker pool
	select {
	case s.TaskChan <- task:
		s.Logger.Debug("Queued archive operation", zap.String("stream", streamName), zap.Int("message_count", len(messages)))
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while queueing archive operation")
	default:
		return fmt.Errorf("storage operation queue is full")
	}

	// Wait for operation result
	select {
	case err := <-resultChan:
		return err
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for archive operation")
	}
}

func (s *StorageManagerImpl) GetArchivedMessages(ctx context.Context, streamName string, startID, endID string) ([]redis.XMessage, error) {
	return nil, nil
}

func (s *StorageManagerImpl) Start() error {
	if s.Driver == nil {
		return fmt.Errorf("no storage driver registered")
	}

	s.Logger.Info("Starting storage manager...", zap.Int("worker_pool_size", s.Config.WorkerPoolSize), zap.Int("queue_size", s.Config.QueueSize))

	for i := 0; i < s.Config.WorkerPoolSize; i++ {
		s.Workers.Add(1)
		go s.Process(i)
	}
	return nil
}

// Worker function to process storage operations
func (s *StorageManagerImpl) Process(id int) {
	defer s.Workers.Done()
	s.Logger.Info("Starting worker...", zap.Int("id", id))

	for {
		select {
		case <-s.Ctx.Done():
			s.Logger.Info("Stopping worker...", zap.Int("id", id))
			return
		case task := <-s.TaskChan:
			err := s.ExcuteTask(task)
			if err != nil {
				s.Logger.Error("Failed to execute storage task", zap.Error(err), zap.String("type", task.Type), zap.String("stream", task.StreamName))
			}
			// Notify the caller about the result
			if task.ResultChan != nil {
				task.ResultChan <- err
			}
		}
	}
}

func (s *StorageManagerImpl) ExcuteTask(task StorageTask) error {
	if task.Type == "" {
		return fmt.Errorf("task type is empty")
	}

	if !slices.Contains([]string{"Write", "Read"}, task.Type) {
		return fmt.Errorf("invalid task type: %s", task.Type)
	}

	backoff := backoff.NewExponentialBackOff()
	backoff.MaxElapsedTime = s.Config.BackoffLimit

	var lastError error
	attempts := 0

	switch task.Type {
	case "Write":
		for attempts < task.RetryCount {
			err := s.Driver.Write(task.StreamName, task.Messages, task.Metadata)
			if err == nil {
				return nil
			}

			lastError = err
			attempts++
			nextRetryTime := time.Now().Add(backoff.NextBackOff())
			s.Logger.Error("Failed to write messages to storage",
				zap.Error(err),
				zap.Time("next_retry_at", nextRetryTime),
				zap.Int("attempt", attempts),
				zap.Int("max_attempts", task.RetryCount))

			if attempts >= task.RetryCount {
				break
			}

			backoffDuration := backoff.NextBackOff()
			s.Logger.Info("Retrying write operation",
				zap.Int("attempt", attempts),
				zap.Duration("backoff_duration", backoffDuration))
			time.Sleep(backoffDuration)
		}
	case "Read":
		// Implement read logic when needed
		return fmt.Errorf("read operation not implemented")
	}

	if lastError != nil {
		return fmt.Errorf("failed to execute task after %d attempts: %w",
			attempts, lastError)
	}

	return nil
}

func (s *StorageManagerImpl) Stop(ctx context.Context) error {
	s.Logger.Info("Stopping storage manager...")
	s.Cancel()

	done := make(chan struct{})
	go func() {
		s.Workers.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.Logger.Info("Storage manager stopped")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for workers to stop")
	}
}

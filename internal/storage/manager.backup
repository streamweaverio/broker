package storage

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/alitto/pond/v2"
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
	// Configuration for the storage manager
	Config *StorageManagerOpts
	// Context for the storage manager
	Ctx context.Context
	// Cancel function for the storage manager
	Cancel context.CancelFunc
	// Logger for the storage manager
	Logger logging.LoggerContract
	// Storage driver for writing and reading messages
	Driver StorageProviderDriver
	// Worker pool for executing storage tasks
	Pool pond.Pool
}

type StorageTask struct {
	// Type of storage operation; Write, Read
	Type string
	// Stream name
	StreamName string
	// Messages to store
	Messages []redis.XMessage
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

	if opts.BackoffLimit <= 0 {
		opts.BackoffLimit = 60 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create a new worker pool with the specified size and maximum queue capacity
	pool := pond.NewPool(opts.WorkerPoolSize)

	return &StorageManagerImpl{
		Config: opts,
		Logger: logger,
		Pool:   pool,
		Ctx:    ctx,
		Cancel: cancel,
	}, nil
}

// Vaidate StorageTask
func (t *StorageTask) Validate() error {
	if t.Type == "" {
		return fmt.Errorf("task type is empty")
	}

	if !slices.Contains([]string{"Write", "Read"}, t.Type) {
		return fmt.Errorf("invalid task type: %s", t.Type)
	}

	return nil
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

	// Create a new storage task for writing messages to the storage provider
	storageTask := StorageTask{
		Type:       "Write",
		StreamName: streamName,
		Messages:   messages,
		Metadata: &ArchiveMetadata{
			StreamName:   streamName,
			StartID:      messages[0].ID,
			EndID:        messages[len(messages)-1].ID,
			MessageCount: len(messages),
			CreatedAt:    time.Now(),
		},
		CreatedAt: time.Now(),
	}

	// Submit the storage task to the worker pool
	task := s.Pool.SubmitErr(func() error {
		return s.ExecuteStorageTask(storageTask)
	})

	// Wait for the task to complete
	err := task.Wait()
	if err != nil {
		return fmt.Errorf("failed to archive messages: %w", err)
	}

	return nil
}

func (s *StorageManagerImpl) GetArchivedMessages(ctx context.Context, streamName string, startID, endID string) ([]redis.XMessage, error) {
	return nil, nil
}

func (s *StorageManagerImpl) ExecuteWithRetry(operation func() error) error {
	backoff := backoff.NewExponentialBackOff()
	backoff.MaxElapsedTime = s.Config.BackoffLimit

	var lastError error
	attempts := 0

	for attempts < s.Config.MaxRetries {
		err := operation()
		if err == nil {
			return nil
		}

		lastError = err
		attempts++
		nextRetryTime := time.Now().Add(backoff.NextBackOff())
		s.Logger.Error("Operation failed",
			zap.Error(err),
			zap.Time("next_retry_at", nextRetryTime),
			zap.Int("attempt", attempts),
			zap.Int("max_attempts", s.Config.MaxRetries))

		if attempts >= s.Config.MaxRetries {
			break
		}

		backoffDuration := backoff.NextBackOff()
		s.Logger.Info("Retrying operation",
			zap.Int("attempt", attempts),
			zap.Duration("backoff_duration", backoffDuration))
		time.Sleep(backoffDuration)
	}

	return fmt.Errorf("operation failed after %d attempts: %w", attempts, lastError)
}

func (s *StorageManagerImpl) ExecuteStorageTask(task StorageTask) error {
	err := task.Validate()
	if err != nil {
		return fmt.Errorf("invalid storage task: %w", err)
	}

	switch task.Type {
	case "Write":
		return s.ExecuteWithRetry(func() error {
			return s.Driver.Write(task.StreamName, task.Messages, task.Metadata)
		})
	case "Read":
		return fmt.Errorf("read operation not implemented")
	}

	return nil
}

func (s *StorageManagerImpl) Start() error {
	if s.Driver == nil {
		return fmt.Errorf("no storage driver registered")
	}

	s.Logger.Info("Starting storage manager...", zap.Int("worker_pool_size", s.Config.WorkerPoolSize))

	return nil
}

func (s *StorageManagerImpl) Stop(ctx context.Context) error {
	s.Logger.Info("Stopping storage manager...")
	s.Cancel()

	// Stop accepting new tasks and wait for current tasks to complete
	stopped := make(chan struct{})
	go func() {
		s.Pool.StopAndWait()
		close(stopped)
	}()

	select {
	case <-stopped:
		s.Logger.Info("Storage manager stopped")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for workers to stop")
	}
}

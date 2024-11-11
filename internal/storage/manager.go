package storage

import "github.com/streamweaverio/broker/internal/logging"

type StorageManager interface {
	// Register a storage driver
	RegisterDriver(driver StorageProviderDriver) StorageManager
	Start() error
}

type StorageManagerImpl struct {
	Logger logging.LoggerContract
	Driver StorageProviderDriver
}

type StorageManagerOpts struct {
	// Number of workers to use for storage operations
	WorkerPoolSize int
}

func NewStorageManager(opts *StorageManagerOpts, logger logging.LoggerContract) (StorageManager, error) {
	return &StorageManagerImpl{
		Logger: logger,
	}, nil
}

func (s *StorageManagerImpl) RegisterDriver(driver StorageProviderDriver) StorageManager {
	s.Driver = driver
	return s
}

func (s *StorageManagerImpl) Start() error {
	s.Logger.Info("Starting storage manager...")
	return nil
}

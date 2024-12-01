package storage

import (
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/config"
)

type StorageProviderDriver interface {
	Write(streamName string, messages []redis.XMessage, metadata *ArchiveMetadata) error
}

func NewStorageProviderDriver(cfg *config.StorageConfig) (StorageProviderDriver, error) {
	switch cfg.Provider {
	case "local":
		return NewLocalFilesystemDriver(cfg.Local.Directory)
	case "s3":
		return NewS3StorageDriver()
	default:
		return nil, fmt.Errorf("unknown storage provider: %s", cfg.Provider)
	}
}

package storage

import (
	"fmt"

	"github.com/streamweaverio/broker/internal/config"
)

type StorageProviderDriver interface{}

func NewStorageProviderDriver(name string, cfg *config.StorageConfig) (StorageProviderDriver, error) {
	switch name {
	case "local":
		return NewLocalFilesystemDriver(cfg.Local.Directory)
	case "s3":
		return NewS3StorageDriver()
	default:
		return nil, fmt.Errorf("unknown storage provider: %s", name)
	}
}

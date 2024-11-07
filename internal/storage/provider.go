package storage

import "fmt"

type StorageProviderDriver interface{}

func NewStorageProviderDriver(name string) (StorageProviderDriver, error) {
	switch name {
	case "local":
		return NewLocalFilesystemDriver()
	case "s3":
		return NewS3StorageDriver()
	default:
		return nil, fmt.Errorf("unknown storage provider: %s", name)
	}
}

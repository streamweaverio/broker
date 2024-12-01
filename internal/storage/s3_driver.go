package storage

import "github.com/redis/go-redis/v9"

type S3StorageDriver struct{}

func NewS3StorageDriver() (*S3StorageDriver, error) {
	return &S3StorageDriver{}, nil
}

func (s *S3StorageDriver) Write(streamName string, messages []redis.XMessage, meta *ArchiveMetadata) error {
	return nil
}

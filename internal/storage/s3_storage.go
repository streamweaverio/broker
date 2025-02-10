package storage

import (
	"context"

	"github.com/streamweaverio/broker/internal/block"
	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/s3"
)

type S3Storage struct {
	Client s3.Client
	Bucket string
	Logger logging.LoggerContract
}

type S3StorageOptions struct {
	Client     s3.Client
	BucketName string
}

func NewS3Storage(opts *S3StorageOptions, logger logging.LoggerContract) (Storage, error) {
	return &S3Storage{
		Client: opts.Client,
		Bucket: opts.BucketName,
		Logger: logger,
	}, nil
}

func (s *S3Storage) ArchiveBlock(ctx context.Context, block *block.Block) error {
	return nil
}

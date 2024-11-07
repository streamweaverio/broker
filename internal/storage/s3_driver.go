package storage

type S3StorageDriver struct{}

func NewS3StorageDriver() (*S3StorageDriver, error) {
	return &S3StorageDriver{}, nil
}

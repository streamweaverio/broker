package storage

type LocalFilesystemDriver struct{}

func NewLocalFilesystemDriver() (*LocalFilesystemDriver, error) {
	return &LocalFilesystemDriver{}, nil
}

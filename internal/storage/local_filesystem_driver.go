package storage

import (
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

type LocalFSDriver struct {
	Directory string
}

func InitDirectory(dir string) error {
	// Check if the directory exists, if not create it
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return fmt.Errorf("error creating directory: %v", err)
		}
	}

	return nil
}

func NewLocalFilesystemDriver(directory string) (*LocalFSDriver, error) {
	err := InitDirectory(directory)
	if err != nil {
		return nil, err
	}

	return &LocalFSDriver{
		Directory: directory,
	}, nil
}

func (l *LocalFSDriver) Write(streamName string, messages []redis.XMessage, meta *ArchiveMetadata) error {
	return nil
}

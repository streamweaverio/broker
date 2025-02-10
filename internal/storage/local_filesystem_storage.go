package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/streamweaverio/broker/internal/block"
)

type LocalFilesystemStorage struct {
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

func NewLocalFilesystemDriver(directory string) (Storage, error) {
	err := InitDirectory(directory)
	if err != nil {
		return nil, err
	}

	return &LocalFilesystemStorage{
		Directory: directory,
	}, nil
}

func (s *LocalFilesystemStorage) ArchiveBlock(ctx context.Context, block *block.Block) error {
	return nil
}

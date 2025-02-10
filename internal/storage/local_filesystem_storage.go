package storage

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
	// Create stream directory if it doesn't exist
	streamDir := filepath.Join(s.Directory, block.StreamName)
	if err := InitDirectory(streamDir); err != nil {
		return fmt.Errorf("failed to create stream directory: %v", err)
	}

	// Create block directory
	blockDir := filepath.Join(streamDir, block.BlockID)
	if err := InitDirectory(blockDir); err != nil {
		return fmt.Errorf("failed to create block directory: %v", err)
	}

	// Define paths for block components
	parquetPath := filepath.Join(blockDir, "data.parquet")
	bloomPath := filepath.Join(blockDir, "filter.bloom")
	metaPath := filepath.Join(blockDir, "meta.json")

	// Use a channel to collect errors from goroutines
	errChan := make(chan error, 3)

	// Context with cancellation for cleanup in case of errors
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Write block components concurrently
	go func() {
		if err := writeFile(ctx, parquetPath, block.Parquet); err != nil {
			errChan <- fmt.Errorf("failed to write parquet file: %v", err)
			cancel()
			return
		}
		errChan <- nil
	}()

	go func() {
		if err := writeFile(ctx, bloomPath, block.Bloom); err != nil {
			errChan <- fmt.Errorf("failed to write bloom filter: %v", err)
			cancel()
			return
		}
		errChan <- nil
	}()

	go func() {
		if err := os.WriteFile(metaPath, block.Meta, 0644); err != nil {
			errChan <- fmt.Errorf("failed to write metadata: %v", err)
			cancel()
			return
		}
		errChan <- nil
	}()

	// Collect results from all goroutines
	for i := 0; i < 3; i++ {
		if err := <-errChan; err != nil {
			// Clean up the block directory on error
			os.RemoveAll(blockDir)
			return err
		}
	}

	return nil
}

// writeFile handles writing a ReadCloser to a file with proper cleanup
func writeFile(ctx context.Context, path string, reader io.ReadCloser) error {
	if reader == nil {
		return fmt.Errorf("nil reader provided")
	}
	defer reader.Close()

	// Create the file
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a buffered writer for better performance
	writer := bufio.NewWriter(file)

	// Copy data with context cancellation support
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(writer, reader)
		if err != nil {
			done <- err
			return
		}
		done <- writer.Flush()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

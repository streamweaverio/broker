package archiver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/bits-and-blooms/bloom"
	rdb "github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/block"
	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/storage"
	"github.com/streamweaverio/broker/pkg/utils"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
)

type Archiver interface {
	Archive(ctx context.Context, streamName string, messages []rdb.XMessage) error
}

type ArchiverOptions struct {
	Storage storage.Storage
}

type ArchiverImpl struct {
	Storage storage.Storage
	Logger  logging.LoggerContract
}

// Create a new Archiver instance
func New(opts *ArchiverOptions, logger logging.LoggerContract) Archiver {
	return &ArchiverImpl{
		Storage: opts.Storage,
		Logger:  logger,
	}
}

func (a *ArchiverImpl) Archive(ctx context.Context, streamName string, messages []rdb.XMessage) error {
	if len(messages) == 0 {
		a.Logger.Warn("No messages to archive", zap.String("stream", streamName))
		return nil
	}

	blockStartTimestamp, err := utils.GetTimestampFromStreamMessageID(messages[0].ID)
	if err != nil {
		return fmt.Errorf("failed to parse block start timestamp: %w", err)
	}

	blockEndTimestamp, err := utils.GetTimestampFromStreamMessageID(messages[len(messages)-1].ID)
	if err != nil {
		return fmt.Errorf("failed to parse block end timestamp: %w", err)
	}

	// Generate block ID
	blockStart := messages[0].ID
	blockEnd := messages[len(messages)-1].ID
	blockID := GenerateBlockID(blockEnd, blockStart)

	meta := &block.BlockMetadata{
		StreamName:          streamName,
		BlockID:             blockID,
		BlockStartTimestamp: blockStartTimestamp,
		BlockEndTimestamp:   blockEndTimestamp,
		BlockStartId:        blockStart,
		BlockEndId:          blockEnd,
		MessageCount:        len(messages),
	}

	// Serialize messages to Parquet
	parquetData, err := a.SerializeToParquet(messages, meta)
	if err != nil {
		return fmt.Errorf("failed to serialize to Parquet: %w", err)
	}
	defer parquetData.Close() // Important to close the reader

	// Create Bloom filter
	bloomData, err := a.CreateBloomFilter(messages, meta)
	if err != nil {
		return fmt.Errorf("failed to create Bloom filter: %w", err)
	}
	defer bloomData.Close() // Important to close the reader

	metadata, err := a.CreateMetadata(meta)
	if err != nil {
		return fmt.Errorf("failed to create metadata: %w", err)
	}

	// 5. Archive block using StorageManager
	err = a.Storage.ArchiveBlock(ctx, &block.Block{
		StreamName: streamName,
		BlockID:    blockID,
		Parquet:    parquetData,
		Bloom:      bloomData,
		Meta:       metadata,
	})

	if err != nil {
		return fmt.Errorf("failed to archive block: %w", err)
	}

	a.Logger.Info("Archived block", zap.String("stream", streamName), zap.String("block_id", blockID), zap.Int("message_count", len(messages)))

	return nil
}

func (a *ArchiverImpl) SerializeToParquet(messages []rdb.XMessage, meta *block.BlockMetadata) (io.ReadCloser, error) {
	r, w := io.Pipe()

	go func() {
		defer w.Close()

		buf := &bytes.Buffer{}
		fw := NewBufferFile(buf, 0)

		pw, err := writer.NewParquetWriter(fw, new(block.BlockParquet), 4)
		if err != nil {
			a.Logger.Error("Failed to create Parquet writer", zap.Error(err))
			w.CloseWithError(fmt.Errorf("failed to create Parquet writer: %w", err))
			return
		}

		pw.CompressionType = parquet.CompressionCodec_SNAPPY

		for _, msg := range messages {
			record := &block.BlockParquet{
				MessageID: msg.ID,
				Data:      utils.SerializeStreamMessageValues(msg.Values),
			}

			if err = pw.Write(record); err != nil {
				pw.WriteStop()
				a.Logger.Error("Failed to write record to Parquet", zap.Error(err))
				w.CloseWithError(fmt.Errorf("failed to write record to Parquet: %w", err))
				return
			}
		}

		if err = pw.WriteStop(); err != nil {
			a.Logger.Error("Failed to close Parquet writer", zap.Error(err))
			w.CloseWithError(fmt.Errorf("failed to write Parquet file: %w", err))
			return
		}

		meta.ParquetFileSize = buf.Len()
		meta.ParquetFooter = pw.Footer
	}()

	return r, nil
}

// CreateBloomFilter generates a bloom filter containing all message IDs in the block
func (a *ArchiverImpl) CreateBloomFilter(messages []rdb.XMessage, meta *block.BlockMetadata) (io.ReadCloser, error) {
	// Create a new bloom filter with the specified size and false positive rate
	filter := bloom.NewWithEstimates(uint(len(messages)), 0.01)

	// Add all message IDs to the filter
	for _, msg := range messages {
		filter.Add([]byte(msg.ID))
	}

	// Create a buffer to store the serialized filter
	var buf bytes.Buffer

	// Serialize the bloom filter
	_, err := filter.WriteTo(&buf)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize bloom filter: %w", err)
	}

	// Create a ReadCloser from the buffer
	reader := io.NopCloser(bytes.NewReader(buf.Bytes()))

	// Update the metadata with the size of the bloom filter
	meta.BloomFilterSize = buf.Len()

	return reader, nil
}

func (a *ArchiverImpl) CreateMetadata(meta *block.BlockMetadata) ([]byte, error) {
	a.Logger.Info("Creating block metadata...", zap.Any("metadata", meta)) // Log the metadata for debugging

	jsonData, err := json.Marshal(meta)
	if err != nil {
		a.Logger.Error("Failed to marshal metadata to JSON", zap.Error(err))
		return nil, fmt.Errorf("failed to marshal metadata to JSON: %w", err)
	}

	return jsonData, nil
}

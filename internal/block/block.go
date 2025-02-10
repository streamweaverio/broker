package block

import (
	"io"

	"github.com/xitongsys/parquet-go/parquet"
)

type Block struct {
	StreamName string
	BlockID    string
	Parquet    io.ReadCloser
	Bloom      io.ReadCloser
	Meta       []byte
}

type BlockMetadata struct {
	// Name of the stream
	StreamName string `json:"stream_name"`
	// ID of the block
	BlockID string `json:"block_id"`
	// Start timestamp of the block
	BlockStartTimestamp int64 `json:"block_start"`
	// End timestamp of the block
	BlockEndTimestamp int64 `json:"block_end"`
	// ID of the first message in the block
	BlockStartId string `json:"block_start_id"`
	// ID of the last message in the block
	BlockEndId string `json:"block_end_id"`
	// Number of messages in the block
	MessageCount int `json:"message_count"`
	// Size of the block's bloom filter
	BloomFilterSize int `json:"bloom_filter_size"`
	// Size of the block's parquet file
	ParquetFileSize int `json:"parquet_file_size"`
	// Parquet file footer
	ParquetFooter *parquet.FileMetaData `json:"parquet_footer"`
}

type BlockParquet struct {
	MessageID string `parquet:"name=message_id, type=BYTE_ARRAY, convertedType=UTF8, encoding=PLAIN_DICTIONARY"`
	Data      string `parquet:"name=data, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

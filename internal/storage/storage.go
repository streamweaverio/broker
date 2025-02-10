package storage

import (
	"context"

	"github.com/streamweaverio/broker/internal/block"
)

type Storage interface {
	ArchiveBlock(ctx context.Context, block *block.Block) error
}

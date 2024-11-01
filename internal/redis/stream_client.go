package redis

import (
	"context"

	rdb "github.com/redis/go-redis/v9"
)

type RedisStreamClient interface {
	XAdd(ctx context.Context, args *rdb.XAddArgs) *rdb.StringCmd
	XDel(ctx context.Context, stream string, ids ...string) *rdb.IntCmd
}

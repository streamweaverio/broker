package redis

import (
	"context"

	rdb "github.com/redis/go-redis/v9"
)

type RedisStreamClient interface {
	XAdd(ctx context.Context, args *rdb.XAddArgs) *rdb.StringCmd
	XDel(ctx context.Context, stream string, ids ...string) *rdb.IntCmd
	HSet(ctx context.Context, key string, values ...interface{}) *rdb.IntCmd
	HSetNX(ctx context.Context, key, field string, value interface{}) *rdb.BoolCmd
	HGetAll(ctx context.Context, key string) *rdb.MapStringStringCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *rdb.IntCmd
}

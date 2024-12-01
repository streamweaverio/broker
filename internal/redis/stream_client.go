package redis

import (
	"context"

	rdb "github.com/redis/go-redis/v9"
)

type RedisStreamClient interface {
	XAdd(ctx context.Context, args *rdb.XAddArgs) *rdb.StringCmd
	XDel(ctx context.Context, stream string, ids ...string) *rdb.IntCmd
	XInfoStream(ctx context.Context, stream string) *rdb.XInfoStreamCmd
	XTrimMinID(ctx context.Context, stream string, minID string) *rdb.IntCmd
	XRange(ctx context.Context, stream, start, stop string) *rdb.XMessageSliceCmd
	XRangeN(ctx context.Context, stream, start, stop string, count int64) *rdb.XMessageSliceCmd
	HSet(ctx context.Context, key string, values ...interface{}) *rdb.IntCmd
	HSetNX(ctx context.Context, key, field string, value interface{}) *rdb.BoolCmd
	HGetAll(ctx context.Context, key string) *rdb.MapStringStringCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *rdb.IntCmd
	SMembers(ctx context.Context, key string) *rdb.StringSliceCmd
}

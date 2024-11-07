package redis

import (
	"context"

	rdb "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/mock"
)

type MockRedisClient struct {
	mock.Mock
}

func (m *MockRedisClient) XAdd(ctx context.Context, params *rdb.XAddArgs) *rdb.StringCmd {
	args := m.Called(ctx, params)
	return args.Get(0).(*rdb.StringCmd)
}

func (m *MockRedisClient) XDel(ctx context.Context, stream string, ids ...string) *rdb.IntCmd {
	args := m.Called(ctx, stream, ids)
	return args.Get(0).(*rdb.IntCmd)
}

func (m *MockRedisClient) XTrimMinID(ctx context.Context, stream string, minID string) *rdb.IntCmd {
	args := m.Called(ctx, stream, minID)
	return args.Get(0).(*rdb.IntCmd)
}

func (m *MockRedisClient) HSet(ctx context.Context, key string, values ...interface{}) *rdb.IntCmd {
	args := m.Called(ctx, key, values)
	return args.Get(0).(*rdb.IntCmd)
}

func (m *MockRedisClient) HSetNX(ctx context.Context, key, field string, value interface{}) *rdb.BoolCmd {
	args := m.Called(ctx, key, field, value)
	return args.Get(0).(*rdb.BoolCmd)
}

func (m *MockRedisClient) HGetAll(ctx context.Context, key string) *rdb.MapStringStringCmd {
	args := m.Called(ctx, key)
	return args.Get(0).(*rdb.MapStringStringCmd)
}

func (m *MockRedisClient) SAdd(ctx context.Context, key string, members ...interface{}) *rdb.IntCmd {
	args := m.Called(ctx, key, members)
	return args.Get(0).(*rdb.IntCmd)
}

func (m *MockRedisClient) SMembers(key string) *rdb.StringSliceCmd {
	args := m.Called(key)
	return args.Get(0).(*rdb.StringSliceCmd)
}

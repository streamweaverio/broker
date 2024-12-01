package redis

import (
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/mock"
)

type RedisStreamServiceMock struct {
	mock.Mock
}

func NewRedisStreamServiceMock() *RedisStreamServiceMock {
	return &RedisStreamServiceMock{}
}

func (m *RedisStreamServiceMock) CreateStream(params *CreateStreamParameters) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *RedisStreamServiceMock) DeleteMessagesOlderThan(streamName string, minId string) error {
	args := m.Called(streamName, minId)
	return args.Error(0)
}

func (m *RedisStreamServiceMock) CountMessagesOlderThan(streamName string, minId string) (int64, error) {
	args := m.Called(streamName, minId)
	return args.Get(0).(int64), args.Error(1)
}

func (m *RedisStreamServiceMock) GetMessagesOlderThan(streamName string, minId string, count int64) ([]redis.XMessage, error) {
	args := m.Called(streamName, minId, count)
	return args.Get(0).([]redis.XMessage), args.Error(1)
}

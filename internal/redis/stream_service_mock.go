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

func (m *RedisStreamServiceMock) StreamExists(streamName string) (bool, error) {
	args := m.Called(streamName)
	return args.Bool(0), args.Error(1)
}

func (m *RedisStreamServiceMock) CreateStream(params *CreateStreamParameters) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *RedisStreamServiceMock) DeleteMessagesOlderThan(streamName string, minId string) error {
	args := m.Called(streamName, minId)
	return args.Error(0)
}

func (m *RedisStreamServiceMock) CountMessagesOlderThan(streamName string, minId string, batchSize int64) (int64, error) {
	args := m.Called(streamName, minId, batchSize)
	return args.Get(0).(int64), args.Error(1)
}

func (m *RedisStreamServiceMock) GetMessagesOlderThan(streamName string, minId string, count int64) ([]redis.XMessage, error) {
	args := m.Called(streamName, minId, count)
	return args.Get(0).([]redis.XMessage), args.Error(1)
}

func (m *RedisStreamServiceMock) PublishMessages(streamName string, messages [][]byte) (*StreamPublishResult, error) {
	args := m.Called(streamName, messages)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*StreamPublishResult), args.Error(1)
}

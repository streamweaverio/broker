package redis

import "github.com/stretchr/testify/mock"

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

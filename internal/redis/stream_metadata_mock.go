package redis

import (
	"github.com/stretchr/testify/mock"
)

type StreamMetadataServiceMock struct {
	mock.Mock
}

func NewStreamMetadataServiceMock() *StreamMetadataServiceMock {
	return &StreamMetadataServiceMock{}
}

func (m *StreamMetadataServiceMock) WriteStreamMetadata(value *StreamMetadata) error {
	args := m.Called(value)
	return args.Error(0)
}

func (m *StreamMetadataServiceMock) ReadStreamMetadata(streamName string) (*StreamMetadata, error) {
	args := m.Called(streamName)
	return args.Get(0).(*StreamMetadata), args.Error(1)
}

func (m *StreamMetadataServiceMock) AddToRegistry(streamName string) error {
	args := m.Called(streamName)
	return args.Error(0)
}

func (m *StreamMetadataServiceMock) AddToCleanupBucket(streamName string, bucketKey string) error {
	args := m.Called(streamName, bucketKey)
	return args.Error(0)
}

func (m *StreamMetadataServiceMock) ListStreams() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *StreamMetadataServiceMock) GetStreamMetadata(streamName string) (*StreamMetadata, error) {
	args := m.Called(streamName)
	return args.Get(0).(*StreamMetadata), args.Error(1)
}

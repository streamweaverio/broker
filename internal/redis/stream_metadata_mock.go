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

func (m *StreamMetadataServiceMock) AddToRetentionBucket(streamName string, retentionPolicy string) error {
	args := m.Called(streamName, retentionPolicy)
	return args.Error(0)
}

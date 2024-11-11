package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/testutils"
	"github.com/streamweaverio/broker/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func CreateTestSubject() (StreamMetadataService, *MockRedisClient) {
	client := &MockRedisClient{}
	logger := testutils.NewMockLogger()
	ctx := context.Background()

	metadataSvc := NewStreamMetadataService(ctx, client, logger)
	return metadataSvc, client
}

func MetadataKeyMatcher(streamName string) func(interface{}) bool {
	expectedKey := fmt.Sprintf("%s%s", STREAM_META_DATA_PREFIX, utils.HashString(streamName))
	return func(value interface{}) bool {
		key, ok := value.(string)
		return ok && key == expectedKey
	}
}

func TestStreamMetadataImpl_WriteStreamMetadata(t *testing.T) {
	t.Run("Updates metadata in Redis if it exists", func(t *testing.T) {
		svc, client := CreateTestSubject()
		streamName := "test-stream"
		streamMetadata := &StreamMetadata{
			Name:            streamName,
			RetentionPolicy: "time",
			MaxAge:          "2h",
			CreatedAt:       1620000000,
		}

		// Setup: Metadata exists in Redis
		existingMetadata := map[string]string{
			"name":             streamName,
			"retention_policy": "time",
			"max_age":          "1h",
			"created_at":       "1620000000",
		}

		// Mock the HGetAll call to simulate existing metadata retrieval
		client.On("HGetAll", mock.Anything, mock.MatchedBy(MetadataKeyMatcher(streamName))).Return(redis.NewMapStringStringResult(existingMetadata, nil))

		// Expect HSet to update the metadata
		client.
			On("HSet", mock.Anything, mock.MatchedBy(MetadataKeyMatcher(streamName)), mock.MatchedBy(func(value []interface{}) bool {
				val, ok := value[0].(map[string]string)
				return ok && val["name"] == streamMetadata.Name &&
					val["retention_policy"] == streamMetadata.RetentionPolicy &&
					val["max_age"] == streamMetadata.MaxAge
			})).
			Return(redis.NewIntResult(1, nil))

		// Run test
		err := svc.WriteStreamMetadata(streamMetadata)
		assert.NoError(t, err)

		// Verify mock expectations
		client.AssertExpectations(t)
	})
}

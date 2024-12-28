package redis

import (
	"context"
	"testing"

	rdb "github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/config"
	"github.com/streamweaverio/broker/internal/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Helper function for setting up the service and mock client
func setupRedisStreamService() (RedisStreamService, *MockRedisClient, *StreamMetadataServiceMock) {
	client := &MockRedisClient{}
	logger := testutils.NewMockLogger()
	metadataService := NewStreamMetadataServiceMock()
	retentionOptions := &config.RetentionConfig{
		MaxAge: 3600000,
	}

	service := NewRedisStreamService(&RedisStreamServiceOptions{
		Ctx:                    context.Background(),
		MetadataService:        metadataService,
		RedisClient:            client,
		GlobalRetentionOptions: retentionOptions,
	}, logger)

	return service, client, metadataService
}

func TestRedisStreamService_CreateStream(t *testing.T) {
	t.Run("CreateStream with valid parameters and size retention policy", func(t *testing.T) {
		service, client, metadataService := setupRedisStreamService()
		params := &CreateStreamParameters{
			Name:          "test-stream",
			MaxAge:        3600000,
			CleanupPolicy: "delete",
		}

		metadataService.On("AddToRegistry", params.Name).Return(nil)
		metadataService.On("WriteStreamMetadata", mock.MatchedBy(func(value interface{}) bool {
			streamMetadata, ok := value.(*StreamMetadata)
			return ok &&
				streamMetadata.Name == params.Name &&
				streamMetadata.MaxAge == params.MaxAge
		})).Return(nil)
		metadataService.On("AddToCleanupBucket", params.Name, STREAM_CLEANUP_BUCKET_DELETE).Return(nil)
		client.On("XAdd", mock.Anything, mock.MatchedBy(func(args *rdb.XAddArgs) bool {
			return args.Stream == params.Name
		})).Return(&rdb.StringCmd{})

		client.On("XDel", mock.Anything, "test-stream", mock.Anything).Return(&rdb.IntCmd{})

		err := service.CreateStream(params)
		assert.NoError(t, err)

		client.AssertExpectations(t)
	})
}

func TestRedisStreamService_PublishMessages(t *testing.T) {
	t.Run("Publish multiple messages successfully", func(t *testing.T) {
		service, client, _ := setupRedisStreamService()
		streamName := "test-stream"
		messages := [][]byte{
			[]byte("event_name=login"),
			[]byte("event_name=logout"),
			[]byte("event_name=click"),
		}

		// Set up mock for XInfoStream to indicate stream exists
		infoCmd := &rdb.XInfoStreamCmd{}
		infoCmd.SetVal(&rdb.XInfoStream{}) // This sets a successful result
		client.On("XInfoStream", mock.Anything, streamName).Return(infoCmd)

		// Set up mock for XAdd with successful message IDs
		expectedIds := []string{"1-0", "2-0", "3-0"}
		for i := range messages {
			cmdVal := &rdb.StringCmd{}
			cmdVal.SetVal(expectedIds[i])
			client.On("XAdd", mock.Anything, mock.MatchedBy(func(args *rdb.XAddArgs) bool {
				values, ok := args.Values.(map[string]interface{})
				return args.Stream == streamName && ok && len(values) > 0
			})).Return(cmdVal).Once()
		}

		result, err := service.PublishMessages(streamName, messages)

		assert.NoError(t, err)
		assert.Equal(t, len(messages), result.Published)
		assert.Equal(t, expectedIds, result.MessageIds)
		client.AssertExpectations(t)
	})

	t.Run("Return an error if stream does not exist", func(t *testing.T) {
		service, client, _ := setupRedisStreamService()
		streamName := "test-stream"
		messages := [][]byte{
			[]byte("event_name=login"),
			[]byte("event_name=logout"),
			[]byte("event_name=click"),
		}

		// Set up mock for XInfoStream to indicate stream does not exist
		infoCmd := &rdb.XInfoStreamCmd{}
		infoCmd.SetErr(rdb.Nil)
		client.On("XInfoStream", mock.Anything, streamName).Return(infoCmd)

		result, err := service.PublishMessages(streamName, messages)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.IsType(t, &RedisStreamNotFoundError{}, err)
		client.AssertExpectations(t)
	})
}

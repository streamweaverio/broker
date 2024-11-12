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
func setupRedisStreamService() (*RedisStreamService, *MockRedisClient, *StreamMetadataServiceMock) {
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

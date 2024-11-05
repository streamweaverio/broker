package redis

import (
	"context"
	"fmt"
	"testing"

	rdb "github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/config"
	"github.com/streamweaverio/broker/internal/testutils"
	"github.com/streamweaverio/broker/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Helper function for setting up the service and mock client
func setupRedisStreamService() (*RedisStreamService, *MockRedisClient) {
	client := &MockRedisClient{}
	logger := testutils.NewMockLogger()
	opts := &RedisStreamServiceOptions{
		GlobalRetentionOptions: &config.RetentionConfig{
			Policy:  "size",
			MaxSize: 10000,
			MaxAge:  "1h",
		},
	}
	service := NewRedisStreamService(context.Background(), client, logger, opts)
	return service, client
}

func MetadataKeyMatcher(streamName string) func(interface{}) bool {
	expectedKey := fmt.Sprintf("%s%s", STREAM_META_DATA_PREFIX, utils.HashString(streamName))
	return func(value interface{}) bool {
		key, ok := value.(string)
		return ok && key == expectedKey
	}
}

func TestRedisStreamService_CreateStream(t *testing.T) {
	t.Run("CreateStream with valid parameters and size retention policy", func(t *testing.T) {
		service, client := setupRedisStreamService()
		params := &CreateStreamParameters{
			Name:            "test-stream",
			MaxSize:         10000,
			RetentionPolicy: "size",
		}

		// Mock HGetAll to return nil, simulating no existing metadata
		client.On("HGetAll", mock.Anything, mock.MatchedBy(MetadataKeyMatcher(params.Name))).Return(rdb.NewMapStringStringResult(nil, nil))
		client.On("XAdd", mock.Anything, mock.MatchedBy(func(args *rdb.XAddArgs) bool {
			return args.Stream == params.Name
		})).Return(&rdb.StringCmd{})

		client.On("HSet", mock.Anything, mock.MatchedBy(MetadataKeyMatcher(params.Name)), mock.MatchedBy(func(values []interface{}) bool {
			if len(values) != 1 {
				return false
			}
			fields, ok := values[0].(map[string]interface{})
			return ok &&
				fields["name"] == "test-stream" &&
				fields["retention_policy"] == "size" &&
				fields["max_size"] == "10000" &&
				fields["created_at"] != ""
		})).Return(&rdb.IntCmd{})

		client.On("SAdd",
			mock.Anything,
			STREAM_RETENTION_POLICY_SIZE_BUCKET_KEY,
			[]interface{}{utils.HashString(params.Name)}).Return(&rdb.IntCmd{})

		client.On("XDel", mock.Anything, "test-stream", mock.Anything).Return(&rdb.IntCmd{})

		err := service.CreateStream(params)
		assert.NoError(t, err)

		client.AssertExpectations(t)
	})
}

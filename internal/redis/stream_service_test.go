package redis

import (
	"context"
	"testing"

	rdb "github.com/redis/go-redis/v9"
	"github.com/streamweaverio/broker/internal/testutils"
	"github.com/stretchr/testify/mock"
)

type CreateStreamTestCase struct {
	Title       string
	Params      *CreateStreamParameters
	ExpectError bool
}

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

func TestRedisStreamService_CreateStream(t *testing.T) {
	context := context.Background()
	client := &MockRedisClient{}
	logger := testutils.NewMockLogger()
	opts := &RedisStreamServiceOptions{}

	subject := NewRedisStreamService(context, client, logger, opts)

	client.On("XAdd", context, mock.Anything).Return(&rdb.StringCmd{})
	client.On("XDel", context, mock.Anything, mock.Anything, mock.Anything).Return(&rdb.IntCmd{})

	testCases := []CreateStreamTestCase{
		{
			Title: "Create stream with valid parameters",
			Params: &CreateStreamParameters{
				Name:            "test-stream",
				MaxSize:         10000,
				RetentionPolicy: "size",
			},
			ExpectError: false,
		},
		{
			Title: "Create stream without name",
			Params: &CreateStreamParameters{
				MaxSize:         10000,
				RetentionPolicy: "size",
			},
			ExpectError: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Title, func(t *testing.T) {
			err := subject.CreateStream(testCase.Params)
			if (err != nil) != testCase.ExpectError {
				t.Errorf("CreateStream() error = %v, expectedError %v", err, testCase.ExpectError)
			}
		})
	}
}

package broker

import (
	"context"
	"testing"

	"github.com/streamweaverio/broker/internal/redis"
	"github.com/streamweaverio/broker/internal/testutils"
	brokerpb "github.com/streamweaverio/go-protos/broker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCHandler_CreateStream(t *testing.T) {
	logger := testutils.NewMockLogger()
	svc := redis.NewRedisStreamServiceMock()
	handler := NewRPCHandler(svc, logger)

	ctx := context.Background()
	req := &brokerpb.CreateStreamRequest{
		StreamName:      "test-stream",
		RetentionTimeMs: 3600000,
	}

	// Set up the mock to expect the call
	svc.On("CreateStream", mock.MatchedBy(func(p *redis.CreateStreamParameters) bool {
		return p.Name == req.StreamName && p.MaxAge == req.RetentionTimeMs
	})).Return(nil)

	// Call the CreateStream method
	resp, err := handler.CreateStream(ctx, req)

	// Assertions
	assert.NoError(t, err, "CreateStream should not return an error")
	assert.Equal(t, "OK", resp.Status, "CreateStream should return a status of OK")

	// Verify that CreateStream was called with the correct parameters by checking the printed output
	svc.AssertExpectations(t)
}

func TestRPCHandler_Publish(t *testing.T) {
	logger := testutils.NewMockLogger()
	svc := redis.NewRedisStreamServiceMock()
	handler := NewRPCHandler(svc, logger)
	streamName := "test-stream"

	ctx := context.Background()

	t.Run("Publish messages successfully", func(t *testing.T) {
		req := &brokerpb.PublishRequest{
			StreamName: streamName,
			Messages: []*brokerpb.StreamMessage{
				{MessageContent: []byte("event_name=login")},
				{MessageContent: []byte("event_name=logout")},
				{MessageContent: []byte("event_name=click")},
			},
		}

		result := &redis.StreamPublishResult{
			MessageIds: []string{"1", "2", "3"},
			Published:  3,
			Failed:     0,
			Errors:     nil,
		}
		svc.On("PublishMessages", streamName, mock.MatchedBy(func(value [][]byte) bool {
			return len(req.Messages) == len(value)
		})).Return(result, nil).Once()

		resp, err := handler.Publish(ctx, req)

		assert.NoError(t, err)
		assert.Equal(t, "OK", resp.Status)
		assert.Equal(t, result.MessageIds, resp.MessageIds)
		svc.AssertExpectations(t)
	})

	t.Run("Return error when stream not found", func(t *testing.T) {
		req := &brokerpb.PublishRequest{
			StreamName: streamName,
			Messages: []*brokerpb.StreamMessage{
				{MessageContent: []byte("event_name=login")},
				{MessageContent: []byte("event_name=logout")},
				{MessageContent: []byte("event_name=click")},
			},
		}

		notFoundErr := &redis.RedisStreamNotFoundError{Name: streamName}

		svc.On("PublishMessages", streamName, mock.MatchedBy(func(value [][]byte) bool {
			return len(req.Messages) == len(value)
		})).Return(nil, notFoundErr).Once()

		resp, err := handler.Publish(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, err, status.Error(codes.NotFound, notFoundErr.Error()))
		svc.AssertExpectations(t)
	})
}

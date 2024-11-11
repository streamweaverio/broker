package broker

import (
	"context"
	"testing"

	"github.com/streamweaverio/broker/internal/redis"
	"github.com/streamweaverio/broker/internal/testutils"
	brokerpb "github.com/streamweaverio/go-protos/broker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockRedisStreamService struct {
	mock.Mock
}

func (s *MockRedisStreamService) CreateStream(params *redis.CreateStreamParameters) error {
	// fmt.Printf("CreateStream called with params: %+v\n", params)
	args := s.Called(params)
	return args.Error(0)
}

func TestRPCHandler_CreateStream(t *testing.T) {
	logger := testutils.NewMockLogger()
	svc := &MockRedisStreamService{}
	handler := NewRPCHandler(svc, logger)

	ctx := context.Background()
	req := &brokerpb.CreateStreamRequest{
		StreamName: "test-stream",
		RetentionOptions: &brokerpb.StreamRetentionOptions{
			RetentionPolicy: brokerpb.StreamRetentionPolicy_SIZE_RETENTION_POLICY,
			MaxSize:         10000,
			MaxAge:          "1d",
		},
	}

	// Set up the mock to expect the call
	svc.On("CreateStream", mock.MatchedBy(func(p *redis.CreateStreamParameters) bool {
		return p.Name == req.StreamName &&
			p.MaxAge == req.RetentionOptions.MaxAge
	})).Return(nil)

	// Call the CreateStream method
	resp, err := handler.CreateStream(ctx, req)

	// Assertions
	assert.NoError(t, err, "CreateStream should not return an error")
	assert.Equal(t, "OK", resp.Status, "CreateStream should return a status of OK")

	// Verify that CreateStream was called with the correct parameters by checking the printed output
	svc.AssertExpectations(t)
}

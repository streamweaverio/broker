package broker

import (
	"context"
	"testing"

	"github.com/streamweaverio/broker/internal/testutils"
	brokerpb "github.com/streamweaverio/go-protos/broker"
	"github.com/stretchr/testify/assert"
)

func TestRPCHandler_CreateStream(t *testing.T) {
	logger := testutils.NewMockLogger()
	// Create a new RPCHandler
	handler := NewRPCHandler(logger)

	ctx := context.Background()
	req := &brokerpb.CreateStreamRequest{
		StreamName: "test-stream",
	}

	// Call the CreateStream method
	resp, err := handler.CreateStream(ctx, req)

	assert.NoError(t, err, "CreateStream should not return an error")
	assert.Equal(t, "OK", resp.Status, "CreateStream should return a status of OK")
}

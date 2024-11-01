package broker

import (
	"context"

	"github.com/streamweaverio/broker/internal/logging"
	brokerpb "github.com/streamweaverio/go-protos/broker"
	"go.uber.org/zap"
)

type RPCHandler struct {
	logger logging.LoggerContract
	brokerpb.UnimplementedStreamWeaverBrokerServer
}

func NewRPCHandler(logger logging.LoggerContract) *RPCHandler {
	return &RPCHandler{logger: logger}
}

// Creates a new stream
func (h *RPCHandler) CreateStream(ctx context.Context, req *brokerpb.CreateStreamRequest) (*brokerpb.CreateStreamResponse, error) {
	h.logger.Info("CreateStream", zap.String("stream_name", req.StreamName))
	// TODO: Implement stream creation logic here (create stream in Redis)
	return &brokerpb.CreateStreamResponse{Status: "OK"}, nil
}

package broker

import (
	"context"

	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/redis"
	brokerpb "github.com/streamweaverio/go-protos/broker"
)

type RPCHandler struct {
	Logger  logging.LoggerContract
	Service redis.RedisStreamServiceContract
	brokerpb.UnimplementedStreamWeaverBrokerServer
}

func NewRPCHandler(svc redis.RedisStreamServiceContract, logger logging.LoggerContract) *RPCHandler {
	return &RPCHandler{
		Logger:  logger,
		Service: svc,
	}
}

// Creates a new stream
func (h *RPCHandler) CreateStream(ctx context.Context, req *brokerpb.CreateStreamRequest) (*brokerpb.CreateStreamResponse, error) {
	err := h.Service.CreateStream(&redis.CreateStreamParameters{
		Name:   req.StreamName,
		MaxAge: req.RetentionOptions.MaxAge,
	})
	if err != nil {
		return &brokerpb.CreateStreamResponse{
			Status:       "ERROR",
			ErrorMessage: err.Error(),
		}, err
	}

	return &brokerpb.CreateStreamResponse{Status: "OK"}, nil
}

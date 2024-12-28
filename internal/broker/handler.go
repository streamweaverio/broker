package broker

import (
	"context"

	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/broker/internal/redis"
	brokerpb "github.com/streamweaverio/go-protos/broker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RPCHandler struct {
	Logger  logging.LoggerContract
	Service redis.RedisStreamService
	brokerpb.UnimplementedStreamWeaverBrokerServer
}

func NewRPCHandler(svc redis.RedisStreamService, logger logging.LoggerContract) *RPCHandler {
	return &RPCHandler{
		Logger:  logger,
		Service: svc,
	}
}

// Creates a new stream
func (h *RPCHandler) CreateStream(ctx context.Context, req *brokerpb.CreateStreamRequest) (*brokerpb.CreateStreamResponse, error) {
	err := h.Service.CreateStream(&redis.CreateStreamParameters{
		Name:   req.StreamName,
		MaxAge: req.RetentionTimeMs,
	})
	if err != nil {
		return &brokerpb.CreateStreamResponse{
			Status:       "ERROR",
			ErrorMessage: err.Error(),
		}, err
	}

	return &brokerpb.CreateStreamResponse{Status: "OK"}, nil
}

func (h *RPCHandler) Publish(ctx context.Context, req *brokerpb.PublishRequest) (*brokerpb.PublishResponse, error) {
	// Prepare messages
	messages := make([][]byte, len(req.Messages))
	for i, msg := range req.Messages {
		messages[i] = msg.MessageContent
	}

	// Publish messages
	result, err := h.Service.PublishMessages(req.StreamName, messages)
	if err != nil {
		switch err.(type) {
		case *redis.RedisStreamNotFoundError:
			return nil, status.Error(codes.NotFound, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	// All messages published successfully
	return &brokerpb.PublishResponse{
		Status:     "OK",
		MessageIds: result.MessageIds,
	}, nil
}

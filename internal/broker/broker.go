package broker

import (
	"context"
	"fmt"
	"net"

	brokerpb "github.com/streamweaverio/go-protos/broker"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Broker struct {
	ctx    context.Context
	config *Options
	logger *zap.Logger
	server *grpc.Server
	rpc    brokerpb.StreamWeaverBrokerServer
}

type Options struct {
	Ctx    context.Context
	Port   int
	Logger *zap.Logger
	RPC    brokerpb.StreamWeaverBrokerServer
	Server *grpc.Server
}

func New(opts *Options) *Broker {
	return &Broker{
		ctx:    opts.Ctx,
		config: opts,
		logger: opts.Logger,
		server: opts.Server,
		rpc:    opts.RPC,
	}
}

func (b *Broker) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", b.config.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	brokerpb.RegisterStreamWeaverBrokerServer(b.server, b.rpc)
	b.logger.Info("Broker listening on port", zap.Int("port", b.config.Port))
	return b.server.Serve(lis)
}

func (b *Broker) Stop() {
	b.logger.Info("Stopping broker")
	b.server.Stop()
}

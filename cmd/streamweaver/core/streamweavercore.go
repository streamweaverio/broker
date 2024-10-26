package core

import (
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type StreamWeaverCore struct {
	Redis  *redis.Client
	Logger *zap.Logger
}

type StreamWeaverCoreOptions struct {
	RedisClient *redis.Client
}

func New(opts *StreamWeaverCoreOptions, logger *zap.Logger) *StreamWeaverCore {
	return &StreamWeaverCore{
		Logger: logger,
		Redis:  opts.RedisClient,
	}
}

func (s *StreamWeaverCore) Start() {
	s.Logger.Info("Starting StreamWeaver core service...")
}

func (s *StreamWeaverCore) Stop() {}

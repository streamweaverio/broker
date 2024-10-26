package core

type StreamWeaverCore struct{}

func NewCoreService() *StreamWeaverCore {
	return &StreamWeaverCore{}
}

func (s *StreamWeaverCore) Start() {}

func (s *StreamWeaverCore) Stop() {}

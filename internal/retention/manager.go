package retention

import (
	"time"

	"github.com/streamweaverio/broker/internal/logging"
	"go.uber.org/zap"
)

type RetentionPolicy struct {
	// Name of the retention policy (e.g. size, time)
	Name string
	// Rule to enforce the retention policy
	Rule RetentionPolicyRule
}

type RetentionManager interface {
	RegisterPolicy(opts *RetentionPolicy) RetentionManager
	Start() error
	Stop()
}

type RetentionManagerOptions struct {
	// Time interval to run retention policies in seconds
	Interval int
}

type RetentionManagerConfig struct {
	// Time interval to run retention policies in seconds
	Interval int
}

type RetentionManagerImpl struct {
	Policies []*RetentionPolicy
	Config   *RetentionManagerConfig
	Logger   logging.LoggerContract
}

func NewRetentionManager(opts *RetentionManagerOptions, logger logging.LoggerContract) (RetentionManager, error) {
	return &RetentionManagerImpl{
		Config: &RetentionManagerConfig{
			Interval: opts.Interval,
		},
		Logger: logger,
	}, nil
}

func (r *RetentionManagerImpl) RegisterPolicy(opts *RetentionPolicy) RetentionManager {
	r.Logger.Info("Registering retention policy...")
	r.Policies = append(r.Policies, opts)
	r.Logger.Info("Registered retention policy", zap.String("name", opts.Name))
	return r
}

func (r *RetentionManagerImpl) Start() error {
	r.Logger.Info("Starting retention manager...")
	if len(r.Policies) == 0 {
		r.Logger.Warn("No retention policies registered")
	}

	ticker := time.NewTicker(time.Duration(r.Config.Interval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		r.Logger.Info("Running retention policies...")
		for _, policy := range r.Policies {
			if err := policy.Rule.Enforce(); err != nil {
				r.Logger.Error("Failed to enforce policy", zap.String("policy", policy.Name), zap.Error(err))
			}
		}
	}
	return nil
}

func (r *RetentionManagerImpl) Stop() {
	r.Logger.Info("Stopping retention manager...")
}

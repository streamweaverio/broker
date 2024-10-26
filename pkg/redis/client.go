package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/redis/go-redis/v9"
)

const DEFAULT_MAX_CONNECTION_ATTEMPTS = 10
const DEFAULT_MAX_CONNECTION_BACKOFF_ELAPSED_TIME = 60

type ClientOptions struct {
	// Context for the client, used for sigterm handling and timeouts
	Context context.Context
	// Redis host
	Host string
	// Redis port
	Port int
	// Redis password
	Password string
	// Redis database to use
	DB int
	// Maximum number of connection retries
	MaxConnectionRetries int
	// Maximum time to wait for a connection to be established in seconds
	MaxConnectionBackoff int
}

// Create a new Redis client connection
func NewClient(opts *ClientOptions) (*redis.Client, error) {
	if opts.MaxConnectionRetries == 0 {
		opts.MaxConnectionRetries = DEFAULT_MAX_CONNECTION_ATTEMPTS
	}

	if opts.MaxConnectionBackoff == 0 {
		opts.MaxConnectionBackoff = DEFAULT_MAX_CONNECTION_BACKOFF_ELAPSED_TIME
	}

	var lastError error = nil

	connectionAttempts := 0
	connectionBackoff := backoff.NewExponentialBackOff()
	connectionBackoff.MaxElapsedTime = time.Duration(opts.MaxConnectionBackoff) * time.Second

	redisOpts := &redis.Options{
		Addr: fmt.Sprintf("%s:%d", opts.Host, opts.Port),
		DB:   opts.DB,
	}

	if opts.Password != "" {
		redisOpts.Password = opts.Password
	}

	client := redis.NewClient(redisOpts)

	for {
		ping, err := client.Ping(opts.Context).Result()
		if err == nil && ping == "PONG" {
			// connection established
			connectionBackoff.Reset()
			break
		}

		connectionAttempts++
		lastError = err
		// nextRetryTime := time.Now().Add(connectionBackoff.NextBackOff())
		if connectionAttempts >= opts.MaxConnectionRetries {
			lastError = fmt.Errorf("failed to connect to Redis. Max connection attempts reached: %v", opts.MaxConnectionRetries)
			break
		}

		time.Sleep(connectionBackoff.NextBackOff())
	}

	if lastError != nil {
		return nil, lastError
	}

	return client, nil
}

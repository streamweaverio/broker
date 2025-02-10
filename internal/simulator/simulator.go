package simulator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/streamweaverio/broker/internal/logging"
	"github.com/streamweaverio/go-protos/broker"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientSimulator struct {
	BrokerUrl    string
	Frequency    int
	Logger       logging.LoggerContract
	BrokerClient broker.StreamWeaverBrokerClient
	BrokerConn   *grpc.ClientConn
	StreamName   string
	StreamMaxAge int64
}

type SimulatorClientOptions struct {
	BrokerUrl string
	Frequency int
}

func NewClient(opts *SimulatorClientOptions, logger logging.LoggerContract) (*ClientSimulator, error) {
	conn, err := grpc.Dial(opts.BrokerUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %w", err)
	}

	brokerClient := broker.NewStreamWeaverBrokerClient(conn)

	logger.Info("Connected to broker", zap.String("url", opts.BrokerUrl))

	return &ClientSimulator{
		BrokerUrl:    opts.BrokerUrl,
		Frequency:    opts.Frequency,
		Logger:       logger,
		BrokerClient: brokerClient,
		BrokerConn:   conn,
	}, nil
}

func ProduceMessages(stream string, client broker.StreamWeaverBrokerClient, logger logging.LoggerContract) {
	logger.Info("Producing message...")
	limit := 1000
	messages := make([]*broker.StreamMessage, limit)

	for i := 0; i < limit; i++ {
		id := uuid.New().String()
		// Random value between 1 and 4000
		total := 1 + rand.Intn(4000)
		timestamp := time.Now().UnixMilli()
		messageContent := fmt.Sprintf("id=\"%s\" order_total=%d created_at=%d", id, total, timestamp)
		messages[i] = &broker.StreamMessage{
			MessageContent: []byte(messageContent),
		}
	}

	_, err := client.Publish(context.Background(), &broker.PublishRequest{
		StreamName: stream,
		Messages:   messages,
	})

	if err != nil {
		logger.Error("Failed to publish message", zap.Error(err))
	}
}

func (c *ClientSimulator) Start() error {
	c.Logger.Info("Starting client simulator...")

	ticker := time.NewTicker(time.Duration(c.Frequency) * time.Second)
	defer ticker.Stop()
	defer c.BrokerConn.Close()

	for range ticker.C {
		ProduceMessages(c.StreamName, c.BrokerClient, c.Logger)
	}

	return nil
}

func (c *ClientSimulator) CreateStream(ctx context.Context, name string, maxAge int64) error {
	c.Logger.Info(fmt.Sprintf("Creating stream %s with max age %d ...", name, maxAge))

	res, err := c.BrokerClient.CreateStream(ctx, &broker.CreateStreamRequest{
		StreamName:      name,
		RetentionTimeMs: maxAge,
	})
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	if res.Status != "OK" {
		return fmt.Errorf("failed to create stream: %s", res.ErrorMessage)
	}

	c.StreamName = name
	c.StreamMaxAge = maxAge

	return nil
}

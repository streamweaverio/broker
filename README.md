# StreamWeaver Broker

**StreamWeaver Broker** is a distributed message broker built on top of **Redis Streams** and written in **Go**. It provides real-time message streaming with support for flexible retention and archival mechanisms, allowing messages to be stored in cost-effective object storage solutions like **S3**, block storage, or local disk for long-term retention. StreamWeaver is designed to handle high-throughput message workloads, making it an ideal choice for modern event-driven architectures.

---

## Key Features

- **Real-Time Message Streaming**: Powered by Redis Streams for low-latency, high-throughput messaging.
- **Flexible Retention & Archival**: Configure retention policies to offload older messages to S3, block storage, or local disk.
- **Replay Support**: Fetch historical messages from long-term storage and replay them into Redis Streams for processing.
- **Consumer Group Support**: Distribute message processing across multiple consumers using Redis Streams' native consumer group functionality.
- **Multi-Language Client Libraries**: Seamless integration with multiple environments via client libraries in Go, Python, Node.js, Java, and C#.
- **Monitoring & Scalability**: Expose metrics for tracking message throughput, stream health, and storage utilization. StreamWeaver can scale horizontally to meet increasing demand.

---

## Table of Contents

1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Usage](#usage)
    - [Running the Broker](#running-the-broker)
    - [Producers](#producers)
    - [Consumers](#consumers)
    - [Retention & Replay](#retention--replay)
4. [Client Libraries](#client-libraries)
5. [Monitoring](#monitoring)
6. [Contributing](#contributing)
7. [License](#license)

---

## Installation

StreamWeaver Broker can be installed from source or using pre-built Docker images.

### From Source

1. Clone the repository:
   ```bash
   git clone https://github.com/firstshift/streamweaver-broker.git
   cd streamweaver-broker
   ```

2. Build the binary:
   ```bash
   go build -o streamweaver-broker
   ```

3. Run the broker:
   ```bash
   ./streamweaver-broker --config=config.yml
   ```

### Docker

1. Pull the Docker image:
   ```bash
   docker pull firstshift/streamweaver-broker:latest
   ```

2. Run the broker using Docker:
   ```bash
   docker run -d --name streamweaver-broker \
      -v /path/to/config.yml:/app/config.yml \
      firstshift/streamweaver-broker:latest
   ```

---

## Configuration

The broker is configured using a YAML file (`config.yml`). Here’s an example configuration:

```yaml
redis:
  host: "localhost"
  port: 6379
  password: ""
  stream_max_length: 1000000  # Maximum number of messages in the Redis Stream before pruning

storage:
  type: "s3"  # Options: s3, block, local
  s3:
    bucket: "streamweaver-archive"
    region: "us-east-1"
    access_key: "your-access-key"
    secret_key: "your-secret-key"
  local:
    path: "/var/streamweaver/storage"

retention:
  policy: "time"  # Options: time, size
  max_age: "7d"   # Messages older than 7 days will be moved to archival storage
  max_size: "5GB" # If using size-based policy, streams will be pruned when they exceed this size

replay:
  enabled: true
  max_replay_batch_size: 1000  # Max number of messages replayed at a time

monitoring:
  enabled: true
  port: 8080
```

---

## Usage

### Running the Broker

Once the broker is configured, you can start it by running:

```bash
./streamweaver-broker --config=config.yml
```

This will start the broker, connecting to the Redis instance and beginning to process messages according to the defined retention policies.

### Producers

Producers are clients that publish messages to a Redis Stream. Below is an example of producing messages using the Go client library.

```go
package main

import (
    "streamweaver"
    "fmt"
)

func main() {
    client, err := streamweaver.NewClient("localhost:6379", "stream-name")
    if err != nil {
        panic(err)
    }

    err = client.ProduceMessage("stream-name", map[string]interface{}{
        "message": "Hello, StreamWeaver!",
    })
    if err != nil {
        fmt.Println("Error producing message:", err)
    }
}
```

### Consumers

Consumers subscribe to Redis Streams to consume messages. Here’s an example of consuming messages in Python:


### Retention & Replay

The broker automatically manages message retention based on your configuration, moving older messages to long-term storage. Consumers can request to replay messages from archival storage.

To replay messages, use the following command from the client:

---

## Client Libraries

StreamWeaver provides official client libraries in the following languages:

- [Go](https://github.com/firstshift/streamweaver-go-client)
- [Node.js](https://github.com/firstshift/streamweaver-nodejs-client)

For details on how to produce and consume messages using these libraries, refer to their respective documentation.

---

## Monitoring

StreamWeaver exposes metrics for monitoring via HTTP on the configured port (default: 8080). Metrics include:

- Stream message throughput
- Retention and archival status
- Consumer group activity
- Storage usage

To enable monitoring, set `monitoring.enabled` to `true` in the configuration file.

Example Prometheus-compatible metrics endpoint:

```
http://localhost:8080/metrics
```

---

## Contributing

We welcome contributions to StreamWeaver! If you'd like to contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -am 'Add your feature'`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a Pull Request.

---

## License

StreamWeaver is licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for more information.

---

For more details and documentation, visit the official [StreamWeaver website](https://streamweaver.example.com).
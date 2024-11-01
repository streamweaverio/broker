# StreamWeaver

**StreamWeaver Broker** is a proof of concept message broker built on top of **Redis Streams** and written in **Go**.
It attemps provides real-time message streaming with support for flexible retention and archival mechanisms, allowing messages to be stored in cost-effective object storage solutions like **S3**, block storage, or local disk for long-term retention.

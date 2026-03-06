# Prerequisites

## Runtime
- Python 3.14+
- `uv` package manager (`pip install uv`)

## Per ecosystem

### Confluent Cloud
- CCloud API key + secret with billing read access
- (Optional) Metrics API key for usage-ratio allocation

### Self-managed Kafka
- Prometheus endpoint scraping Kafka JMX metrics
  (kafka_server_brokertopicmetrics_bytesin_total, etc.)
- (Optional) Kafka admin API access for resource discovery

### Generic metrics
- Prometheus endpoint with your custom metrics

## Storage
- SQLite (default, no setup needed) or PostgreSQL
- Write access to the configured database path

## Docker (optional)
- For containerized deployment

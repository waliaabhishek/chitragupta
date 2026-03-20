# Prerequisites

## Runtime

- **Docker Engine 24+** and **Docker Compose v2+** (recommended)
- For local development without Docker: Python 3.14+ and `uv` (`pip install uv`)

## Per ecosystem

### Confluent Cloud
- A **Service Account** with the following RBAC role bindings:
    - **MetricsViewer** — base permission for metrics access
    - **OrganizationAdmin** — required for Objects API, Metrics API, and viewing connector/ksqlDB principals for accurate chargeback
    - **BillingAdmin** — required to pull billing API data

    ```bash
    # Create the service account
    confluent login
    confluent iam sa create chargeback_handler \
      --description "Chargeback handler user"

    # Assign required role bindings (replace <sa_id> with the output from above)
    confluent iam rbac role-binding create --principal User:<sa_id> --role MetricsViewer
    confluent iam rbac role-binding create --principal User:<sa_id> --role OrganizationAdmin
    confluent iam rbac role-binding create --principal User:<sa_id> --role BillingAdmin
    ```

- A **Cloud API key + secret** created for that service account:

    ```bash
    confluent api-key create --resource cloud --service-account <sa_id>
    ```

- (Optional) Separate Metrics API key for usage-ratio allocation

!!! note
    OrganizationAdmin is broader than ideal. Confluent Cloud RBAC doesn't currently allow more granular scoping for the APIs this tool requires.

### Self-managed Kafka
- Prometheus endpoint scraping Kafka JMX metrics
  (kafka_server_brokertopicmetrics_bytesin_total, etc.)
- (Optional) Kafka admin API access for resource discovery

### Generic metrics
- Prometheus endpoint with your custom metrics

## Storage
- SQLite (default, no setup needed) or PostgreSQL
- Write access to the configured database path

## Local development (optional)
- Python 3.14+ and `uv` for running without Docker — see [Quickstart](quickstart.md#alternative-local-development-without-docker)

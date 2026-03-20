# Quickstart — Confluent Cloud

Get Chitragupt running against your Confluent Cloud organization in under 10 minutes.

## Prerequisites

- **Confluent CLI** installed ([install guide](https://docs.confluent.io/confluent-cli/current/install.html))
- **Python 3.14+** and **`uv`** (`pip install uv`)
- A Confluent Cloud organization

## 1. Create a Service Account

The chargeback engine needs a dedicated service account to access Confluent Cloud APIs.

```bash
confluent login
confluent iam sa create chargeback_handler \
  --description "Chargeback handler user"
```

Save the Service Account ID (`sa-*******`) from the output — you'll need it in the next steps.

## 2. Assign permissions

The service account needs three role bindings:

```bash
# Replace <sa_id> with your Service Account ID from above
confluent iam rbac role-binding create --principal User:<sa_id> --role MetricsViewer
confluent iam rbac role-binding create --principal User:<sa_id> --role OrganizationAdmin
confluent iam rbac role-binding create --principal User:<sa_id> --role BillingAdmin
```

| Role | Why it's needed |
|------|----------------|
| **MetricsViewer** | Base permission for metrics access |
| **OrganizationAdmin** | Objects API, Metrics API, and viewing connector/ksqlDB principals |
| **BillingAdmin** | Pull billing data from the Billing API |

!!! note
    OrganizationAdmin is broader than ideal. Confluent Cloud RBAC doesn't currently offer more granular scoping for the APIs this tool requires.

## 3. Create an API key

```bash
confluent api-key create --resource cloud --service-account <sa_id>
```

Save the **API Key** and **API Secret** from the output.

## 4. Install and set up

```bash
git clone <repo-url>
cd chitragupt
uv sync
```

## 5. Write a minimal config

Save as `config.yaml` in the project root:

```yaml
logging:
  level: INFO

tenants:
  my-org:
    ecosystem: confluent_cloud
    tenant_id: my-org              # internal partition key (not the CCloud org ID)
    storage:
      connection_string: "sqlite:///data/my-org.db"
    plugin_settings:
      ccloud_api:
        key: ${CCLOUD_API_KEY}
        secret: ${CCLOUD_API_SECRET}
      emitters:
        - type: csv
          aggregation: daily
          params:
            output_dir: ./output
```

## 6. Set environment variables

Use the API key and secret from step 3:

```bash
export CCLOUD_API_KEY=your-key
export CCLOUD_API_SECRET=your-secret
```

Alternatively, create a `.env` file in the same directory as `config.yaml`:

```
CCLOUD_API_KEY=your-key
CCLOUD_API_SECRET=your-secret
```

The config loader auto-discovers `.env` from the config file's directory. Variables already in the environment take precedence.

## 7. Run once

```bash
uv run python src/main.py --config-file config.yaml --run-once
```

## 8. Check output

```bash
ls output/
# my-org_2026-01-01.csv  my-org_2026-01-02.csv  ...
```

## Next steps

- [Docker deployment](https://github.com/waliaabhishek/chitragupt/blob/main/examples/) — self-contained examples for running the full stack with Grafana dashboards
- [Full configuration reference](../configuration/ccloud-reference.md) — all available settings
- [Run as a service](../operations/deployment.md) — continuous operation

# Quickstart

## 1. Install

```bash
pip install uv
git clone <repo-url>
cd chargeback-engine
uv sync
```

## 2. Write a minimal config

Confluent Cloud example (save as `config.yaml`):

```yaml
logging:
  level: INFO

tenants:
  my-org:
    ecosystem: confluent_cloud
    tenant_id: t-abc123
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

## 3. Set environment variables

```bash
export CCLOUD_API_KEY=your-key
export CCLOUD_API_SECRET=your-secret
```

## 4. Run once

```bash
uv run python src/main.py --config-file config.yaml --run-once
```

## 5. Check output

```bash
ls output/
# my-org_2026-01-01.csv  my-org_2026-01-02.csv  ...
```

## Next steps
- [Full configuration reference](../configuration/ccloud-reference.md)
- [Run as a service](../operations/deployment.md)

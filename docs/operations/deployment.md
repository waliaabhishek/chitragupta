# Deployment

## Run modes

| `--mode` | Use case |
|---|---|
| `worker` | Background pipeline only. API served separately. |
| `api` | REST API only. No pipeline. Query existing data. |
| `both` | Pipeline + API in one process. Simplest deployment. |

## Systemd unit (worker)

```ini
[Unit]
Description=Chitragupt Worker
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/chitragupt
ExecStart=uv run python src/main.py \
    --config-file /etc/chargeback/config.yaml \
    --mode worker
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

## Docker

```dockerfile
FROM python:3.14-slim
WORKDIR /app
COPY . .
RUN pip install uv && uv sync
CMD ["uv", "run", "python", "src/main.py", "--config-file", "/config/config.yaml", "--mode", "both"]
```

## Environment variables

Pass secrets via environment — never hardcode in YAML:

```bash
docker run -e CCLOUD_API_KEY=... -e CCLOUD_API_SECRET=... chitragupt
```

## API server

The REST API is a FastAPI application served by uvicorn.

```yaml
api:
  host: 0.0.0.0
  port: 8080
  enable_cors: true
  cors_origins:
    - "https://your-dashboard.example.com"
```

Health endpoint: `GET /health` — returns `{"status": "ok", "version": "..."}`

## Storage

Default is SQLite. For production with multiple instances or high volume:

```yaml
storage:
  backend: sqlmodel
  connection_string: "postgresql+psycopg2://user:pass@host/dbname"
```

Each tenant needs a separate database/schema.

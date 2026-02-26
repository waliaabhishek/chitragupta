# Chargeback Engine — Docker Deployment

## Prerequisites
- Docker and Docker Compose v2+
- API credentials for your ecosystem (CCloud or self-managed)

## Quick Start

1. Configure credentials:
   ```bash
   cd deployables
   cp config/.env.example config/.env
   # Edit config/.env with your credentials
   ```

2. Select ecosystem config:
   ```bash
   # For Confluent Cloud:
   cp config/config-ccloud.yaml config/config.yaml

   # For self-managed Kafka:
   cp config/config-self-managed.yaml config/config.yaml
   ```

3. Create data directory:
   ```bash
   mkdir -p data
   ```

4. Start services:
   ```bash
   docker compose up -d
   ```

5. Access:
   - API: http://localhost:8080
   - Grafana: http://localhost:3000 (admin/password)

## Services

| Service | Port | Description |
|---------|------|-------------|
| chargeback-engine | 8080 | API + pipeline worker |
| grafana | 3000 | Dashboards |

## Run Modes

Edit `docker-compose.yml` command to change mode:
- `--mode both` (default): API server + periodic pipeline
- `--mode api`: API server only
- `--mode worker`: Pipeline worker only
- Add `--run-once`: Single pipeline run then exit

## Configuration

### Volume Mounts
| Host Path | Container Path | Purpose |
|-----------|----------------|---------|
| `./config/` | `/app/config/` | Config files (read-only) |
| `./data/` | `/app/data/` | SQLite database (read-write) |

### Environment Variables
Set in `config/.env`. See `.env.example` for required variables per ecosystem.

## Building the Image

From repo root:
```bash
docker build -t chargeback-engine .
```

The build uses multi-stage caching for fast rebuilds.

## Troubleshooting

**Engine fails to start**
- Check credentials: `docker compose logs chargeback-engine`
- Verify config syntax: mount and run with `--run-once` to test

**Grafana shows "No data"**
- Ensure engine has run at least once (pipeline populates database)
- Check time range covers dates with data
- Verify datasource: Connections > Data Sources > Chargeback SQLite > Test

**Permission denied on data directory**
- Ensure `./data/` is writable by uid 1000: `chown 1000:1000 data` or `chmod 755 data`

**Port conflicts**
- Change ports in `docker-compose.yml` under `ports:`

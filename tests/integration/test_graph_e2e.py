"""Integration tests for TASK-220 graph search, diff, and timeline endpoints.

Exercises the full stack: HTTP request → FastAPI route → real ReadOnlyUnitOfWork
→ real SQLModelGraphRepository → real SQLite database.

get_unit_of_work is overridden to inject a backend-controlled real UoW;
get_tenant_config is NOT overridden — tenant resolution uses real settings.
"""

from __future__ import annotations

from collections.abc import Generator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.storage.backends.sqlmodel.base_tables import ResourceTable
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

ECOSYSTEM = "confluent_cloud"
TENANT_ID = "prod"
_CREATED = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def backend(tmp_path: Path) -> Generator[SQLModelBackend]:
    db_path = tmp_path / "graph_e2e_test.db"
    connection_string = f"sqlite:///{db_path}"
    b = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    b.create_tables()
    yield b
    b.dispose()


def _seed_data(backend: SQLModelBackend) -> None:
    """Insert environment, cluster, and chargeback data into the real SQLite DB."""
    with Session(backend._engine) as s:
        # Environment resource
        s.add(
            ResourceTable(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                resource_id="env-abc",
                resource_type="environment",
                display_name="env-abc",
                parent_id=None,
                status="active",
                cloud=None,
                region=None,
                created_at=_CREATED,
                deleted_at=None,
            )
        )
        # Kafka cluster with "kafka" in display_name — searchable
        s.add(
            ResourceTable(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                resource_id="lkc-kafka-prod",
                resource_type="kafka_cluster",
                display_name="kafka-prod-cluster",
                parent_id="env-abc",
                status="active",
                cloud=None,
                region=None,
                created_at=_CREATED,
                deleted_at=None,
            )
        )
        # Chargeback dimension keyed to env-abc (env_id grouping for environment cost)
        s.add(
            ChargebackDimensionTable(
                dimension_id=1,
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                resource_id="lkc-kafka-prod",
                identity_id="",
                env_id="env-abc",
                product_category="KAFKA",
                product_type="KAFKA_NUM_CKUS",
                cost_type="usage",
                allocation_method=None,
                allocation_detail=None,
            )
        )
        # Chargeback dimension keyed to cluster (resource_id grouping for timeline)
        s.add(
            ChargebackDimensionTable(
                dimension_id=2,
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                resource_id="lkc-kafka-prod",
                identity_id="",
                env_id="env-abc",
                product_category="KAFKA",
                product_type="KAFKA_NUM_CKUS",
                cost_type="usage",
                allocation_method=None,
                allocation_detail=None,
            )
        )
        # Fact in March (before period for diff)
        s.add(
            ChargebackFactTable(
                timestamp=datetime(2026, 3, 15, tzinfo=UTC),
                dimension_id=1,
                amount="100.00",
            )
        )
        # Fact in April (after period for diff + timeline period)
        s.add(
            ChargebackFactTable(
                timestamp=datetime(2026, 4, 5, tzinfo=UTC),
                dimension_id=2,
                amount="150.00",
            )
        )
        s.commit()


@contextmanager
def _app_with_real_uow(backend: SQLModelBackend) -> Iterator[TestClient]:
    settings = AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        logging=LoggingConfig(),
        tenants={
            TENANT_ID: TenantConfig(
                tenant_id=TENANT_ID,
                ecosystem=ECOSYSTEM,
                storage=StorageConfig(connection_string=backend._connection_string),
            )
        },
    )
    app = create_app(settings)

    def _uow_override() -> Iterator[Any]:
        with backend.create_read_only_unit_of_work() as uow:
            yield uow

    app.dependency_overrides[get_unit_of_work] = _uow_override
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


class TestGraphEndpointsIntegration:
    def test_search_endpoint_returns_matching_resources(self, backend: SQLModelBackend) -> None:
        """Integration: GET /graph/search?q=kafka → 200, results include the seeded cluster."""
        _seed_data(backend)

        with _app_with_real_uow(backend) as client:
            resp = client.get(f"/api/v1/tenants/{TENANT_ID}/graph/search", params={"q": "kafka"})

        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        result_ids = [r["id"] for r in data["results"]]
        assert "lkc-kafka-prod" in result_ids

    def test_diff_endpoint_returns_environment_nodes(self, backend: SQLModelBackend) -> None:
        """Integration: GET /graph/diff returns env-level diff nodes with cost_before and cost_after."""
        _seed_data(backend)

        params = {
            "from_start": "2026-03-01",
            "from_end": "2026-03-31",
            "to_start": "2026-04-01",
            "to_end": "2026-04-13",
        }
        with _app_with_real_uow(backend) as client:
            resp = client.get(f"/api/v1/tenants/{TENANT_ID}/graph/diff", params=params)

        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data
        node_ids = [n["id"] for n in data["nodes"]]
        assert "env-abc" in node_ids

        env_node = next(n for n in data["nodes"] if n["id"] == "env-abc")
        assert "cost_before" in env_node
        assert "cost_after" in env_node
        assert "status" in env_node

    def test_timeline_endpoint_returns_daily_points(self, backend: SQLModelBackend) -> None:
        """Integration: GET /graph/timeline returns one point per day with gap-filled zeros."""
        _seed_data(backend)

        params = {
            "entity_id": "lkc-kafka-prod",
            "start": "2026-04-01",
            "end": "2026-04-13",
        }
        with _app_with_real_uow(backend) as client:
            resp = client.get(f"/api/v1/tenants/{TENANT_ID}/graph/timeline", params=params)

        assert resp.status_code == 200
        data = resp.json()
        assert data["entity_id"] == "lkc-kafka-prod"
        assert "points" in data
        # 13 days: Apr 1 through Apr 13 (end is inclusive in the URL param, exclusive in DB query)
        assert len(data["points"]) == 13
        # April 5 has seeded billing data
        points_by_date = {p["date"]: p["cost"] for p in data["points"]}
        assert "2026-04-05" in points_by_date

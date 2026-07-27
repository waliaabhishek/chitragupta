from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import respx
from alembic import command
from httpx import Response
from sqlalchemy import create_engine, text

from core.api.app import create_app
from core.config.models import AppSettings, PreviewConfig, StorageConfig, TenantConfig
from core.preview import spooling
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from tests.integration.core.api.test_focus_preview_pipeline import (
    PipelineApiClient,
    PreviewPipelineHandler,
    PreviewPipelinePlugin,
    _cost_response,
    _focus_preview_block,
    _mock_organization_api,
)
from tests.unit.core.preview.test_bounded_artifacts import _install_direct_sqlite_tracker
from tests.unit.core.storage.test_migration_019_focus_preview import _alembic_config
from workflow_runner import WorkflowRunner

if TYPE_CHECKING:
    import httpx
    import pytest

RELEASE_HEAD = "ddebea2fe0a8"
START = date(2026, 6, 1)
END = date(2026, 7, 1)
OUTSIDE = date(2026, 5, 31)


def _seed_release_database(connection_string: str) -> None:
    engine = create_engine(connection_string)
    all_dates = (OUTSIDE,) + tuple(START + timedelta(days=offset) for offset in range((END - START).days))
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO resources (
                    ecosystem, tenant_id, resource_id, resource_type, display_name,
                    parent_id, owner_id, status, cloud, region, created_at,
                    deleted_at, last_seen_at, metadata_json
                ) VALUES
                    ('confluent_cloud', 'tenant-1', 'env-1', 'environment',
                     'Production', NULL, NULL, 'active', 'aws', 'us-east-1',
                     '2026-01-01 00:00:00', NULL, NULL, '{}'),
                    ('confluent_cloud', 'tenant-1', 'lkc-1', 'kafka_cluster',
                     'Orders', 'env-1', NULL, 'active', 'aws', 'us-east-1',
                     '2026-01-01 00:00:00', NULL, NULL,
                     '{"provider_cloud":"AWS","provider_region":"us-east-1"}')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO identities (
                    ecosystem, tenant_id, identity_id, identity_type, display_name,
                    created_at, deleted_at, last_seen_at, metadata_json
                ) VALUES (
                    'confluent_cloud', 'tenant-1', 'sa-1', 'service_account',
                    'Orders service', '2026-01-01 00:00:00', NULL, NULL, '{}'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO chargeback_dimensions (
                    dimension_id, ecosystem, tenant_id, resource_id,
                    product_category, product_type, identity_id, cost_type,
                    allocation_method, allocation_detail
                ) VALUES (
                    41, 'confluent_cloud', 'tenant-1', 'lkc-1', 'KAFKA',
                    'KAFKA_STORAGE', 'sa-1', 'usage', 'direct', NULL
                )
                """
            )
        )
        for tracking_date in all_dates:
            timestamp = datetime.combine(tracking_date, datetime.min.time())
            connection.execute(
                text(
                    """
                    INSERT INTO ccloud_billing (
                        ecosystem, tenant_id, timestamp, env_id, resource_id,
                        product_type, product_category, quantity, unit_price,
                        total_cost, currency, granularity, allocation_attempts,
                        metadata_json
                    ) VALUES (
                        'confluent_cloud', 'tenant-1', :timestamp, 'env-1', 'lkc-1',
                        'KAFKA_STORAGE', 'KAFKA', '5', '2', '8', 'USD', 'daily',
                        0, '{}'
                    )
                    """
                ),
                {"timestamp": timestamp},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO pipeline_state (
                        ecosystem, tenant_id, tracking_date, billing_gathered,
                        resources_gathered, chargeback_calculated
                    ) VALUES (
                        'confluent_cloud', 'tenant-1', :tracking_date, 1, 1, 1
                    )
                    """
                ),
                {"tracking_date": tracking_date},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO chargeback_facts (
                        timestamp, dimension_id, amount, tags_json
                    ) VALUES (:timestamp, 41, '8', '[]')
                    """
                ),
                {"timestamp": timestamp},
            )
    engine.dispose()


def _preview(
    client: PipelineApiClient,
    *,
    grain: str,
    start: date,
    end: date,
) -> dict[str, Any]:
    payload: dict[str, str] = {
        "grain": grain,
        "column_profile": "full",
    }
    if grain == "monthly":
        payload["month"] = f"{start.year:04d}-{start.month:02d}"
    else:
        payload.update(
            start_date=start.isoformat(),
            end_date=end.isoformat(),
        )
    response = client.post(
        "/api/v1/tenants/production/focus-preview/requests",
        json=payload,
    )
    assert response.status_code == 202
    request_id = response.json()["request_id"]
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        status = client.get(
            f"/api/v1/tenants/production/focus-preview/requests/{request_id}",
        )
        assert status.status_code == 200
        if status.json()["status"] in {"ready", "failed"}:
            return status.json()
        time.sleep(0.01)
    raise AssertionError("Preview request did not reach a terminal status")


def _repair(client: PipelineApiClient) -> dict[str, Any]:
    response = client.post(
        "/api/v1/tenants/production/focus-preview/repairs",
        json={"start_date": START.isoformat(), "end_date": END.isoformat()},
    )
    assert response.status_code == 202
    repair_id = response.json()["repair_id"]
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status = client.get(
            f"/api/v1/tenants/production/focus-preview/repairs/{repair_id}",
        )
        assert status.status_code == 200
        if status.json()["status"] in {
            "completed",
            "completed_with_failures",
            "failed",
        }:
            return status.json()
        time.sleep(0.01)
    raise AssertionError("historical repair did not reach a terminal status")


def _outside_snapshot(
    connection_string: str,
) -> tuple[tuple[object, ...], tuple[object, ...], tuple[object, ...]]:
    engine = create_engine(connection_string)
    try:
        with engine.connect() as connection:
            billing = tuple(
                connection.execute(
                    text(
                        """
                        SELECT * FROM ccloud_billing
                        WHERE tenant_id = 'tenant-1' AND date(timestamp) = :tracking_date
                        """
                    ),
                    {"tracking_date": OUTSIDE},
                ).one()
            )
            chargeback = tuple(
                connection.execute(
                    text(
                        """
                        SELECT chargeback_facts.*
                        FROM chargeback_facts
                        JOIN chargeback_dimensions USING (dimension_id)
                        WHERE chargeback_dimensions.tenant_id = 'tenant-1'
                          AND date(chargeback_facts.timestamp) = :tracking_date
                        """
                    ),
                    {"tracking_date": OUTSIDE},
                ).one()
            )
            pipeline_state = tuple(
                connection.execute(
                    text(
                        """
                        SELECT * FROM pipeline_state
                        WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1'
                          AND tracking_date = :tracking_date
                        """
                    ),
                    {"tracking_date": OUTSIDE},
                ).one()
            )
        return billing, chargeback, pipeline_state
    finally:
        engine.dispose()


def _utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    assert isinstance(parsed, datetime)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _assert_repaired_state_agrees(
    connection_string: str,
    repair_id: str,
    repaired_dates: list[dict[str, Any]],
) -> None:
    engine = create_engine(connection_string)
    try:
        with engine.connect() as connection:
            for repaired in repaired_dates:
                tracking_date = date.fromisoformat(repaired["tracking_date"])
                pipeline = connection.execute(
                    text(
                        """
                        SELECT calculation_id, calculation_completed_at,
                               chargeback_calculated
                        FROM pipeline_state
                        WHERE ecosystem = 'confluent_cloud'
                          AND tenant_id = 'tenant-1'
                          AND tracking_date = :tracking_date
                        """
                    ),
                    {"tracking_date": tracking_date},
                ).one()
                lineage = connection.execute(
                    text(
                        """
                        SELECT calculation_id, calculation_completed_at,
                               capture_status
                        FROM ccloud_allocation_lineage_runs
                        WHERE ecosystem = 'confluent_cloud'
                          AND tenant_id = 'tenant-1'
                          AND tracking_date = :tracking_date
                        """
                    ),
                    {"tracking_date": tracking_date},
                ).one()
                attempt = connection.execute(
                    text(
                        """
                        SELECT attempt_sequence, refresh_start, refresh_end, status
                        FROM ccloud_source_evidence_attempts
                        WHERE ecosystem = 'confluent_cloud'
                          AND tenant_id = 'tenant-1'
                          AND refresh_token = :refresh_token
                        """
                    ),
                    {"refresh_token": (f"repair:{repair_id}:{tracking_date.isoformat()}")},
                ).one()
                readiness = connection.execute(
                    text(
                        """
                        SELECT capture_id, source_count
                        FROM ccloud_source_capture_readiness_history
                        WHERE ecosystem = 'confluent_cloud'
                          AND tenant_id = 'tenant-1'
                          AND attempt_sequence = :attempt_sequence
                        ORDER BY window_start, window_end
                        """
                    ),
                    {"attempt_sequence": attempt.attempt_sequence},
                ).all()
                source_count = connection.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM ccloud_cost_source_records AS source
                        JOIN ccloud_source_capture_readiness_history AS readiness
                          ON readiness.ecosystem = source.ecosystem
                         AND readiness.tenant_id = source.tenant_id
                         AND readiness.capture_id = source.capture_id
                        WHERE readiness.attempt_sequence = :attempt_sequence
                          AND date(source.billing_timestamp) = :tracking_date
                        """
                    ),
                    {
                        "attempt_sequence": attempt.attempt_sequence,
                        "tracking_date": tracking_date,
                    },
                ).scalar_one()
                billing = connection.execute(
                    text(
                        """
                        SELECT COUNT(*), SUM(total_cost)
                        FROM ccloud_billing
                        WHERE ecosystem = 'confluent_cloud'
                          AND tenant_id = 'tenant-1'
                          AND date(timestamp) = :tracking_date
                        """
                    ),
                    {"tracking_date": tracking_date},
                ).one()
                chargeback = connection.execute(
                    text(
                        """
                        SELECT COUNT(*), SUM(chargeback_facts.amount)
                        FROM chargeback_facts
                        JOIN chargeback_dimensions USING (dimension_id)
                        WHERE chargeback_dimensions.ecosystem = 'confluent_cloud'
                          AND chargeback_dimensions.tenant_id = 'tenant-1'
                          AND date(chargeback_facts.timestamp) = :tracking_date
                        """
                    ),
                    {"tracking_date": tracking_date},
                ).one()

                calculation_id = repaired["calculation_id"]
                calculation_completed_at = _utc(repaired["calculation_completed_at"])
                assert pipeline.calculation_id == lineage.calculation_id == calculation_id
                assert _utc(pipeline.calculation_completed_at) == calculation_completed_at
                assert _utc(lineage.calculation_completed_at) == calculation_completed_at
                assert pipeline.chargeback_calculated == 1
                assert lineage.capture_status == "complete"
                assert attempt.status == "complete"
                assert _utc(attempt.refresh_start).date() == tracking_date
                assert _utc(attempt.refresh_end).date() == tracking_date + timedelta(days=1)
                assert readiness
                assert sum(item.source_count for item in readiness) == 1
                assert all(item.capture_id for item in readiness)
                assert source_count == repaired["rows_written"] == 1
                assert billing == (1, 8)
                assert chargeback == (1, 8)
    finally:
        engine.dispose()


@respx.mock
def test_v210_retained_month_fails_then_production_rest_repair_enables_daily_and_monthly_preview(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    direct_sqlite = _install_direct_sqlite_tracker(spooling, monkeypatch)
    request.addfinalizer(direct_sqlite.cleanup)
    connection_string = f"sqlite:///{tmp_path / 'v2.1.0-upgrade.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, RELEASE_HEAD)
    _seed_release_database(connection_string)
    command.upgrade(config, "head")

    engine = create_engine(connection_string)
    try:
        with engine.connect() as connection:
            missing_correlations = connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM pipeline_state
                    WHERE tracking_date >= :start AND tracking_date < :end
                      AND calculation_id IS NULL
                      AND calculation_completed_at IS NULL
                      AND calculation_run_id IS NULL
                    """
                ),
                {"start": START, "end": END},
            ).scalar_one()
    finally:
        engine.dispose()
    assert missing_correlations == 30

    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=200,
        cutoff_days=5,
        retention_days=250,
        storage=StorageConfig(connection_string=connection_string),
        focus_preview=_focus_preview_block(),
        plugin_settings={
            "ccloud_api": {"key": "key", "secret": "secret"},  # pragma: allowlist secret
            "billing_api": {"days_per_query": 1},
            "min_refresh_gap_seconds": 0,
        },
    )
    settings = AppSettings(
        preview=PreviewConfig(artifact_root=tmp_path / "artifacts", max_workers=1),
        tenants={"production": tenant},
    )
    _mock_organization_api()
    fail_provider_history = True
    requested_dates: list[date] = []

    def cost_response(request: httpx.Request) -> httpx.Response:
        nonlocal fail_provider_history
        requested_start = date.fromisoformat(request.url.params["start_date"])
        requested_end = date.fromisoformat(request.url.params["end_date"])
        assert requested_end == requested_start + timedelta(days=1)
        requested_dates.append(requested_start)
        if fail_provider_history and requested_start == date(2026, 6, 15):
            return Response(503, json={"message": "controlled provider history gap"})
        return _cost_response(
            id=f"cost-{requested_start.isoformat()}",
            start_date=requested_start.isoformat(),
            end_date=requested_end.isoformat(),
        )

    costs = respx.get("https://api.confluent.cloud/billing/v1/costs")
    costs.side_effect = cost_response
    plugin = PreviewPipelinePlugin(PreviewPipelineHandler())
    registry = MagicMock()
    registry.create.return_value = plugin
    runner = WorkflowRunner(settings, registry)
    runner.bootstrap_storage()
    with runner.acquire_backend("production", tenant) as storage:
        assert isinstance(storage, SQLModelBackend)
        backend = storage
    outside_before = _outside_snapshot(connection_string)
    app = create_app(settings, workflow_runner=runner, mode="both")
    client = PipelineApiClient(app, use_lifespan=True, backend=backend)
    repair_id = ""
    try:
        outside_export_request = {
            "start_date": OUTSIDE.isoformat(),
            "end_date": OUTSIDE.isoformat(),
            "timezone": "Asia/Kolkata",
        }
        outside_export_before = client.post(
            "/api/v1/tenants/production/export",
            json=outside_export_request,
        )
        assert outside_export_before.status_code == 200
        outside_export_bytes = outside_export_before.content
        outside_export_rows = tuple(outside_export_bytes.splitlines())
        assert len(outside_export_rows) == 2
        assert OUTSIDE.isoformat().encode() in outside_export_rows[1]

        initial = _preview(
            client,
            grain="daily",
            start=START,
            end=START + timedelta(days=1),
        )
        assert initial["status"] == "failed"
        assert initial["diagnostic"]["code"] == "calculation_metadata_unavailable"

        runtime = runner._tenant_runtimes["production"]
        runtime.orchestrator._refresh_preview_organization_authority()
        first_repair = _repair(client)
        first_requested_dates = tuple(requested_dates)

        assert first_repair["status"] == "completed_with_failures"
        assert set(first_requested_dates) == {START + timedelta(days=offset) for offset in range((END - START).days)}
        assert date(2026, 6, 16) in first_requested_dates
        assert all(item["status"] == "failed" for item in first_repair["dates"])

        fail_provider_history = False
        requested_dates.clear()
        repaired = _repair(client)
        repair_id = repaired["repair_id"]

        assert repaired["status"] == "completed"
        assert len(repaired["dates"]) == 30
        assert all(item["status"] == "succeeded" for item in repaired["dates"])
        assert all(item["calculation_id"] for item in repaired["dates"])
        assert all(item["calculation_completed_at"] for item in repaired["dates"])
        assert all(item["rows_written"] == 1 for item in repaired["dates"])
        _assert_repaired_state_agrees(connection_string, repair_id, repaired["dates"])
        assert set(requested_dates) == {START + timedelta(days=offset) for offset in range((END - START).days)}
        assert costs.call_count >= 60
        assert _outside_snapshot(connection_string) == outside_before
        outside_export_after = client.post(
            "/api/v1/tenants/production/export",
            json=outside_export_request,
        )
        assert outside_export_after.status_code == 200
        assert outside_export_after.content == outside_export_bytes
        assert tuple(outside_export_after.content.splitlines()) == outside_export_rows

        daily = _preview(
            client,
            grain="daily",
            start=START,
            end=START + timedelta(days=1),
        )
        monthly = _preview(client, grain="monthly", start=START, end=END)
        assert daily["status"] == "ready"
        assert monthly["status"] == "ready"
    finally:
        client.close()

    restarted_plugin = PreviewPipelinePlugin(PreviewPipelineHandler())
    restarted_registry = MagicMock()
    restarted_registry.create.return_value = restarted_plugin
    restarted = create_app(
        settings,
        mode="api",
        plugin_registry=restarted_registry,
    )
    restarted_client = PipelineApiClient(restarted, use_lifespan=True)
    try:
        retained = restarted_client.get(
            f"/api/v1/tenants/production/focus-preview/repairs/{repair_id}",
        )
        assert retained.status_code == 200
        assert retained.json()["status"] == "completed"
    finally:
        restarted_client.close()

    assert direct_sqlite.opens > 0
    assert direct_sqlite.opens == direct_sqlite.explicit_closes
    assert direct_sqlite.live == 0

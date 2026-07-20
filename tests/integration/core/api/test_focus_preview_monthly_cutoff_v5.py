from __future__ import annotations

import csv
import io
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import anyio.to_thread
import pytest
import respx
from sqlalchemy import text

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, PreviewConfig, StorageConfig, TenantConfig
from core.engine.orchestrator import ChargebackOrchestrator
from core.models.pipeline import PipelineState
from core.preview.persistence import SQLModelPreviewCalculationRepository
from core.storage.backends.sqlmodel.repositories import (
    SQLModelEntityTagRepository,
    SQLModelIdentityRepository,
    SQLModelResourceRepository,
)
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud import ConfluentCloudPlugin
from tests.integration.core.api.test_focus_preview import SameThreadApiClient
from tests.integration.core.api.test_focus_preview_allocation_lineage import _seed_context
from tests.unit.core.preview.test_service import ControlledExecutor, _aggregate, _source


@pytest.fixture(autouse=True)
def _inline_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run_inline(function: Callable[..., object], *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Callable[..., object], *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", run_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)


def _settings(tmp_path: Path, *, cutoff_days: int = 1) -> tuple[AppSettings, TenantConfig]:
    connection = f"sqlite:///{tmp_path / 'monthly-cutoff.db'}"
    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        lookback_days=364,
        cutoff_days=cutoff_days,
        storage=StorageConfig(connection_string=connection),
        focus_preview={
            "commercial_profile": "direct_payg",
            "billing_currency": "USD",
            "effective_start_date": "2020-01-01",
            "effective_end_date": "2030-01-01",
        },
        plugin_settings={"ccloud_api": {"key": "key", "secret": "secret"}},  # pragma: allowlist secret
    )
    return (
        AppSettings(
            api=ApiConfig(host="127.0.0.1", port=8080),
            preview=PreviewConfig(artifact_root=tmp_path / "artifacts", max_workers=1),
            tenants={"production": tenant},
        ),
        tenant,
    )


def _zero_lineage_backend(
    tmp_path: Path, *, through: date = date(2026, 7, 31)
) -> tuple[SQLModelBackend, AppSettings, TenantConfig, ConfluentCloudPlugin]:
    settings, tenant = _settings(tmp_path)
    connection = tenant.storage.connection_string.get_secret_value()
    plugin = ConfluentCloudPlugin()
    plugin.initialize(tenant.plugin_settings.model_dump())
    backend = SQLModelBackend(connection, plugin.get_storage_module(), use_migrations=False)
    backend.create_tables()
    orchestrator = ChargebackOrchestrator("production", tenant, plugin, backend)
    tracking = date(2026, 7, 1)
    with backend.create_unit_of_work() as uow:
        while tracking <= through:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="confluent_cloud",
                    tenant_id="org-1",
                    tracking_date=tracking,
                    billing_gathered=True,
                    resources_gathered=True,
                )
            )
            tracking += timedelta(days=1)
        uow.commit()
    tracking = date(2026, 7, 1)
    while tracking <= through:
        with backend.create_unit_of_work() as uow:
            assert orchestrator._calculate_phase.run(uow, tracking) == 0
            uow.commit()
        tracking += timedelta(days=1)
    return backend, settings, tenant, plugin


def _submit(
    client: SameThreadApiClient,
    executor: ControlledExecutor,
    body: dict[str, object],
) -> dict[str, object]:
    response = client.post("/api/v1/tenants/production/focus-preview/requests", json=body)
    assert response.status_code == 202, response.text
    request_id = response.json()["request_id"]
    executor.run_all()
    terminal = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
    assert terminal.status_code == 200
    return terminal.json()


def _csv(client: SameThreadApiClient, ready: dict[str, object]) -> bytes:
    package = ready["package"]
    assert isinstance(package, dict)
    files = package["files"]
    assert isinstance(files, list)
    response = client.get(files[0]["download_url"])
    assert response.status_code == 200
    return response.content


@pytest.mark.parametrize(
    ("submitted_at", "cutoff_days", "expected_status", "effective_end", "cutoff_end"),
    [
        (datetime(2026, 8, 1, 12, tzinfo=UTC), 1, "provisional", "2026-07-31", "2026-07-31"),
        (datetime(2026, 8, 2, 12, tzinfo=UTC), 1, "provisional", "2026-08-01", "2026-08-01"),
        (datetime(2026, 8, 4, 0, tzinfo=UTC), 1, "settled", "2026-08-01", "2026-08-03"),
        (datetime(2026, 8, 5, 0, tzinfo=UTC), 5, "provisional", "2026-07-31", "2026-07-31"),
    ],
)
def test_primary_api_monthly_cutoff_and_72_hour_classification_uses_real_zero_lineage(
    tmp_path: Path,
    submitted_at: datetime,
    cutoff_days: int,
    expected_status: str,
    effective_end: str,
    cutoff_end: str,
) -> None:
    backend, settings, tenant, plugin = _zero_lineage_backend(tmp_path)
    settings.tenants["production"] = tenant.model_copy(update={"cutoff_days": cutoff_days})
    app = create_app(settings)
    executor = ControlledExecutor()
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
            app.state.backends["production"] = backend
            app.state.preview_runtime._executor = executor
            app.state.preview_runtime._owns_executor = False
            app.state.preview_runtime._clock = lambda: submitted_at
            terminal = _submit(
                client,
                executor,
                {"grain": "monthly", "month": "2026-07", "column_profile": "full"},
            )

            assert terminal["status"] == "ready", terminal["diagnostic"]
            snapshot = terminal["source_snapshot"]
            assert snapshot["monthly_status"] == expected_status
            assert snapshot["effective_coverage_start_date"] == "2026-07-01"
            assert snapshot["effective_coverage_end_date"] == effective_end
            assert snapshot["availability_cutoff_end_date"] == cutoff_end
            assert len(list(csv.reader(io.StringIO(_csv(client, terminal).decode())))) == 1
    finally:
        backend.dispose()
        plugin.close()


def test_monthly_submission_freezes_cutoff_classification_before_delayed_worker(
    tmp_path: Path,
) -> None:
    backend, settings, _tenant, plugin = _zero_lineage_backend(tmp_path)
    app = create_app(settings)
    executor = ControlledExecutor()
    now = datetime(2026, 8, 2, 12, tzinfo=UTC)
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
            app.state.backends["production"] = backend
            app.state.preview_runtime._executor = executor
            app.state.preview_runtime._owns_executor = False
            app.state.preview_runtime._clock = lambda: now
            submitted = client.post(
                "/api/v1/tenants/production/focus-preview/requests",
                json={"grain": "monthly", "month": "2026-07", "column_profile": "full"},
            )
            assert submitted.status_code == 202

            now = datetime(2026, 8, 5, tzinfo=UTC)
            executor.run_all()
            frozen = client.get(
                f"/api/v1/tenants/production/focus-preview/requests/{submitted.json()['request_id']}"
            ).json()
            later = _submit(
                client,
                executor,
                {"grain": "monthly", "month": "2026-07", "column_profile": "full"},
            )

            assert frozen["source_snapshot"]["monthly_status"] == "provisional"
            assert frozen["source_snapshot"]["availability_cutoff_end_date"] == "2026-08-01"
            assert later["source_snapshot"]["monthly_status"] == "settled"
            assert later["source_snapshot"]["availability_cutoff_end_date"] == "2026-08-04"
    finally:
        backend.dispose()
        plugin.close()


@respx.mock
def test_settled_monthly_positive_sources_use_real_calculate_lineage_and_persist_exact_package(
    tmp_path: Path,
) -> None:
    backend, settings, tenant, plugin = _zero_lineage_backend(tmp_path)
    _seed_context(backend)
    sources = []
    for offset in range(2):
        tracking_date = date(2026, 7, 1) + timedelta(days=offset)
        start = datetime.combine(tracking_date, datetime.min.time(), tzinfo=UTC)
        end = start + timedelta(days=1)
        sources.append(
            _source(
                tenant_id="org-1",
                source_record_id=f"provider:cluster-link-{offset + 1}",
                provider_cost_id=f"cluster-link-{offset + 1}",
                source_period_start=start,
                source_period_end=end,
                collection_window_start=datetime(2026, 7, 1, tzinfo=UTC),
                collection_window_end=datetime(2026, 7, 3, tzinfo=UTC),
                evidence_scope_start=start,
                evidence_scope_end=end,
                allocation_timestamp=start,
                retention_timestamp=start,
                product="CLUSTER_LINK",
                line_type="CLUSTER_LINKING_PER_LINK",
                amount=3,
                original_amount=3,
                discount_amount=0,
                price=3,
                quantity=1,
                unit="LINK_HOUR",
                description="Cluster Linking per-link usage",
            )
        )
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "org-1",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 3, tzinfo=UTC),
            sources,
        )
        for offset in range(2):
            tracking_date = date(2026, 7, 1) + timedelta(days=offset)
            uow.billing.upsert(
                _aggregate(
                    tenant_id="org-1",
                    timestamp=datetime.combine(tracking_date, datetime.min.time(), tzinfo=UTC),
                    product_category="CLUSTER_LINK",
                    product_type="CLUSTER_LINKING_PER_LINK",
                    quantity=1,
                    unit_price=3,
                    total_cost=3,
                )
            )
            uow.pipeline_state.mark_needs_recalculation("confluent_cloud", "org-1", tracking_date)
        uow.commit()
    orchestrator = ChargebackOrchestrator("production", tenant, plugin, backend)
    for tracking_date in (date(2026, 7, 1), date(2026, 7, 2)):
        with backend.create_unit_of_work() as uow:
            assert orchestrator._calculate_phase.run(uow, tracking_date) == 1
            uow.commit()

    app = create_app(settings)
    executor = ControlledExecutor()
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
            app.state.backends["production"] = backend
            app.state.preview_runtime._executor = executor
            app.state.preview_runtime._owns_executor = False
            app.state.preview_runtime._clock = lambda: datetime(2026, 8, 4, tzinfo=UTC)
            ready = _submit(
                client,
                executor,
                {"grain": "monthly", "month": "2026-07", "column_profile": "full"},
            )

            assert ready["status"] == "ready", ready["diagnostic"]
            snapshot = ready["source_snapshot"]
            assert snapshot["monthly_status"] == "settled"
            assert snapshot["effective_coverage_end_date"] == "2026-08-01"
            assert snapshot["availability_cutoff_end_date"] == "2026-08-03"
            assert snapshot["source_through"] is not None
            rows = list(csv.DictReader(io.StringIO(_csv(client, ready).decode())))
            assert len(rows) == 1
            assert rows[0]["ChargePeriodStart"] == "2026-07-01T00:00:00Z"
            assert rows[0]["ChargePeriodEnd"] == "2026-08-01T00:00:00Z"
            assert rows[0]["BilledCost"] == "6"
            assert rows[0]["EffectiveCost"] == "6"
            assert rows[0]["PricingQuantity"] == "2"
            assert rows[0]["x_ChitraguptaSourceCostId"].startswith("chitragupta:confluent-cloud:source-cost-set:v1:")
            manifest_response = client.get(ready["package"]["manifest"]["download_url"])
            assert manifest_response.status_code == 200
            manifest = manifest_response.json()
            assert manifest["monthly_status"] == "settled"
            assert manifest["source_snapshot"]["source_through"] == snapshot["source_through"]
            assert manifest["reconciliation"] == {
                "source_cost": "6",
                "allocated_cost": "6",
                "difference": "0",
            }
            assert len(respx.calls) == 0

        with backend._engine.connect() as connection:
            persisted = connection.execute(
                text(
                    """
                    SELECT status, storage_key, monthly_status, effective_coverage_end_date,
                           manifest_metadata_json, data_files_json
                    FROM preview_requests WHERE request_id = :request_id
                    """
                ),
                {"request_id": ready["request_id"]},
            ).one()
        assert persisted.status == "ready"
        assert persisted.storage_key
        assert persisted.monthly_status == "settled"
        assert str(persisted.effective_coverage_end_date) == "2026-08-01"
        assert persisted.manifest_metadata_json
        assert persisted.data_files_json
    finally:
        backend.dispose()
        plugin.close()


@pytest.mark.parametrize(
    ("grain", "profile"),
    [(grain, profile) for grain in ("daily", "monthly") for profile in ("full", "summary", "custom")],
)
def test_complete_zero_portion_lineage_is_ready_without_enrichment_reads_for_all_profiles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    grain: str,
    profile: str,
) -> None:
    backend, settings, _tenant, plugin = _zero_lineage_backend(tmp_path)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("zero-source Preview must not read enrichment repositories")

    monkeypatch.setattr(SQLModelResourceRepository, "find_active_at", forbidden)
    monkeypatch.setattr(SQLModelIdentityRepository, "get_many", forbidden)
    monkeypatch.setattr(SQLModelIdentityRepository, "find_active_at", forbidden)
    monkeypatch.setattr(SQLModelEntityTagRepository, "find_tags_for_entities", forbidden)
    app = create_app(settings)
    executor = ControlledExecutor()
    body: dict[str, object] = {"grain": grain, "column_profile": profile}
    if grain == "daily":
        body.update(start_date="2026-07-01", end_date="2026-08-01")
    else:
        body["month"] = "2026-07"
    if profile == "custom":
        body["columns"] = ["Tags", "BilledCost"]
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
            app.state.backends["production"] = backend
            app.state.preview_runtime._executor = executor
            app.state.preview_runtime._owns_executor = False
            app.state.preview_runtime._clock = lambda: datetime(2026, 8, 5, tzinfo=UTC)
            ready = _submit(client, executor, body)

            assert ready["status"] == "ready", ready["diagnostic"]
            manifest = client.get(ready["package"]["manifest"]["download_url"])
            assert manifest.status_code == 200
            assert manifest.json()["mapping_profile_version"] == "focus-1.4-preview-v5"
            assert len(list(csv.reader(io.StringIO(_csv(client, ready).decode())))) == 1
    finally:
        backend.dispose()
        plugin.close()


def test_future_and_empty_early_months_do_not_require_calculation_evidence(tmp_path: Path) -> None:
    backend, settings, _tenant, plugin = _zero_lineage_backend(tmp_path, through=date(2026, 7, 1))
    app = create_app(settings)
    executor = ControlledExecutor()
    now = datetime(2026, 6, 30, tzinfo=UTC)
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
            app.state.backends["production"] = backend
            app.state.preview_runtime._executor = executor
            app.state.preview_runtime._owns_executor = False
            app.state.preview_runtime._clock = lambda: now
            future = _submit(
                client,
                executor,
                {"grain": "monthly", "month": "2026-07", "column_profile": "full"},
            )
            assert future["status"] == "failed"
            assert future["diagnostic"]["code"] == "calculation_pending_cutoff_window"
            assert future["diagnostic"]["retryable"] is True
            assert future["package"] is None

            now = datetime(2026, 7, 1, tzinfo=UTC)
            empty = _submit(
                client,
                executor,
                {"grain": "monthly", "month": "2026-07", "column_profile": "full"},
            )
            assert empty["status"] == "ready", empty["diagnostic"]
            assert empty["source_snapshot"]["effective_coverage_start_date"] == "2026-07-01"
            assert empty["source_snapshot"]["effective_coverage_end_date"] == "2026-07-01"
            assert empty["source_snapshot"]["calculation_coverage"] == []
            assert len(list(csv.reader(io.StringIO(_csv(client, empty).decode())))) == 1
    finally:
        backend.dispose()
        plugin.close()


def test_monthly_gap_checks_only_the_effective_interval_and_not_later_dates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend, settings, _tenant, plugin = _zero_lineage_backend(tmp_path, through=date(2026, 7, 3))
    with backend.create_unit_of_work() as uow:
        state = uow.pipeline_state.get("confluent_cloud", "org-1", date(2026, 7, 2))
        assert state is not None
        state.calculation_id = None
        state.calculation_completed_at = None
        state.chargeback_calculated = False
        uow.pipeline_state.upsert(state)
        uow.commit()
    calls: list[tuple[date, date]] = []
    original = SQLModelPreviewCalculationRepository.find_current_coverage

    def spy(self: SQLModelPreviewCalculationRepository, **kwargs: object) -> object:
        calls.append((kwargs["start_date"], kwargs["end_date"]))  # type: ignore[arg-type]
        return original(self, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(SQLModelPreviewCalculationRepository, "find_current_coverage", spy)
    app = create_app(settings)
    executor = ControlledExecutor()
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
            app.state.backends["production"] = backend
            app.state.preview_runtime._executor = executor
            app.state.preview_runtime._owns_executor = False
            app.state.preview_runtime._clock = lambda: datetime(2026, 7, 4, tzinfo=UTC)
            failed = _submit(
                client,
                executor,
                {"grain": "monthly", "month": "2026-07", "column_profile": "full"},
            )

            assert failed["status"] == "failed"
            assert failed["diagnostic"]["code"] == "calculation_coverage_incomplete"
            assert calls == [(date(2026, 7, 1), date(2026, 7, 3))]
    finally:
        backend.dispose()
        plugin.close()


def test_monthly_reconciliation_ignores_corrupt_evidence_after_effective_interval(tmp_path: Path) -> None:
    backend, settings, _tenant, plugin = _zero_lineage_backend(tmp_path, through=date(2026, 7, 3))
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "org-1",
            datetime(2026, 7, 3, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [
                _source(
                    tenant_id="org-1",
                    source_record_id="provider:later-corrupt",
                    provider_cost_id="later-corrupt",
                    source_period_start=datetime(2026, 7, 3, tzinfo=UTC),
                    source_period_end=datetime(2026, 7, 4, tzinfo=UTC),
                    collection_window_start=datetime(2026, 7, 3, tzinfo=UTC),
                    collection_window_end=datetime(2026, 7, 4, tzinfo=UTC),
                    evidence_scope_start=datetime(2026, 7, 3, tzinfo=UTC),
                    evidence_scope_end=datetime(2026, 7, 4, tzinfo=UTC),
                    allocation_timestamp=datetime(2026, 7, 3, tzinfo=UTC),
                    retention_timestamp=datetime(2026, 7, 3, tzinfo=UTC),
                    amount=99,
                )
            ],
        )
        uow.billing.upsert(
            _aggregate(
                tenant_id="org-1",
                timestamp=datetime(2026, 7, 3, tzinfo=UTC),
                total_cost=1,
            )
        )
        uow.commit()
    app = create_app(settings)
    executor = ControlledExecutor()
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
            app.state.backends["production"] = backend
            app.state.preview_runtime._executor = executor
            app.state.preview_runtime._owns_executor = False
            app.state.preview_runtime._clock = lambda: datetime(2026, 7, 4, tzinfo=UTC)
            ready = _submit(
                client,
                executor,
                {"grain": "monthly", "month": "2026-07", "column_profile": "full"},
            )

            assert ready["status"] == "ready", ready["diagnostic"]
            snapshot = ready["source_snapshot"]
            assert snapshot["effective_coverage_end_date"] == "2026-07-03"
            assert [entry["tracking_date"] for entry in snapshot["calculation_coverage"]] == [
                "2026-07-01",
                "2026-07-02",
            ]
            assert len(list(csv.reader(io.StringIO(_csv(client, ready).decode())))) == 1
    finally:
        backend.dispose()
        plugin.close()


@pytest.mark.parametrize(
    ("invalid_table", "expected_code"),
    [
        ("ccloud_cost_source_records", "preview_source_coverage_incomplete"),
        ("ccloud_billing", "preview_source_coverage_incomplete"),
        ("ccloud_allocation_lineage_runs", "preview_allocation_lineage_incomplete"),
    ],
)
def test_primary_api_invalid_evidence_permutations_start_from_real_calculate_phase_lineage(
    tmp_path: Path,
    invalid_table: str,
    expected_code: str,
) -> None:
    backend, settings, tenant, plugin = _zero_lineage_backend(tmp_path, through=date(2026, 7, 1))
    _seed_context(backend)
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "org-1",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 2, tzinfo=UTC),
            [
                _source(
                    tenant_id="org-1",
                    collection_window_start=datetime(2026, 7, 1, tzinfo=UTC),
                    collection_window_end=datetime(2026, 7, 2, tzinfo=UTC),
                )
            ],
        )
        uow.billing.upsert(_aggregate(tenant_id="org-1"))
        uow.pipeline_state.mark_needs_recalculation("confluent_cloud", "org-1", date(2026, 7, 1))
        uow.commit()
    orchestrator = ChargebackOrchestrator("production", tenant, plugin, backend)
    with backend.create_unit_of_work() as uow:
        assert orchestrator._calculate_phase.run(uow, date(2026, 7, 1)) > 0
        uow.commit()
    with backend._engine.begin() as connection:
        connection.exec_driver_sql(f"DELETE FROM {invalid_table}")

    app = create_app(settings)
    executor = ControlledExecutor()
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
            app.state.backends["production"] = backend
            app.state.preview_runtime._executor = executor
            app.state.preview_runtime._owns_executor = False
            app.state.preview_runtime._clock = lambda: datetime(2026, 7, 4, tzinfo=UTC)
            failed = _submit(
                client,
                executor,
                {
                    "grain": "daily",
                    "start_date": "2026-07-01",
                    "end_date": "2026-07-02",
                    "column_profile": "full",
                },
            )

            assert failed["status"] == "failed"
            assert failed["diagnostic"]["code"] == expected_code
            assert failed["package"] is None
    finally:
        backend.dispose()
        plugin.close()

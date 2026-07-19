from __future__ import annotations

import asyncio
import csv
import io
import time
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import anyio.to_thread
import httpx
import pytest
import respx
from alembic import command
from sqlalchemy import create_engine, text

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, PreviewConfig, StorageConfig, TenantConfig
from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.orchestrator import ChargebackOrchestrator
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity, Identity, IdentityResolution, IdentitySet
from core.models.pipeline import PipelineState
from core.models.resource import CoreResource, Resource, ResourceStatus
from core.preview.evidence import PreviewEvidenceScope
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud import ConfluentCloudPlugin
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem, CCloudCostSourceRecord
from tests.unit.core.storage.test_migration_019_focus_preview import (
    _alembic_config,
    _seed_legacy_rows,
    _snapshots,
)
from workflow_runner import TenantRuntime, WorkflowRunner, _config_hash


class PreviewPipelineHandler:
    service_type = "kafka"
    handles_product_types = ("KAFKA_STORAGE",)
    gathered_resource_types = ("kafka_cluster",)

    def __init__(self) -> None:
        self.failing_dates: set[date] = set()
        self.identity = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            identity_id="sa-1",
            identity_type="service_account",
            display_name="Orders service",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

    def gather_resources(
        self,
        tenant_id: str,
        uow: Any,
        shared_ctx: object | None = None,
    ) -> Iterable[Resource]:
        del uow, shared_ctx
        return (
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id=tenant_id,
                resource_id="lkc-1",
                resource_type="kafka_cluster",
                display_name="Orders",
                status=ResourceStatus.ACTIVE,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
        )

    def gather_identities(self, tenant_id: str, uow: Any) -> Iterable[Identity]:
        del tenant_id, uow
        return (self.identity,)

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: object,
        uow: Any,
        context: object | None = None,
    ) -> IdentityResolution:
        del tenant_id, resource_id, billing_duration, metrics_data, uow, context
        if billing_timestamp.date() in self.failing_dates:
            raise RuntimeError(f"intentional calculation failure for {billing_timestamp.date()}")
        active = IdentitySet()
        active.add(self.identity)
        return IdentityResolution(active, IdentitySet(), IdentitySet())

    def get_metrics_for_product_type(self, product_type: str) -> list[Any]:
        del product_type
        return []

    def get_allocator(self, product_type: str) -> Any:
        del product_type

        def allocate(ctx: AllocationContext) -> AllocationResult:
            return AllocationResult(
                rows=[
                    ChargebackRow(
                        ecosystem=ctx.billing_line.ecosystem,
                        tenant_id=ctx.billing_line.tenant_id,
                        timestamp=ctx.billing_line.timestamp,
                        resource_id=ctx.billing_line.resource_id,
                        product_category=ctx.billing_line.product_category,
                        product_type=ctx.billing_line.product_type,
                        identity_id="sa-1",
                        cost_type=CostType.USAGE,
                        amount=ctx.split_amount,
                        allocation_method="direct",
                        metadata={"env_id": "env-1"},
                    )
                ]
            )

        return allocate


class PreviewPipelinePlugin(ConfluentCloudPlugin):
    def __init__(self, handler: PreviewPipelineHandler) -> None:
        super().__init__()
        self._preview_handler = handler
        self.cost_input_override: object | None = None

    def initialize(self, config: dict[str, Any]) -> None:
        super().initialize(config)
        assert self._connection is not None
        self._connection.request_interval_seconds = 0
        self._handlers = {"kafka": self._preview_handler}

    def build_shared_context(self, tenant_id: str) -> None:
        del tenant_id
        return None

    def get_fallback_allocator(self) -> None:
        return None

    def get_cost_input(self) -> Any:
        if self.cost_input_override is not None:
            return self.cost_input_override
        return super().get_cost_input()


def _focus_preview_block() -> dict[str, object]:
    return {
        "commercial_profile": "direct_payg",
        "billing_currency": "USD",
        "effective_start_date": "2020-01-01",
        "effective_end_date": "2030-01-01",
    }


def _cost_response(**overrides: object) -> httpx.Response:
    item: dict[str, object] = {
        "id": "cost-1",
        "start_date": "2026-07-01",
        "end_date": "2026-07-02",
        "granularity": "DAILY",
        "product": "KAFKA",
        "line_type": "KAFKA_STORAGE",
        "amount": "8",
        "original_amount": "10",
        "discount_amount": "2",
        "price": "2",
        "quantity": "5",
        "unit": "GB",
        "description": "Kafka storage usage",
        "network_access_type": "PUBLIC_INTERNET",
        "resource": {
            "id": "lkc-1",
            "display_name": "Orders",
            "environment": {"id": "env-1"},
        },
        "tier_dimensions": {"tier": "standard"},
    }
    item.update(overrides)
    return httpx.Response(
        200,
        json={
            "data": [item],
            "metadata": {},
        },
    )


class ReplacementCostInput:
    """Bounded test collector that exercises the ordinary GatherPhase replacement path."""

    def __init__(self, tracking_date: date) -> None:
        self._tracking_date = tracking_date

    def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[CCloudBillingLineItem]:
        del start, end
        window_start = datetime.combine(self._tracking_date, datetime.min.time(), tzinfo=UTC)
        window_end = window_start + timedelta(days=1)
        uow.billing.replace_source_window(
            "confluent_cloud",
            tenant_id,
            window_start,
            window_end,
            [
                CCloudCostSourceRecord(
                    ecosystem="confluent_cloud",
                    tenant_id=tenant_id,
                    source_record_id="provider:replacement-cost",
                    identity_scheme="provider_cost_id",
                    provider_cost_id="replacement-cost",
                    source_period_start=window_start,
                    source_period_end=window_end,
                    collection_window_start=window_start,
                    collection_window_end=window_end,
                    evidence_scope_start=window_start,
                    evidence_scope_end=window_end,
                    allocation_timestamp=window_start,
                    retention_timestamp=window_start,
                    granularity="DAILY",
                    product="KAFKA",
                    line_type="KAFKA_STORAGE",
                    amount=Decimal("8"),
                    original_amount=Decimal("10"),
                    discount_amount=Decimal("2"),
                    price=Decimal("2"),
                    quantity=Decimal("5"),
                    unit="GB",
                    description="Kafka storage usage",
                    network_access_type="PUBLIC_INTERNET",
                    resource_id="lkc-1",
                    resource_name="Orders",
                    environment_id="env-1",
                    tier_dimensions={"tier": "standard"},
                    malformed=False,
                    diagnostics=(),
                    raw_payload={"id": "replacement-cost"},
                )
            ],
        )
        yield CCloudBillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id=tenant_id,
            timestamp=window_start,
            env_id="env-1",
            resource_id="lkc-1",
            product_category="KAFKA",
            product_type="KAFKA_STORAGE",
            quantity=Decimal("5"),
            unit_price=Decimal("2"),
            total_cost=Decimal("8"),
            currency="USD",
            granularity="daily",
            metadata={},
        )


class PipelineApiClient:
    def __init__(self, app: object, *, use_lifespan: bool = False) -> None:
        self._app = app
        self._loop = asyncio.new_event_loop()
        self._lifespan: object | None = None
        if use_lifespan:
            self._lifespan = app.router.lifespan_context(app)  # type: ignore[attr-defined]
            self._loop.run_until_complete(self._lifespan.__aenter__())  # type: ignore[attr-defined]
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),  # type: ignore[arg-type]
            base_url="http://testserver",
        )
        self._loop.run_until_complete(self._client.__aenter__())

    def get(self, url: str) -> httpx.Response:
        return self._loop.run_until_complete(self._client.get(url))

    def post(self, url: str, **kwargs: object) -> httpx.Response:
        return self._loop.run_until_complete(self._client.post(url, **kwargs))  # type: ignore[arg-type]

    def close(self) -> None:
        self._loop.run_until_complete(self._client.__aexit__(None, None, None))
        if self._lifespan is not None:
            self._loop.run_until_complete(self._lifespan.__aexit__(None, None, None))  # type: ignore[attr-defined]
        self._loop.close()


def _request(client: PipelineApiClient, start: date, end: date) -> dict[str, Any]:
    submitted = client.post(
        "/api/v1/tenants/production/focus-preview/requests",
        json={
            "grain": "daily",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "column_profile": "full",
        },
    )
    assert submitted.status_code == 202
    request_id = submitted.json()["request_id"]
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
        assert response.status_code == 200
        if response.json()["status"] in {"ready", "failed"}:
            return response.json()
        time.sleep(0.01)
    raise AssertionError("preview request did not finish")


def _legacy_july_first_snapshot(engine: object) -> dict[str, tuple[object, ...]]:
    statements = {
        "pipeline_state": "SELECT * FROM pipeline_state WHERE tracking_date = '2026-07-01'",
        "ccloud_billing": "SELECT * FROM ccloud_billing WHERE timestamp = '2026-07-01 00:00:00'",
        "chargeback_dimensions": "SELECT * FROM chargeback_dimensions WHERE dimension_id = 41",
        "chargeback_facts": (
            "SELECT * FROM chargeback_facts WHERE dimension_id = 41 AND timestamp = '2026-07-01 00:00:00'"
        ),
        "topic_attribution_dimensions": "SELECT * FROM topic_attribution_dimensions WHERE dimension_id = 51",
        "topic_attribution_facts": (
            "SELECT * FROM topic_attribution_facts WHERE dimension_id = 51 AND timestamp = '2026-07-01 00:00:00'"
        ),
    }
    with engine.connect() as connection:  # type: ignore[union-attr]
        return {name: tuple(connection.execute(text(statement)).one()) for name, statement in statements.items()}


@respx.mock
def test_workflow_runner_provider_calculation_to_preview_mixed_retry_and_unrelated_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def to_thread_inline(function: Any, *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Any, *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", to_thread_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)
    monkeypatch.setattr("workflow_runner.cleanup_orphaned_runs_for_all_tenants", lambda *_args, **_kwargs: None)
    route = respx.get("https://api.confluent.cloud/billing/v1/costs")

    def provider_response(request: httpx.Request) -> httpx.Response:
        start = date.fromisoformat(request.url.params["start_date"])
        end = date.fromisoformat(request.url.params["end_date"])
        return (
            _cost_response()
            if start <= date(2026, 7, 1) and end >= date(2026, 7, 2)
            else httpx.Response(200, json={"data": [], "metadata": {}})
        )

    route.side_effect = provider_response
    connection_string = f"sqlite:///{tmp_path / 'pipeline.db'}"
    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=31,
        cutoff_days=5,
        storage=StorageConfig(connection_string=connection_string),
        focus_preview=_focus_preview_block(),
        plugin_settings={
            "ccloud_api": {"key": "key", "secret": "secret"},  # pragma: allowlist secret
            "billing_api": {"days_per_query": 30},
            "min_refresh_gap_seconds": 3600,
        },
    )
    settings = AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        preview=PreviewConfig(artifact_root=tmp_path / "artifacts", max_workers=1),
        tenants={"production": tenant},
    )
    handler = PreviewPipelineHandler()
    handler.failing_dates = {date(2026, 7, 2), date(2026, 7, 3)}
    plugin = PreviewPipelinePlugin(handler)
    plugin.initialize(tenant.plugin_settings.model_dump())
    backend = SQLModelBackend(connection_string, plugin.get_storage_module(), use_migrations=False)
    backend.create_tables()
    with backend.create_unit_of_work() as uow:
        for tracking_date in (date(2026, 7, 2), date(2026, 7, 3)):
            uow.billing.upsert(
                CCloudBillingLineItem(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    timestamp=datetime.combine(tracking_date, datetime.min.time(), tzinfo=UTC),
                    env_id="env-1",
                    resource_id="lkc-1",
                    product_category="KAFKA",
                    product_type="KAFKA_STORAGE",
                    quantity=Decimal("5"),
                    unit_price=Decimal("2"),
                    total_cost=Decimal("8"),
                    currency="USD",
                    granularity="daily",
                    metadata={},
                )
            )
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    tracking_date=tracking_date,
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=False,
                )
            )
        uow.commit()
    orchestrator = ChargebackOrchestrator("production", tenant, plugin, backend)
    runner = WorkflowRunner(settings, MagicMock())
    runner._bootstrapped = True
    runner._tenant_runtimes["production"] = TenantRuntime(
        tenant_name="production",
        plugin=plugin,
        storage=backend,
        orchestrator=orchestrator,
        config_hash=_config_hash(tenant),
        created_at=datetime.now(UTC),
    )
    app = create_app(settings)
    client = PipelineApiClient(app, use_lifespan=True)
    try:
        recoverable_before = _request(client, date(2026, 7, 1), date(2026, 7, 2))
        assert recoverable_before["status"] == "failed"
        assert recoverable_before["diagnostic"] == {
            "code": "calculation_unavailable",
            "message": (
                "No successful persisted calculation is available for the requested dates; run the pipeline and retry."
            ),
            "retryable": True,
        }

        first = runner.run_tenant("production")
        assert first.dates_calculated == 1
        assert len(first.errors) == 2
        with backend.create_read_only_unit_of_work() as uow:
            failed_run = uow.pipeline_runs.get_latest_run("production")
            committed_a = uow.pipeline_state.get("confluent_cloud", "tenant-1", date(2026, 7, 1))
        assert failed_run is not None
        assert failed_run.status == "failed"
        assert failed_run.ended_at is not None
        assert committed_a is not None
        assert committed_a.has_usable_calculation is True
        assert committed_a.calculation_run_id == failed_run.id
        provider_call_count = len(respx.calls)
        route.side_effect = AssertionError("provider access is disabled during Preview")

        a_ready = _request(client, date(2026, 7, 1), date(2026, 7, 2))
        assert a_ready["status"] == "ready"
        assert len(respx.calls) == provider_call_count
        a_entry = a_ready["source_snapshot"]["calculation_coverage"][0]
        assert a_entry["calculation_run_id"] is not None
        assert a_ready["source_snapshot"]["source_through"]
        manifest_response = client.get(a_ready["package"]["manifest"]["download_url"])
        csv_response = client.get(a_ready["package"]["files"][0]["download_url"])
        assert manifest_response.status_code == 200
        assert csv_response.status_code == 200
        manifest = manifest_response.json()
        assert manifest["source_snapshot"]["source_through"] == a_ready["source_snapshot"]["source_through"]
        assert manifest["known_gaps"]
        assert manifest["conformance_status"] == "non_conforming"
        row = next(csv.DictReader(io.StringIO(csv_response.text)))
        assert row["BillingCurrency"] == ""
        assert row["BillingPeriodStart"] == ""
        assert row["BillingPeriodEnd"] == ""

        abc_failed = _request(client, date(2026, 7, 1), date(2026, 7, 4))
        assert abc_failed["status"] == "failed"
        assert abc_failed["diagnostic"] == {
            "code": "calculation_coverage_incomplete",
            "message": "No successful persisted calculation covers every requested date; run the pipeline and retry.",
            "retryable": True,
        }
        assert len(respx.calls) == provider_call_count
        handler.failing_dates.remove(date(2026, 7, 2))
        from core.storage.backends.sqlmodel import repositories

        original_update = repositories.SQLModelPipelineRunRepository.update_run

        def fail_failed_finalization(repository: Any, pipeline_run: Any) -> object:
            if pipeline_run.status == "failed":
                raise RuntimeError("intentional finalization persistence failure")
            return original_update(repository, pipeline_run)

        with patch.object(
            repositories.SQLModelPipelineRunRepository,
            "update_run",
            fail_failed_finalization,
        ):
            second = runner.run_tenant("production")
        assert second.dates_calculated == 1
        assert len(second.errors) == 1
        with backend.create_read_only_unit_of_work() as uow:
            running_run = uow.pipeline_runs.get_latest_run("production")
            committed_b = uow.pipeline_state.get("confluent_cloud", "tenant-1", date(2026, 7, 2))
        assert running_run is not None
        assert running_run.status == "running"
        assert committed_b is not None
        assert committed_b.has_usable_calculation is True
        assert committed_b.calculation_run_id == running_run.id
        ab_failed = _request(client, date(2026, 7, 1), date(2026, 7, 3))
        assert ab_failed["status"] == "failed"
        assert ab_failed["diagnostic"]["code"] == "preview_source_coverage_incomplete"
        assert len(respx.calls) == provider_call_count

        handler.failing_dates.clear()
        third = runner.run_tenant("production")
        assert third.errors == []
        abc_failed = _request(client, date(2026, 7, 1), date(2026, 7, 4))
        assert abc_failed["status"] == "failed"
        assert abc_failed["diagnostic"]["code"] == "preview_source_coverage_incomplete"
        assert len(respx.calls) == provider_call_count
    finally:
        client.close()
        runner.close()


@pytest.mark.parametrize(
    "provider_overrides",
    [
        {
            "amount": "0",
            "original_amount": "0",
            "discount_amount": "0",
            "price": "0",
            "quantity": "0",
        },
        {"amount": "-8", "original_amount": "-10", "discount_amount": "-2", "price": "-2"},
        {"line_type": "PROMO_CREDIT"},
        {"description": "Refund adjustment for prior period"},
    ],
)
@respx.mock
def test_provider_backed_unsupported_economics_and_semantics_fail_before_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider_overrides: dict[str, object],
) -> None:
    async def to_thread_inline(function: Any, *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Any, *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", to_thread_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)
    monkeypatch.setattr("workflow_runner.cleanup_orphaned_runs_for_all_tenants", lambda *_args, **_kwargs: None)
    route = respx.get("https://api.confluent.cloud/billing/v1/costs")

    def provider_response(request: httpx.Request) -> httpx.Response:
        start = date.fromisoformat(request.url.params["start_date"])
        end = date.fromisoformat(request.url.params["end_date"])
        return (
            _cost_response(**provider_overrides)
            if start <= date(2026, 7, 1) and end >= date(2026, 7, 2)
            else httpx.Response(200, json={"data": [], "metadata": {}})
        )

    route.side_effect = provider_response
    connection_string = f"sqlite:///{tmp_path / 'provider-negative.db'}"
    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=31,
        cutoff_days=5,
        storage=StorageConfig(connection_string=connection_string),
        focus_preview=_focus_preview_block(),
        plugin_settings={
            "ccloud_api": {"key": "key", "secret": "secret"},  # pragma: allowlist secret
            "billing_api": {"days_per_query": 30},
            "min_refresh_gap_seconds": 0,
        },
    )
    settings = AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        preview=PreviewConfig(artifact_root=tmp_path / "provider-negative-artifacts", max_workers=1),
        tenants={"production": tenant},
    )
    handler = PreviewPipelineHandler()
    handler.handles_product_types = ("KAFKA_STORAGE", "PROMO_CREDIT")
    plugin = PreviewPipelinePlugin(handler)
    plugin.initialize(tenant.plugin_settings.model_dump())
    backend = SQLModelBackend(connection_string, plugin.get_storage_module(), use_migrations=False)
    backend.create_tables()
    orchestrator = ChargebackOrchestrator("production", tenant, plugin, backend)
    runner = WorkflowRunner(settings, MagicMock())
    runner._bootstrapped = True
    runner._tenant_runtimes["production"] = TenantRuntime(
        tenant_name="production",
        plugin=plugin,
        storage=backend,
        orchestrator=orchestrator,
        config_hash=_config_hash(tenant),
        created_at=datetime.now(UTC),
    )
    app = create_app(settings)
    client = PipelineApiClient(app, use_lifespan=True)
    try:
        result = runner.run_tenant("production")
        assert result.errors == []
        assert result.dates_calculated == 1
        with backend.create_read_only_unit_of_work() as uow:
            pipeline_run = uow.pipeline_runs.get_latest_run("production")
            state = uow.pipeline_state.get("confluent_cloud", "tenant-1", date(2026, 7, 1))
        assert pipeline_run is not None
        assert pipeline_run.status == "completed"
        assert pipeline_run.id is not None
        assert state is not None
        assert state.has_usable_calculation is True
        assert state.calculation_run_id == pipeline_run.id
        if provider_overrides.get("price") == "0":
            scope = PreviewEvidenceScope(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                start=datetime(2026, 7, 1, tzinfo=UTC),
                end=datetime(2026, 7, 2, tzinfo=UTC),
            )
            with backend.create_preview_read_unit_of_work() as uow:
                sources = uow.cost_evidence.find_preview_source_candidates(scope)
            assert len(sources) == 1
            zero_source = sources[0]
            assert zero_source.price == Decimal("0")
            assert zero_source.quantity == Decimal("0")
            assert zero_source.original_amount == Decimal("0")
            assert zero_source.discount_amount == Decimal("0")
            assert zero_source.amount == Decimal("0")
            assert zero_source.price * zero_source.quantity == zero_source.original_amount
            assert zero_source.original_amount - zero_source.discount_amount == zero_source.amount
        provider_calls = len(respx.calls)
        route.side_effect = AssertionError("provider access is disabled during Preview")

        failed = _request(client, date(2026, 7, 1), date(2026, 7, 2))
        assert failed["status"] == "failed"
        diagnostic = failed["diagnostic"]
        if provider_overrides.get("line_type") == "PROMO_CREDIT" or "description" in provider_overrides:
            assert diagnostic["code"] == "preview_charge_classification_ambiguous"
        else:
            assert diagnostic["code"] == "preview_source_economics_unsupported"
        assert diagnostic["retryable"] is False
        assert len(diagnostic["source_correlation_ids"]) == 1
        assert diagnostic["source_correlation_ids"][0].startswith("src:v1:")
        assert failed["source_snapshot"] is None
        assert failed["package"] is None
        assert list((tmp_path / "provider-negative-artifacts").iterdir()) == []
        assert len(respx.calls) == provider_calls
    finally:
        client.close()
        runner.close()


@respx.mock
def test_migrated_legacy_metadata_failure_preserves_data_when_provider_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def to_thread_inline(function: Any, *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Any, *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", to_thread_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)
    monkeypatch.setattr("workflow_runner.cleanup_orphaned_runs_for_all_tenants", lambda *_args, **_kwargs: None)
    connection_string = f"sqlite:///{tmp_path / 'legacy.db'}"
    migration = _alembic_config(connection_string)
    command.upgrade(migration, "018")
    _seed_legacy_rows(connection_string)
    command.upgrade(migration, "020")

    route = respx.get("https://api.confluent.cloud/billing/v1/costs")
    route.mock(return_value=httpx.Response(200, json={"data": [], "metadata": {}}))
    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=31,
        cutoff_days=30,
        storage=StorageConfig(connection_string=connection_string),
        focus_preview=_focus_preview_block(),
        plugin_settings={
            "ccloud_api": {"key": "key", "secret": "secret"},  # pragma: allowlist secret
            "billing_api": {"days_per_query": 30},
            "min_refresh_gap_seconds": 0,
        },
    )
    settings = AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        preview=PreviewConfig(artifact_root=tmp_path / "legacy-artifacts", max_workers=1),
        tenants={"production": tenant},
    )
    handler = PreviewPipelineHandler()
    plugin = PreviewPipelinePlugin(handler)
    plugin.initialize(tenant.plugin_settings.model_dump())
    backend = SQLModelBackend(connection_string, plugin.get_storage_module(), use_migrations=False)
    snapshot_engine = create_engine(connection_string)
    before_unavailable_run = _snapshots(snapshot_engine)
    orchestrator = ChargebackOrchestrator("production", tenant, plugin, backend)
    runner = WorkflowRunner(settings, MagicMock())
    runner._bootstrapped = True
    runner._tenant_runtimes["production"] = TenantRuntime(
        tenant_name="production",
        plugin=plugin,
        storage=backend,
        orchestrator=orchestrator,
        config_hash=_config_hash(tenant),
        created_at=datetime.now(UTC),
    )
    app = create_app(settings)
    client = PipelineApiClient(app, use_lifespan=True)
    try:
        unavailable_result = runner.run_tenant("production")
        assert unavailable_result.errors == []
        assert unavailable_result.dates_calculated == 0
        after_unavailable_run = _snapshots(snapshot_engine)
        assert after_unavailable_run == before_unavailable_run
        provider_calls = len(respx.calls)
        route.side_effect = AssertionError("provider access is disabled during Preview")

        failed = _request(client, date(2026, 7, 1), date(2026, 7, 2))
        assert failed["status"] == "failed"
        assert failed["diagnostic"] == {
            "code": "calculation_metadata_unavailable",
            "message": "One or more requested dates lack preview calculation metadata.",
            "retryable": False,
        }
        assert failed["source_snapshot"] is None
        assert failed["package"] is None
        assert len(respx.calls) == provider_calls
        assert list((tmp_path / "legacy-artifacts").iterdir()) == []
        assert _snapshots(snapshot_engine) == before_unavailable_run

    finally:
        client.close()
        runner.close()
        snapshot_engine.dispose()


@pytest.mark.parametrize("recoverable_succeeds", [False, True])
@respx.mock
def test_migrated_legacy_precedence_uses_real_recoverable_lifecycle_without_mutating_legacy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recoverable_succeeds: bool,
) -> None:
    async def to_thread_inline(function: Any, *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Any, *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", to_thread_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)
    monkeypatch.setattr("workflow_runner.cleanup_orphaned_runs_for_all_tenants", lambda *_args, **_kwargs: None)
    connection_string = f"sqlite:///{tmp_path / 'legacy-precedence.db'}"
    migration = _alembic_config(connection_string)
    command.upgrade(migration, "018")
    _seed_legacy_rows(connection_string)
    command.upgrade(migration, "020")
    snapshot_engine = create_engine(connection_string)
    legacy_before = _legacy_july_first_snapshot(snapshot_engine)

    route = respx.get("https://api.confluent.cloud/billing/v1/costs")

    def provider_response(request: httpx.Request) -> httpx.Response:
        start = date.fromisoformat(request.url.params["start_date"])
        end = date.fromisoformat(request.url.params["end_date"])
        return (
            _cost_response(id="cost-2", start_date="2026-07-02", end_date="2026-07-03")
            if start <= date(2026, 7, 2) and end >= date(2026, 7, 3)
            else httpx.Response(200, json={"data": [], "metadata": {}})
        )

    route.side_effect = provider_response
    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=31,
        cutoff_days=5,
        storage=StorageConfig(connection_string=connection_string),
        focus_preview=_focus_preview_block(),
        plugin_settings={
            "ccloud_api": {"key": "key", "secret": "secret"},  # pragma: allowlist secret
            "billing_api": {"days_per_query": 30},
            "min_refresh_gap_seconds": 0,
        },
    )
    settings = AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        preview=PreviewConfig(artifact_root=tmp_path / "legacy-precedence-artifacts", max_workers=1),
        tenants={"production": tenant},
    )
    handler = PreviewPipelineHandler()
    if not recoverable_succeeds:
        handler.failing_dates = {date(2026, 7, 2)}
    plugin = PreviewPipelinePlugin(handler)
    plugin.initialize(tenant.plugin_settings.model_dump())
    backend = SQLModelBackend(connection_string, plugin.get_storage_module(), use_migrations=False)
    orchestrator = ChargebackOrchestrator("production", tenant, plugin, backend)
    runner = WorkflowRunner(settings, MagicMock())
    runner._bootstrapped = True
    runner._tenant_runtimes["production"] = TenantRuntime(
        tenant_name="production",
        plugin=plugin,
        storage=backend,
        orchestrator=orchestrator,
        config_hash=_config_hash(tenant),
        created_at=datetime.now(UTC),
    )
    app = create_app(settings)
    client = PipelineApiClient(app, use_lifespan=True)
    try:
        result = runner.run_tenant("production")
        assert result.dates_calculated == (1 if recoverable_succeeds else 0)
        assert len(result.errors) == (0 if recoverable_succeeds else 1)
        with backend.create_read_only_unit_of_work() as uow:
            legacy = uow.pipeline_state.get("confluent_cloud", "tenant-1", date(2026, 7, 1))
            recoverable = uow.pipeline_state.get("confluent_cloud", "tenant-1", date(2026, 7, 2))
        assert legacy is not None
        assert legacy.chargeback_calculated is True
        assert legacy.has_usable_calculation is False
        assert recoverable is not None
        assert recoverable.has_usable_calculation is recoverable_succeeds
        assert _legacy_july_first_snapshot(snapshot_engine) == legacy_before
        after_lifecycle = _snapshots(snapshot_engine)
        provider_calls = len(respx.calls)
        route.side_effect = AssertionError("provider access is disabled during Preview")

        requested_ends = (date(2026, 7, 3), date(2026, 7, 4)) if recoverable_succeeds else (date(2026, 7, 3),)
        for end_date in requested_ends:
            failed = _request(client, date(2026, 7, 1), end_date)
            assert failed["status"] == "failed"
            assert failed["diagnostic"] == {
                "code": "calculation_metadata_unavailable",
                "message": "One or more requested dates lack preview calculation metadata.",
                "retryable": False,
            }
            assert failed["source_snapshot"] is None
            assert failed["package"] is None
        assert len(respx.calls) == provider_calls
        assert _snapshots(snapshot_engine) == after_lifecycle
        assert _legacy_july_first_snapshot(snapshot_engine) == legacy_before
    finally:
        client.close()
        runner.close()
        snapshot_engine.dispose()


def test_ordinary_gather_and_calculate_lifecycle_replaces_incomplete_legacy_correlation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def to_thread_inline(function: Any, *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Any, *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", to_thread_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)
    monkeypatch.setattr("workflow_runner.cleanup_orphaned_runs_for_all_tenants", lambda *_args, **_kwargs: None)
    tracking_date = (datetime.now(UTC) - timedelta(days=2)).date()
    tracking_start = datetime.combine(tracking_date, datetime.min.time(), tzinfo=UTC)
    connection_string = f"sqlite:///{tmp_path / 'replacement.db'}"
    migration = _alembic_config(connection_string)
    command.upgrade(migration, "018")
    _seed_legacy_rows(connection_string)
    legacy_engine = create_engine(connection_string)
    tracking_iso = tracking_date.isoformat()
    tracking_timestamp = f"{tracking_iso} 00:00:00"
    with legacy_engine.begin() as connection:
        connection.execute(
            text("UPDATE pipeline_state SET tracking_date = :tracking_date WHERE tracking_date = '2026-07-01'"),
            {"tracking_date": tracking_iso},
        )
        for table in ("ccloud_billing", "chargeback_facts", "topic_attribution_facts"):
            connection.execute(
                text(f"UPDATE {table} SET timestamp = :tracking_timestamp WHERE timestamp = '2026-07-01 00:00:00'"),
                {"tracking_timestamp": tracking_timestamp},
            )
    legacy_engine.dispose()
    command.upgrade(migration, "020")
    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=31,
        cutoff_days=5,
        storage=StorageConfig(connection_string=connection_string),
        focus_preview=_focus_preview_block(),
        plugin_settings={
            "ccloud_api": {"key": "key", "secret": "secret"},  # pragma: allowlist secret
            "billing_api": {"days_per_query": 30},
            "min_refresh_gap_seconds": 0,
        },
    )
    settings = AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        preview=PreviewConfig(artifact_root=tmp_path / "replacement-artifacts", max_workers=1),
        tenants={"production": tenant},
    )
    handler = PreviewPipelineHandler()
    plugin = PreviewPipelinePlugin(handler)
    plugin.initialize(tenant.plugin_settings.model_dump())
    plugin.cost_input_override = ReplacementCostInput(tracking_date)
    backend = SQLModelBackend(connection_string, plugin.get_storage_module(), use_migrations=False)

    orchestrator = ChargebackOrchestrator("production", tenant, plugin, backend)
    runner = WorkflowRunner(settings, MagicMock())
    runner._bootstrapped = True
    runner._tenant_runtimes["production"] = TenantRuntime(
        tenant_name="production",
        plugin=plugin,
        storage=backend,
        orchestrator=orchestrator,
        config_hash=_config_hash(tenant),
        created_at=datetime.now(UTC),
    )
    app = create_app(settings)
    client = PipelineApiClient(app, use_lifespan=True)
    try:
        result = runner.run_tenant("production")
        assert result.errors == []
        assert result.dates_calculated == 1
        with backend.create_read_only_unit_of_work() as uow:
            replaced = uow.pipeline_state.get("confluent_cloud", "tenant-1", tracking_date)
            chargebacks, total = uow.chargebacks.find_by_filters(
                "confluent_cloud",
                "tenant-1",
                start=tracking_start,
                end=tracking_start + timedelta(days=1),
            )
        assert replaced is not None
        assert replaced.has_usable_calculation is True
        assert replaced.calculation_id
        assert replaced.calculation_completed_at is not None
        assert replaced.calculation_run_id is not None
        assert total == 1
        assert [row.amount for row in chargebacks] == [Decimal("8")]

        ready = _request(client, tracking_date, tracking_date + timedelta(days=1))
        assert ready["status"] == "ready"
        assert ready["source_snapshot"]["calculation_coverage"] == [
            {
                "tracking_date": tracking_date.isoformat(),
                "calculation_id": replaced.calculation_id,
                "calculation_completed_at": replaced.calculation_completed_at.isoformat().replace("+00:00", "Z"),
                "calculation_run_id": replaced.calculation_run_id,
            }
        ]
    finally:
        client.close()
        runner.close()

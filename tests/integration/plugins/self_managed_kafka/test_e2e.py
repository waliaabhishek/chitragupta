"""Cross-component tests for self-managed Kafka telemetry wiring."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from core.models import CostType, MetricRow

if TYPE_CHECKING:
    import pytest


def _settings() -> dict[str, object]:
    return {
        "cluster_id": "billing-cluster-a",
        "metrics_identifier": "kraft-a-001",
        "metrics_identifier_label": "kafka_cluster_id",
        "broker_count": 3,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "identity_source": {
            "source": "static",
            "static_identities": [{"identity_id": "team-data", "identity_type": "team"}],
        },
        "metrics": {"url": "http://prometheus:9090"},
    }


def _cost_metrics() -> dict[str, list[MetricRow]]:
    timestamp = datetime(2026, 8, 1, tzinfo=UTC)
    return {
        "cluster_bytes_in": [MetricRow(timestamp, "cluster_bytes_in", 4096.0, {"kafka_cluster_id": "kraft-a-001"})],
        "cluster_bytes_out": [MetricRow(timestamp, "cluster_bytes_out", 2048.0, {"kafka_cluster_id": "kraft-a-001"})],
        "cluster_storage_bytes": [
            MetricRow(timestamp, "cluster_storage_bytes", 8192.0, {"kafka_cluster_id": "kraft-a-001"})
        ],
    }


def test_plugin_initialization_produces_cluster_scoped_cost_construction() -> None:
    from core.plugin.protocols import ScopeGatePlugin
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source = MagicMock()
    source.query.return_value = _cost_metrics()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(_settings())

    cost_input = plugin.get_cost_input()
    lines = list(
        cost_input.gather(
            "tenant-1",
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
            MagicMock(),
        )
    )

    assert {line.resource_id for line in lines} == {"billing-cluster-a"}
    _, kwargs = source.query.call_args
    assert kwargs["resource_id_filter"] == "kraft-a-001"
    assert all(query.resource_label == "kafka_cluster_id" for query in kwargs["queries"])
    assert isinstance(plugin, ScopeGatePlugin)


def test_orchestrator_persists_open_probe_recovery_and_close_with_real_scope_storage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.config.models import PluginSettingsBase, TenantConfig
    from core.engine.orchestrator import ChargebackOrchestrator
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

    source = MagicMock()
    target_state = "down"

    def target_rows(query: object, kwargs: dict[str, object]) -> list[MetricRow]:
        start = kwargs["start"]
        end = kwargs["end"]
        step = kwargs["step"]
        assert isinstance(start, datetime)
        assert isinstance(end, datetime)
        assert isinstance(step, timedelta)
        value = 0.0 if target_state == "down" else 1.0
        rows: list[MetricRow] = []
        timestamp = start
        while timestamp < end:
            rows.append(MetricRow(timestamp, query.key, value, {"kafka_cluster_id": "kraft-a-001"}))
            timestamp += step
        rows.append(MetricRow(end, query.key, value, {"kafka_cluster_id": "kraft-a-001"}))
        return rows

    def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
        key = queries[0].key
        if key == "target_up":
            return {key: target_rows(queries[0], kwargs)}
        if key == "broker_topic_discovery":
            return {key: []}
        timestamp = kwargs["start"]
        assert isinstance(timestamp, datetime)
        return {
            "cluster_bytes_in": [MetricRow(timestamp, "cluster_bytes_in", 4096.0, {})],
            "cluster_bytes_out": [MetricRow(timestamp, "cluster_bytes_out", 2048.0, {})],
            "cluster_storage_bytes": [MetricRow(timestamp, "cluster_storage_bytes", 8192.0, {})],
        }

    source.query.side_effect = query
    settings = _settings()
    settings["identity_source"] = {
        "source": "static",
        "static_identities": [{"identity_id": "team-data", "identity_type": "team"}],
    }
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(settings)
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'scope-lifecycle.db'}",
        SelfManagedKafkaStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    tenant = TenantConfig(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        lookback_days=6,
        cutoff_days=5,
        plugin_settings=PluginSettingsBase.model_validate(settings),
    )
    orchestrator = ChargebackOrchestrator("tenant", tenant, plugin, backend, source)

    def run_at(now: datetime):
        with patch("core.engine.orchestrator.datetime") as patched_datetime:
            patched_datetime.now.return_value = now
            patched_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            return orchestrator.run()

    first = run_at(datetime(2026, 8, 20, tzinfo=UTC))
    assert first.dates_gathered == 0
    assert first.dates_pending_calculation == 0
    assert source.query.call_count == 1
    assert [query.key for query in source.query.call_args.kwargs["queries"]] == ["target_up"]
    with backend.create_unit_of_work() as uow:
        opened = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "billing-cluster-a")
        assert opened is not None
        assert opened.status == "open"
        assert opened.last_failure_status == "target_down"
        opened_start = opened.first_blocked_window_start

    source.query.reset_mock()
    probe = run_at(datetime(2026, 8, 21, tzinfo=UTC))
    assert probe.dates_gathered == 0
    assert source.query.call_count == 1
    assert [query.key for query in source.query.call_args.kwargs["queries"]] == ["target_up"]
    with backend.create_unit_of_work() as uow:
        probed = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "billing-cluster-a")
        assert probed is not None
        assert probed.status == "open"
        assert probed.first_blocked_window_start == opened_start
        assert probed.last_probe_status == "target_down"

    target_state = "up"
    recovered = run_at(datetime(2026, 9, 20, tzinfo=UTC))
    assert recovered.errors == []
    assert recovered.dates_gathered == 1
    assert recovered.dates_calculated == 1
    with backend.create_unit_of_work() as uow:
        state = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "billing-cluster-a")
        assert state is not None
        assert state.status == "closed"
        assert state.retention_gap_start == opened_start
        assert state.retention_gap_end == datetime(2026, 9, 14, tzinfo=UTC)
        assert state.recovery_cursor_date == datetime(2026, 9, 15, tzinfo=UTC).date()
        rows = uow.chargebacks.find_by_date("self_managed_kafka", "tenant-1", datetime(2026, 9, 14).date())
        assert rows
        assert {row.cost_type for row in rows} == {CostType.SHARED}
        assert {row.allocation_detail for row in rows} == {"even_split_allocation"}

    tracking_date = datetime(2026, 9, 14).date()
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.delete_by_date("self_managed_kafka", "tenant-1", tracking_date)
        uow.pipeline_state.mark_needs_recalculation("self_managed_kafka", "tenant-1", tracking_date)
        uow.commit()

    from core.plugin.protocols import ScopeBlockedError, ScopeGateDecision, ScopeGateResult
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelUnitOfWork

    original_commit = SQLModelUnitOfWork.commit
    fail_calculation_commit = True

    def flush_then_block_commit(unit_of_work: SQLModelUnitOfWork) -> None:
        nonlocal fail_calculation_commit
        if fail_calculation_commit:
            fail_calculation_commit = False
            assert unit_of_work._session is not None
            unit_of_work._session.flush()
            raise ScopeBlockedError(
                ScopeGateResult(ScopeGateDecision.BLOCKED, "billing-cluster-a", "forced post-write scope block")
            )
        original_commit(unit_of_work)

    monkeypatch.setattr(SQLModelUnitOfWork, "commit", flush_then_block_commit)
    failed = run_at(datetime(2026, 9, 20, tzinfo=UTC))

    assert failed.dates_calculated == 0
    assert any("forced post-write scope block" in error for error in failed.errors)
    with backend.create_unit_of_work() as uow:
        state = uow.pipeline_state.get("self_managed_kafka", "tenant-1", tracking_date)
        breaker = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "billing-cluster-a")
        assert state is not None
        assert state.chargeback_calculated is False
        assert uow.chargebacks.find_by_date("self_managed_kafka", "tenant-1", tracking_date) == []
        assert breaker is not None
        assert breaker.status == "open"
    backend.dispose()


def test_orchestrator_rolls_back_gather_writes_when_second_scope_check_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.config.models import PluginSettingsBase, TenantConfig
    from core.engine.orchestrator import ChargebackOrchestrator
    from core.models import Resource
    from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

    source = MagicMock()
    target_values: list[float] = []
    query_keys: list[str] = []

    def target_rows(query: object, kwargs: dict[str, object], value: float) -> list[MetricRow]:
        start = kwargs["start"]
        end = kwargs["end"]
        step = kwargs["step"]
        assert isinstance(start, datetime)
        assert isinstance(end, datetime)
        assert isinstance(step, timedelta)
        rows: list[MetricRow] = []
        timestamp = start
        while timestamp < end:
            rows.append(MetricRow(timestamp, query.key, value, {"kafka_cluster_id": "kraft-a-001"}))
            timestamp += step
        rows.append(MetricRow(end, query.key, value, {"kafka_cluster_id": "kraft-a-001"}))
        return rows

    def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
        metric_query = queries[0]
        query_keys.append(metric_query.key)
        if metric_query.key == "target_up":
            value = 1.0 if not target_values else 0.0
            target_values.append(value)
            return {metric_query.key: target_rows(metric_query, kwargs, value)}
        if metric_query.key == "broker_topic_discovery":
            return {metric_query.key: []}
        raise AssertionError(f"Billing query ran after scope block: {metric_query.key}")

    source.query.side_effect = query
    resource_writes: list[str] = []
    original_resource_upsert = SQLModelResourceRepository.upsert

    def record_resource_upsert(
        repository: SQLModelResourceRepository,
        resource: Resource,
    ) -> Resource:
        resource_writes.append(resource.resource_id)
        return original_resource_upsert(repository, resource)

    monkeypatch.setattr(SQLModelResourceRepository, "upsert", record_resource_upsert)

    settings = _settings()
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(settings)
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'gather-scope-rollback.db'}",
        SelfManagedKafkaStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    tenant = TenantConfig(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        lookback_days=6,
        cutoff_days=5,
        plugin_settings=PluginSettingsBase.model_validate(settings),
    )
    orchestrator = ChargebackOrchestrator("tenant", tenant, plugin, backend, source)

    with patch("core.engine.orchestrator.datetime") as patched_datetime:
        patched_datetime.now.return_value = datetime(2026, 8, 20, tzinfo=UTC)
        patched_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        result = orchestrator.run()

    tracking_date = datetime(2026, 8, 14).date()
    assert target_values == [1.0, 0.0]
    assert query_keys == ["target_up", "broker_topic_discovery", "target_up"]
    assert resource_writes == ["billing-cluster-a"]
    assert result.dates_gathered == 0
    assert result.dates_calculated == 0
    assert any("target down or unhealthy" in error for error in result.errors)
    with backend.create_unit_of_work() as uow:
        assert uow.resources.get("self_managed_kafka", "tenant-1", "billing-cluster-a") is None
        assert uow.identities.get("self_managed_kafka", "tenant-1", "team-data") is None
        assert uow.billing.find_by_date("self_managed_kafka", "tenant-1", tracking_date) == []
        assert uow.pipeline_state.get("self_managed_kafka", "tenant-1", tracking_date) is None
        breaker = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "billing-cluster-a")
        assert breaker is not None
        assert breaker.status == "open"
    backend.dispose()

"""Cross-component tests for self-managed Kafka telemetry wiring."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs

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
    from core.metrics.protocol import MetricsQueryError
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

    source = MagicMock()
    target_state = "down"
    topic_overlay_available = False

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
        if key.startswith("broker_topic_discovery"):
            timestamp = kwargs["start"]
            assert isinstance(timestamp, datetime)
            return {
                query.key: [
                    MetricRow(
                        timestamp,
                        query.key,
                        1.0,
                        {"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders"},
                    )
                ]
                for query in queries
            }
        if key in {"topic_bytes_in", "topic_bytes_out", "topic_storage_bytes"}:
            if not topic_overlay_available:
                raise MetricsQueryError("topic telemetry is temporarily unavailable")
            timestamp = kwargs["start"]
            assert isinstance(timestamp, datetime)
            return {
                key: [
                    MetricRow(
                        timestamp,
                        key,
                        1073741824.0,
                        {"kafka_cluster_id": "kraft-a-001", "topic": "orders"},
                    )
                ]
            }
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
    settings["topic_attribution"] = {"enabled": True, "compute_policy": "shared_even_v1"}
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
        assert state.status == "retention_gap"
        assert state.retention_gap_start == opened_start
        assert state.retention_gap_end == datetime(2026, 9, 14, tzinfo=UTC)
        assert state.recovery_cursor_date == datetime(2026, 9, 14, tzinfo=UTC).date()
        rows = uow.chargebacks.find_by_date("self_managed_kafka", "tenant-1", datetime(2026, 9, 14).date())
        assert rows
        assert {row.cost_type for row in rows} == {CostType.SHARED}
        assert {row.allocation_detail for row in rows} == {"even_split_allocation"}
        assert uow.topic_attributions.find_by_date("self_managed_kafka", "tenant-1", datetime(2026, 9, 14).date()) == []

    topic_overlay_available = True
    terminal = run_at(datetime(2026, 9, 21, tzinfo=UTC))
    assert terminal.errors == []
    with backend.create_unit_of_work() as uow:
        state = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "billing-cluster-a")
        assert state is not None
        assert state.status == "closed"
        assert state.recovery_cursor_date == datetime(2026, 9, 16, tzinfo=UTC).date()
        assert uow.topic_attributions.find_by_date("self_managed_kafka", "tenant-1", datetime(2026, 9, 14).date())

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
    orchestrator._gather_phase._last_resource_gather_at = datetime(2026, 9, 22, tzinfo=UTC)
    failed = run_at(datetime(2026, 9, 22, tzinfo=UTC))

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
        if metric_query.key.startswith("broker_topic_discovery"):
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


def test_two_initialized_plugins_keep_shared_prometheus_topic_evidence_isolated(tmp_path: Path) -> None:
    import httpx

    from core.config.models import PluginSettingsBase, TenantConfig
    from core.engine.orchestrator import ChargebackOrchestrator
    from core.metrics.prometheus import PrometheusConfig, PrometheusMetricsSource
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

    received_queries: list[str] = []

    def selector_topic(query: str) -> tuple[str, str]:
        if 'kafka_cluster_id="kraft-a-001"' in query:
            return "kraft-a-001", "orders-a"
        if 'kafka_cluster_id="kraft-b-001"' in query:
            return "kraft-b-001", "orders-b"
        raise AssertionError(f"Missing configured cluster selector: {query}")

    def as_datetime(value: str) -> datetime:
        return datetime.fromisoformat(value).astimezone(UTC)

    def prometheus_handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        form = parse_qs(request.content.decode())
        query = form["query"][0]
        received_queries.append(query)
        cluster_id, topic_name = selector_topic(query)
        if "up{" in query:
            start = as_datetime(form["start"][0])
            end = as_datetime(form["end"][0])
            step = timedelta(seconds=int(form["step"][0]))
            values: list[list[float | str]] = []
            timestamp = start
            while timestamp <= end:
                values.append([timestamp.timestamp(), "1"])
                timestamp += step
            result = [
                {
                    "metric": {"kafka_cluster_id": cluster_id},
                    "values": values,
                }
            ]
        else:
            timestamp = as_datetime(form.get("start", form.get("time", form.get("end", [])))[0])
            result = [
                {
                    "metric": {
                        "kafka_cluster_id": cluster_id,
                        "broker": "1",
                        "topic": topic_name,
                    },
                    "value": [timestamp.timestamp(), "1073741824"],
                }
            ]
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "matrix" if "up{" in query else "vector",
                    "result": result,
                },
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(prometheus_handler))
    shared_source = PrometheusMetricsSource(PrometheusConfig(url="http://prometheus.test"), client=client)
    first_settings = _settings()
    first_settings["metrics_step_seconds"] = 86400
    first_settings["topic_attribution"] = {"enabled": True, "compute_policy": "shared_even_v1"}
    second_settings = _settings()
    second_settings["cluster_id"] = "billing-cluster-b"
    second_settings["metrics_identifier"] = "kraft-b-001"
    second_settings["metrics_step_seconds"] = 86400
    second_settings["topic_attribution"] = {"enabled": True, "compute_policy": "shared_even_v1"}

    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=shared_source):
        first_plugin = SelfManagedKafkaPlugin()
        first_plugin.initialize(first_settings)
        second_plugin = SelfManagedKafkaPlugin()
        second_plugin.initialize(second_settings)

    persisted_by_cluster: dict[str, tuple[list[object], list[object], list[object]]] = {}
    for plugin, settings, cluster_id, topic_name in (
        (first_plugin, first_settings, "billing-cluster-a", "orders-a"),
        (second_plugin, second_settings, "billing-cluster-b", "orders-b"),
    ):
        backend = SQLModelBackend(
            f"sqlite:///{tmp_path / f'{cluster_id}.db'}",
            SelfManagedKafkaStorageModule(),
            use_migrations=False,
        )
        backend.create_tables()
        tenant = TenantConfig(
            ecosystem="self_managed_kafka",
            tenant_id="tenant-1",
            lookback_days=2,
            cutoff_days=1,
            plugin_settings=PluginSettingsBase.model_validate(settings),
        )
        orchestrator = ChargebackOrchestrator(
            f"tenant-{cluster_id}",
            tenant,
            plugin,
            backend,
            shared_source,
        )
        with patch("core.engine.orchestrator.datetime") as patched_datetime:
            patched_datetime.now.return_value = datetime(2026, 8, 20, tzinfo=UTC)
            patched_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            result = orchestrator.run()

        assert result.errors == []
        tracking_date = datetime(2026, 8, 18, tzinfo=UTC).date()
        with backend.create_read_only_unit_of_work() as uow:
            billing = uow.billing.find_by_date("self_managed_kafka", "tenant-1", tracking_date)
            resources, _ = uow.resources.find_by_period(
                "self_managed_kafka",
                "tenant-1",
                datetime(2026, 8, 18, tzinfo=UTC),
                datetime(2026, 8, 19, tzinfo=UTC),
                parent_id=cluster_id,
                resource_type="topic",
                count=False,
            )
            topic_rows = uow.topic_attributions.find_by_date("self_managed_kafka", "tenant-1", tracking_date)
            state = uow.pipeline_state.get("self_managed_kafka", "tenant-1", tracking_date)
        assert state is not None
        assert state.topic_attribution_calculated is True
        assert {line.resource_id for line in billing} == {cluster_id}
        assert {resource.display_name for resource in resources} == {topic_name}
        assert {row.cluster_resource_id for row in topic_rows} == {cluster_id}
        assert {row.topic_name for row in topic_rows} == {topic_name}
        assert {row.product_type for row in topic_rows} == {
            "SELF_KAFKA_COMPUTE",
            "SELF_KAFKA_STORAGE",
            "SELF_KAFKA_NETWORK_INGRESS",
            "SELF_KAFKA_NETWORK_EGRESS",
        }
        persisted_by_cluster[cluster_id] = (billing, resources, topic_rows)
        backend.dispose()

    client.close()
    assert received_queries
    assert all(
        'kafka_cluster_id="kraft-a-001"' in query or 'kafka_cluster_id="kraft-b-001"' in query
        for query in received_queries
    )
    assert any('kafka_cluster_id="kraft-a-001"' in query for query in received_queries)
    assert any('kafka_cluster_id="kraft-b-001"' in query for query in received_queries)
    first_billing, first_resources, first_rows = persisted_by_cluster["billing-cluster-a"]
    second_billing, second_resources, second_rows = persisted_by_cluster["billing-cluster-b"]
    assert {line.resource_id for line in first_billing} == {"billing-cluster-a"}
    assert {resource.resource_id for resource in first_resources} == {"billing-cluster-a:topic:orders-a"}
    assert {row.cluster_resource_id for row in first_rows} == {"billing-cluster-a"}
    assert {line.resource_id for line in second_billing} == {"billing-cluster-b"}
    assert {resource.resource_id for resource in second_resources} == {"billing-cluster-b:topic:orders-b"}
    assert {row.cluster_resource_id for row in second_rows} == {"billing-cluster-b"}


def test_configured_plugin_keeps_pending_overlay_when_inventory_is_skipped_or_admin_discovery_fails(
    tmp_path: Path,
) -> None:
    from decimal import Decimal

    from core.config.models import PluginSettingsBase, TenantConfig
    from core.engine.orchestrator import ChargebackOrchestrator
    from core.models import CoreBillingLineItem, CoreResource, PipelineState
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

    source = MagicMock()
    observed_query_keys: list[set[str]] = []
    topic_evidence_available = False

    def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
        keys = {query.key for query in queries}
        observed_query_keys.append(keys)
        timestamp = kwargs["start"]
        assert isinstance(timestamp, datetime)
        if keys == {"target_up"}:
            end = kwargs["end"]
            step = kwargs["step"]
            assert isinstance(end, datetime)
            assert isinstance(step, timedelta)
            target_rows: list[MetricRow] = []
            while timestamp < end:
                target_rows.append(
                    MetricRow(timestamp, "target_up", 1.0, {"kafka_cluster_id": "kraft-a-001", "broker": "1"})
                )
                timestamp += step
            target_rows.append(MetricRow(end, "target_up", 1.0, {"kafka_cluster_id": "kraft-a-001", "broker": "1"}))
            return {"target_up": target_rows}
        if keys == {"cluster_bytes_in", "cluster_bytes_out", "cluster_storage_bytes"}:
            return {
                "cluster_bytes_in": [MetricRow(timestamp, "cluster_bytes_in", 0.0, {})],
                "cluster_bytes_out": [MetricRow(timestamp, "cluster_bytes_out", 0.0, {})],
                "cluster_storage_bytes": [MetricRow(timestamp, "cluster_storage_bytes", 0.0, {})],
            }
        if keys <= {"topic_bytes_in", "topic_bytes_out", "topic_storage_bytes"}:
            if not topic_evidence_available:
                raise AssertionError("Topic overlay queried without current inventory")
            key = next(iter(keys))
            return {
                key: [
                    MetricRow(
                        timestamp,
                        key,
                        536870912.0,
                        {"kafka_cluster_id": "kraft-a-001", "topic": topic_name},
                    )
                    for topic_name in ("created-during-window", "deleted-during-window")
                ]
            }
        raise AssertionError(f"Topic overlay queried without current inventory: {keys}")

    source.query.side_effect = query
    admin_client = MagicMock()
    admin_client.describe_cluster.return_value = {"brokers": [{"node_id": 1}]}
    admin_client.list_topics.side_effect = OSError("admin discovery unavailable")
    settings = _settings()
    settings["resource_source"] = {"source": "admin_api", "bootstrap_servers": "kafka:9092"}
    settings["topic_attribution"] = {"enabled": True, "compute_policy": "shared_even_v1"}
    plugin = SelfManagedKafkaPlugin()
    with (
        patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source),
        patch("plugins.self_managed_kafka.gathering.admin_api.create_admin_client", return_value=admin_client),
    ):
        plugin.initialize(settings)
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'pending-overlay.db'}",
        SelfManagedKafkaStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    pending_date = datetime(2026, 8, 10, tzinfo=UTC).date()
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="self_managed_kafka",
                tenant_id="tenant-1",
                resource_id="billing-cluster-a:topic:created-during-window",
                resource_type="topic",
                display_name="created-during-window",
                parent_id="billing-cluster-a",
                created_at=datetime(2026, 8, 10, 12, tzinfo=UTC),
            )
        )
        uow.resources.upsert(
            CoreResource(
                ecosystem="self_managed_kafka",
                tenant_id="tenant-1",
                resource_id="billing-cluster-a:topic:deleted-during-window",
                resource_type="topic",
                display_name="deleted-during-window",
                parent_id="billing-cluster-a",
                created_at=datetime(2026, 8, 1, tzinfo=UTC),
                deleted_at=datetime(2026, 8, 10, 18, tzinfo=UTC),
            )
        )
        uow.billing.upsert(
            CoreBillingLineItem(
                ecosystem="self_managed_kafka",
                tenant_id="tenant-1",
                timestamp=datetime(2026, 8, 10, tzinfo=UTC),
                resource_id="billing-cluster-a",
                product_category="kafka",
                product_type="SELF_KAFKA_NETWORK_INGRESS",
                quantity=Decimal("1"),
                unit_price=Decimal("10"),
                total_cost=Decimal("10"),
            )
        )
        uow.billing.upsert(
            CoreBillingLineItem(
                ecosystem="self_managed_kafka",
                tenant_id="tenant-1",
                timestamp=datetime(2026, 8, 10, tzinfo=UTC),
                resource_id="billing-cluster-a",
                product_category="kafka",
                product_type="SELF_KAFKA_COMPUTE",
                quantity=Decimal("1"),
                unit_price=Decimal("10"),
                total_cost=Decimal("10"),
            )
        )
        uow.pipeline_state.upsert(
            PipelineState(
                ecosystem="self_managed_kafka",
                tenant_id="tenant-1",
                tracking_date=pending_date,
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
                topic_overlay_gathered=True,
            )
        )
        uow.self_managed_kafka_scope_state.open(
            ecosystem="self_managed_kafka",
            tenant_id="tenant-1",
            cluster_id="billing-cluster-a",
            metrics_identifier_label="kafka_cluster_id",
            metrics_identifier="kraft-a-001",
            window_start=datetime(2026, 8, 10, tzinfo=UTC),
            window_end=datetime(2026, 8, 11, tzinfo=UTC),
            reason="target_scope_validation",
            status="target_down",
            detail="previous scope outage",
            opened_at=datetime(2026, 8, 11, tzinfo=UTC),
        )
        uow.commit()

    tenant = TenantConfig(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        lookback_days=2,
        cutoff_days=1,
        plugin_settings=PluginSettingsBase.model_validate(settings),
    )
    orchestrator = ChargebackOrchestrator("tenant", tenant, plugin, backend, source)
    now = datetime(2026, 8, 20, tzinfo=UTC)
    orchestrator._gather_phase._last_resource_gather_at = now

    def run_at(run_time: datetime):
        with patch("core.engine.orchestrator.datetime") as patched_datetime:
            patched_datetime.now.return_value = run_time
            patched_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            return orchestrator.run()

    skipped = run_at(now)
    assert skipped.errors == []
    assert observed_query_keys == []
    with backend.create_read_only_unit_of_work() as uow:
        scope_state = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "billing-cluster-a")
        assert scope_state is not None
        assert scope_state.status == "open"

    orchestrator._gather_phase._last_resource_gather_at = None
    failed_discovery = run_at(now + timedelta(days=1))
    assert any("Handler kafka gather failed" in error for error in failed_discovery.errors)
    assert all(not {"topic_bytes_in", "topic_bytes_out", "topic_storage_bytes"} & keys for keys in observed_query_keys)
    with backend.create_read_only_unit_of_work() as uow:
        state = uow.pipeline_state.get("self_managed_kafka", "tenant-1", pending_date)
        assert state is not None
        assert state.topic_overlay_gathered is True
        assert state.topic_attribution_calculated is False
        assert uow.topic_attributions.find_by_date("self_managed_kafka", "tenant-1", pending_date) == []
        scope_state = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "billing-cluster-a")
        assert scope_state is not None
        assert scope_state.status == "retention_gap"

    admin_client.list_topics.side_effect = None
    admin_client.list_topics.return_value = []
    topic_evidence_available = True
    successful_inventory = run_at(now + timedelta(days=2))
    assert successful_inventory.errors == []
    with backend.create_read_only_unit_of_work() as uow:
        state = uow.pipeline_state.get("self_managed_kafka", "tenant-1", pending_date)
        assert state is not None
        assert state.topic_attribution_calculated is True
        rows = uow.topic_attributions.find_by_date("self_managed_kafka", "tenant-1", pending_date)
        assert {row.topic_name for row in rows if row.product_type == "SELF_KAFKA_COMPUTE"} == {
            "created-during-window",
            "deleted-during-window",
        }
        assert {row.topic_name for row in rows if row.product_type == "SELF_KAFKA_NETWORK_INGRESS"} == {
            "created-during-window",
            "deleted-during-window",
        }
        scope_state = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "billing-cluster-a")
        assert scope_state is not None
        assert scope_state.status == "closed"
    backend.dispose()


def test_configured_plugin_runs_topic_overlay_through_the_normal_orchestrator_path(tmp_path: Path) -> None:
    from core.config.models import PluginSettingsBase, TenantConfig
    from core.engine.orchestrator import ChargebackOrchestrator
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

    source = MagicMock()

    def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
        start = kwargs["start"]
        end = kwargs["end"]
        step = kwargs["step"]
        assert isinstance(start, datetime)
        assert isinstance(end, datetime)
        assert isinstance(step, timedelta)
        rows: dict[str, list[MetricRow]] = {}
        for query_definition in queries:
            key = query_definition.key
            if key == "target_up":
                target_rows: list[MetricRow] = []
                timestamp = start
                while timestamp < end:
                    target_rows.append(
                        MetricRow(
                            timestamp,
                            key,
                            1.0,
                            {"kafka_cluster_id": "kraft-a-001", "broker": "1"},
                        )
                    )
                    timestamp += step
                target_rows.append(
                    MetricRow(
                        end,
                        key,
                        1.0,
                        {"kafka_cluster_id": "kraft-a-001", "broker": "1"},
                    )
                )
                rows[key] = target_rows
            elif key.startswith("broker_topic_discovery"):
                rows[key] = [
                    MetricRow(
                        start,
                        key,
                        1.0,
                        {"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders"},
                    )
                ]
            elif key in {"cluster_bytes_in", "cluster_bytes_out"}:
                rows[key] = [MetricRow(end, key, 1073741824.0, {"kafka_cluster_id": "kraft-a-001", "broker": "1"})]
            elif key == "cluster_storage_bytes":
                rows[key] = [
                    MetricRow(
                        start + (end - start) / 2,
                        key,
                        1073741824.0,
                        {"kafka_cluster_id": "kraft-a-001", "broker": "1"},
                    )
                ]
            elif key in {"topic_bytes_in", "topic_bytes_out", "topic_storage_bytes"}:
                rows[key] = [
                    MetricRow(
                        end if key != "topic_storage_bytes" else start + (end - start) / 2,
                        key,
                        1073741824.0,
                        {"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders", "partition": "0"},
                    )
                ]
            else:
                rows[key] = []
        return rows

    source.query.side_effect = query
    settings = _settings()
    settings["topic_attribution"] = {"enabled": True, "compute_policy": "shared_even_v1"}
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(settings)
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'topic-overlay.db'}",
        SelfManagedKafkaStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    tenant = TenantConfig(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        lookback_days=2,
        cutoff_days=1,
        plugin_settings=PluginSettingsBase.model_validate(settings),
    )
    orchestrator = ChargebackOrchestrator("tenant", tenant, plugin, backend, source)

    with patch("core.engine.orchestrator.datetime") as patched_datetime:
        patched_datetime.now.return_value = datetime(2026, 8, 20, tzinfo=UTC)
        patched_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        result = orchestrator.run()

    assert result.errors == []
    with backend.create_read_only_unit_of_work() as uow:
        dates = uow.topic_attributions.get_distinct_dates("self_managed_kafka", "tenant-1")
        rows = [
            row
            for tracking_date in dates
            for row in uow.topic_attributions.find_by_date("self_managed_kafka", "tenant-1", tracking_date)
        ]

    assert {row.product_type for row in rows} == {
        "SELF_KAFKA_COMPUTE",
        "SELF_KAFKA_STORAGE",
        "SELF_KAFKA_NETWORK_INGRESS",
        "SELF_KAFKA_NETWORK_EGRESS",
    }
    assert {row.topic_name for row in rows} == {"orders"}
    assert {row.attribution_method for row in rows} >= {"bytes_ratio", "retained_bytes_ratio", "shared_even_v1"}
    backend.dispose()

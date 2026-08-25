"""Production backend reconstruction for plugin-owned Self-Managed Kafka storage."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, inspect, text

from core.config.models import PluginSettingsBase, StorageConfig, TenantConfig
from core.models.chargeback import ChargebackRow, CostType
from core.plugin.registry import PluginRegistry
from core.storage.backend_provider import ApiTenantBackendProvider
from core.storage.backends.sqlmodel.module import CoreStorageModule
from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostAllocator, CostInput, ServiceHandler


class _NoPluginStoragePlugin:
    """Full EcosystemPlugin double whose storage has no plugin migration capability."""

    @property
    def ecosystem(self) -> str:
        return "generic_metrics_only"

    def initialize(self, config: dict[str, Any]) -> None:
        del config

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        return {}

    def get_cost_input(self) -> CostInput:
        raise AssertionError("cost input is not used while constructing API storage")

    def get_metrics_source(self) -> MetricsSource | None:
        return None

    def get_fallback_allocator(self) -> CostAllocator | None:
        return None

    def build_shared_context(self, tenant_id: str) -> object | None:
        del tenant_id
        return None

    def get_storage_module(self) -> CoreStorageModule:
        return CoreStorageModule()

    def close(self) -> None:
        return None


def _table_names(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def _version(url: str) -> str:
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            return connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    finally:
        engine.dispose()


def _self_managed_settings() -> dict[str, object]:
    return {
        "cluster_id": "cluster-1",
        "metrics_identifier": "cluster-1",
        "metrics_identifier_label": "kafka_cluster_id",
        "broker_count": 1,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "metrics": {"url": "http://prometheus.invalid:9090"},
    }


def _chargeback_row() -> ChargebackRow:
    return ChargebackRow(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        resource_id="cluster-1",
        product_category="network",
        product_type="SELF_KAFKA_NETWORK_INGRESS",
        identity_id="User:alice",
        cost_type=CostType.USAGE,
        amount=Decimal("1.0000"),
        allocation_method="principal_quota_ready_v1",
        allocation_detail="usage_ratio_allocation",
        metadata={"team": "team-data"},
    )


def test_selected_backend_reconstruction_prepares_plugin_schema_at_existing_core_head(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'shared-tenant.db'}"
    registry = PluginRegistry()
    registry.register("generic_metrics_only", _NoPluginStoragePlugin)
    registry.register("self_managed_kafka", SelfManagedKafkaPlugin)
    provider = ApiTenantBackendProvider(registry)
    no_plugin = TenantConfig(
        ecosystem="generic_metrics_only",
        tenant_id="tenant-1",
        lookback_days=2,
        cutoff_days=1,
        storage=StorageConfig(connection_string=url),
    )
    selected = TenantConfig(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        lookback_days=2,
        cutoff_days=1,
        storage=StorageConfig(connection_string=url),
        plugin_settings=PluginSettingsBase.model_validate(_self_managed_settings()),
    )

    try:
        with provider.acquire_backend("tenant", no_plugin):
            pass
        assert _version(url) == "033"
        assert {
            "self_managed_kafka_scope_state",
            "self_managed_kafka_principal_team_snapshots",
        }.isdisjoint(_table_names(url))

        with provider.acquire_backend("tenant", selected) as backend:
            assert {
                "self_managed_kafka_scope_state",
                "self_managed_kafka_principal_team_snapshots",
            } <= _table_names(url)
            with backend.create_unit_of_work() as uow:
                uow.self_managed_kafka_scope_state.open(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-1",
                    cluster_id="cluster-1",
                    metrics_identifier_label="kafka_cluster_id",
                    metrics_identifier="cluster-1",
                    window_start=datetime(2026, 8, 1, tzinfo=UTC),
                    window_end=datetime(2026, 8, 2, tzinfo=UTC),
                    reason="mismatch",
                    status="mismatch",
                    detail="test",
                    opened_at=datetime(2026, 8, 2, tzinfo=UTC),
                )
                uow.chargebacks.upsert(_chargeback_row())
                uow.commit()
            with backend.create_read_only_unit_of_work() as uow:
                state = uow.self_managed_kafka_scope_state.get("self_managed_kafka", "tenant-1", "cluster-1")
                rows = uow.chargebacks.find_by_date("self_managed_kafka", "tenant-1", datetime(2026, 8, 1).date())
                assert state is not None
                assert state.status == "open"
                assert [row.metadata for row in rows] == [{"team": "team-data"}]
    finally:
        provider.close()

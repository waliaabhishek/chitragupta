from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, call, patch

import pytest

from core.config.models import FocusPreviewTenantConfig, StorageConfig, TenantConfig
from core.plugin.protocols import CostAllocator, CostInput, EcosystemPlugin, ServiceHandler, StorageModule
from core.preview.evidence import PreviewEvidenceBootstrapResult, PreviewEvidenceBootstrapStatus
from core.preview.storage_availability import (
    PreviewEvidenceAvailability,
    PreviewEvidenceAvailabilityState,
)
from core.storage.interface import ReadOnlyUnitOfWork, StorageBackend, UnitOfWork
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.evidence_backend_double import preview_evidence_backend_double

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models.billing import BillingLineItem


class _CostInput:
    def gather(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
    ) -> Iterable[BillingLineItem]:
        del tenant_id, start, end, uow
        return ()


class _Plugin:
    ecosystem = "confluent_cloud"

    def __init__(self) -> None:
        self.initialize_count = 0
        self.close_count = 0
        self.module = CCloudStorageModule()

    def initialize(self, config: dict[str, Any]) -> None:
        del config
        self.initialize_count += 1

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        return {}

    def get_cost_input(self) -> CostInput:
        return _CostInput()

    def get_metrics_source(self) -> MetricsSource | None:
        return None

    def get_fallback_allocator(self) -> CostAllocator | None:
        return None

    def build_shared_context(self, tenant_id: str) -> None:
        del tenant_id

    def get_storage_module(self) -> StorageModule:
        return self.module

    def close(self) -> None:
        self.close_count += 1


class _Registry:
    def __init__(self) -> None:
        self.plugins: list[_Plugin] = []

    def create(self, ecosystem: str) -> _Plugin:
        assert ecosystem == "confluent_cloud"
        plugin = _Plugin()
        self.plugins.append(plugin)
        return plugin


class _Backend:
    def __init__(self) -> None:
        self.create_tables_count = 0
        self.dispose_count = 0

    def create_tables(self) -> None:
        self.create_tables_count += 1

    def create_unit_of_work(self) -> UnitOfWork:
        uow = MagicMock(spec=UnitOfWork)
        uow.__enter__.return_value = uow
        uow.pipeline_runs.get_latest_run.return_value = None
        return uow

    def create_read_only_unit_of_work(self) -> ReadOnlyUnitOfWork:
        uow = MagicMock(spec=ReadOnlyUnitOfWork)
        uow.__enter__.return_value = uow
        return uow

    def dispose(self) -> None:
        self.dispose_count += 1


def test_backend_provider_doubles_satisfy_their_production_protocols() -> None:
    assert isinstance(_Plugin(), EcosystemPlugin)
    assert isinstance(_Backend(), StorageBackend)


def _tenant(tmp_path: Path) -> TenantConfig:
    return TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'provider.db'}"),
    )


def test_api_provider_constructs_once_and_owns_plugin_and_backend_close(tmp_path: Path) -> None:
    from core.storage.backend_provider import ApiTenantBackendProvider

    registry = _Registry()
    backend = _Backend()
    with patch("core.storage.backend_provider.create_storage_backend", return_value=backend) as factory:
        provider = ApiTenantBackendProvider(registry)
        with (
            provider.acquire_backend("production", _tenant(tmp_path)) as first,
            provider.acquire_backend("production", _tenant(tmp_path)) as second,
        ):
            assert first is backend
            assert second is backend
        provider.close()

    factory.assert_called_once()
    assert factory.call_args.kwargs["storage_module"] is registry.plugins[0].module
    assert factory.call_args.kwargs["focus_preview_enabled"] is False
    assert backend.create_tables_count == 1
    assert backend.dispose_count == 1
    assert registry.plugins[0].initialize_count == 1
    assert registry.plugins[0].close_count == 1


def test_api_provider_close_waits_for_active_lease(tmp_path: Path) -> None:
    from core.storage.backend_provider import ApiTenantBackendProvider

    registry = _Registry()
    backend = _Backend()
    provider = ApiTenantBackendProvider(registry)
    lease_entered = threading.Event()
    release_lease = threading.Event()
    close_finished = threading.Event()

    def hold_lease() -> None:
        with provider.acquire_backend("production", _tenant(tmp_path)):
            lease_entered.set()
            assert release_lease.wait(timeout=2)

    def close_provider() -> None:
        provider.close()
        close_finished.set()

    with patch("core.storage.backend_provider.create_storage_backend", return_value=backend):
        lease_thread = threading.Thread(target=hold_lease)
        lease_thread.start()
        assert lease_entered.wait(timeout=2)
        close_thread = threading.Thread(target=close_provider)
        close_thread.start()
        time.sleep(0.05)
        assert not close_finished.is_set()
        assert backend.dispose_count == 0
        release_lease.set()
        lease_thread.join(timeout=2)
        close_thread.join(timeout=2)

    assert close_finished.is_set()
    assert backend.dispose_count == 1
    assert registry.plugins[0].close_count == 1


def test_failed_backend_construction_closes_plugin_and_publishes_no_cache_entry(tmp_path: Path) -> None:
    from core.storage.backend_provider import ApiTenantBackendProvider

    registry = _Registry()
    provider = ApiTenantBackendProvider(registry)
    successful = _Backend()
    with patch(
        "core.storage.backend_provider.create_storage_backend",
        side_effect=[RuntimeError("construction failed"), successful],
    ):
        with (
            pytest.raises(RuntimeError, match="construction failed"),
            provider.acquire_backend("production", _tenant(tmp_path)),
        ):
            pass
        with provider.acquire_backend("production", _tenant(tmp_path)) as backend:
            assert backend is successful
        provider.close()

    assert len(registry.plugins) == 2
    assert registry.plugins[0].close_count == 1
    assert registry.plugins[1].close_count == 1
    assert successful.dispose_count == 1


def test_provider_construction_preserves_original_error_while_attempting_both_cleanup_steps(
    tmp_path: Path,
) -> None:
    from core.storage.backend_provider import ApiTenantBackendProvider

    plugin = MagicMock()
    registry = MagicMock()
    registry.create.return_value = plugin
    backend = MagicMock()
    backend.dispose.side_effect = RuntimeError("storage cleanup failed")
    plugin.close.side_effect = RuntimeError("plugin cleanup failed")
    provider = ApiTenantBackendProvider(registry)

    with (
        patch("core.storage.backend_provider.create_storage_backend", return_value=backend),
        patch(
            "core.storage.backend_provider.prepare_tenant_backend",
            side_effect=ValueError("original construction failure"),
        ),
        pytest.raises(ValueError, match="original construction failure"),
        provider.acquire_backend("production", _tenant(tmp_path)),
    ):
        pass

    backend.dispose.assert_called_once_with()
    plugin.close.assert_called_once_with()


def test_closed_provider_rejects_new_leases(tmp_path: Path) -> None:
    from core.storage.backend_provider import ApiTenantBackendProvider

    provider = ApiTenantBackendProvider(_Registry())
    provider.close()

    with pytest.raises(RuntimeError, match="closed"), provider.acquire_backend("production", _tenant(tmp_path)):
        pass


def test_api_provider_config_replacement_retires_old_entry_and_closes_each_owner_once(
    tmp_path: Path,
) -> None:
    from core.storage.backend_provider import ApiTenantBackendProvider

    registry = _Registry()
    first = _Backend()
    second = _Backend()
    original = _tenant(tmp_path)
    replacement = original.model_copy(update={"lookback_days": original.lookback_days + 1})
    provider = ApiTenantBackendProvider(registry)

    with patch(
        "core.storage.backend_provider.create_storage_backend",
        side_effect=[first, second],
    ):
        with provider.acquire_backend("production", original) as acquired:
            assert acquired is first
        with provider.acquire_backend("production", replacement) as acquired:
            assert acquired is second
        assert first.dispose_count == 1
        assert registry.plugins[0].close_count == 1
        provider.close()

    assert first.dispose_count == 1
    assert second.dispose_count == 1
    assert [plugin.close_count for plugin in registry.plugins] == [1, 1]


def test_api_provider_replaces_cached_backend_when_nested_secret_value_changes(
    tmp_path: Path,
) -> None:
    from core.storage.backend_provider import ApiTenantBackendProvider
    from plugins.confluent_cloud.config import CCloudPluginConfig

    registry = _Registry()
    first = _Backend()
    second = _Backend()
    original = _tenant(tmp_path).model_copy(
        update={
            "plugin_settings": CCloudPluginConfig(
                ccloud_api={"key": "api-key", "secret": "secret-one"},  # pragma: allowlist secret
            )
        }
    )
    replacement = original.model_copy(
        update={
            "plugin_settings": CCloudPluginConfig(
                ccloud_api={"key": "api-key", "secret": "secret-two"},  # pragma: allowlist secret
            )
        }
    )
    provider = ApiTenantBackendProvider(registry)

    with patch(
        "core.storage.backend_provider.create_storage_backend",
        side_effect=[first, second],
    ):
        with provider.acquire_backend("production", original) as acquired:
            assert acquired is first
        with provider.acquire_backend("production", replacement) as acquired:
            assert acquired is second
        provider.close()

    assert len(registry.plugins) == 2
    assert first.dispose_count == second.dispose_count == 1


def test_api_provider_prepares_each_enabled_replacement_exactly_once(tmp_path: Path) -> None:
    from core.storage.backend_provider import ApiTenantBackendProvider

    registry = _Registry()
    first = preview_evidence_backend_double()
    second = preview_evidence_backend_double()
    first_bootstrap = MagicMock()
    second_bootstrap = MagicMock()
    first_result = PreviewEvidenceBootstrapResult(
        status=PreviewEvidenceBootstrapStatus.ALREADY_CURRENT,
        bootstrapped_windows=0,
        bootstrapped_rows=0,
        reason=None,
    )
    second_result = PreviewEvidenceBootstrapResult(
        status=PreviewEvidenceBootstrapStatus.BOOTSTRAPPED,
        bootstrapped_windows=1,
        bootstrapped_rows=2,
        reason=None,
    )
    first_bootstrap.bootstrap_owner.return_value = first_result
    second_bootstrap.bootstrap_owner.return_value = second_result
    first.create_preview_evidence_bootstrap.return_value = first_bootstrap
    second.create_preview_evidence_bootstrap.return_value = second_bootstrap
    original = _tenant(tmp_path).model_copy(
        update={
            "focus_preview": FocusPreviewTenantConfig(
                commercial_profile="direct_payg",
                effective_start_date="2026-01-01",
                effective_end_date="2027-01-01",
            )
        }
    )
    replacement = original.model_copy(update={"lookback_days": original.lookback_days + 1})
    provider = ApiTenantBackendProvider(registry)

    with (
        patch(
            "core.storage.backend_provider.create_storage_backend",
            side_effect=[first, second],
        ),
        patch("core.storage.tenant_lifecycle.cleanup_orphaned_pipeline_run") as cleanup,
    ):
        with provider.acquire_backend("production", original):
            assert provider._entries["production"].bootstrap_result is first_result  # noqa: SLF001
        with provider.acquire_backend("production", replacement):
            assert provider._entries["production"].bootstrap_result is second_result  # noqa: SLF001
        provider.close()

    first.create_tables.assert_called_once_with()
    second.create_tables.assert_called_once_with()
    first_bootstrap.bootstrap_owner.assert_called_once()
    second_bootstrap.bootstrap_owner.assert_called_once()
    assert cleanup.call_args_list == [call(first, "production"), call(second, "production")]


def test_prepare_tenant_backend_skips_bootstrap_unless_preview_schema_is_ready(tmp_path: Path) -> None:
    from core.storage.tenant_lifecycle import prepare_tenant_backend

    backend = preview_evidence_backend_double()
    backend.preview_evidence_availability = PreviewEvidenceAvailability(
        state=PreviewEvidenceAvailabilityState.UNAVAILABLE,
    )
    tenant = _tenant(tmp_path).model_copy(
        update={
            "focus_preview": FocusPreviewTenantConfig(
                commercial_profile="direct_payg",
                effective_start_date="2026-01-01",
                effective_end_date="2027-01-01",
            )
        }
    )

    with patch("core.storage.tenant_lifecycle.cleanup_orphaned_pipeline_run"):
        result = prepare_tenant_backend(backend, "production", tenant)

    assert result is None
    backend.create_tables.assert_called_once_with()
    backend.create_preview_evidence_bootstrap.assert_not_called()


def test_api_owner_retains_generic_backend_but_preview_fails_closed_after_bootstrap_exception(
    tmp_path: Path,
) -> None:
    from core.preview.generator import PreviewGenerationError, PreviewPackageGenerator
    from core.preview.storage_availability import PreviewEvidenceBootstrapUnavailable
    from core.storage.backend_provider import ApiTenantBackendProvider
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.confluent_cloud.preview_bootstrap import PreviewEvidenceBootstrapError
    from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _request
    from tests.unit.core.preview.test_service_profiles_v5 import _policy

    tenant = _tenant(tmp_path).model_copy(
        update={
            "focus_preview": FocusPreviewTenantConfig(
                commercial_profile="direct_payg",
                effective_start_date="2020-01-01",
                effective_end_date="2030-01-01",
            )
        }
    )
    backend = SQLModelBackend(
        tenant.storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    provider = ApiTenantBackendProvider(_Registry())

    with (
        patch("core.storage.backend_provider.create_storage_backend", return_value=backend) as factory,
        patch(
            "plugins.confluent_cloud.preview_bootstrap.CCloudPreviewEvidenceBootstrap.bootstrap_owner",
            side_effect=PreviewEvidenceBootstrapError("bootstrap failed"),
        ),
        provider.acquire_backend("production", tenant) as acquired,
    ):
        assert acquired is backend
        assert acquired.preview_evidence_availability.state is PreviewEvidenceAvailabilityState.UNAVAILABLE
        retained = provider._entries["production"].bootstrap_result  # noqa: SLF001
        assert retained == PreviewEvidenceBootstrapUnavailable("PreviewEvidenceBootstrapError")
        with acquired.create_unit_of_work() as uow:
            assert uow.chargebacks.find_by_date("confluent_cloud", "tenant-1", date(2026, 7, 1)) == []
            uow.rollback()

        header_request = _request(
            grain="monthly",
            created_at=datetime(2026, 7, 1, tzinfo=UTC),
            started_at=datetime(2026, 7, 1, 0, 0, 1, tzinfo=UTC),
        )
        normal_request = _request(grain="daily")
        generator = PreviewPackageGenerator(max_csv_file_bytes=None)
        for request, cutoff in (
            (header_request, date(2026, 7, 1)),
            (normal_request, date(2026, 7, 2)),
        ):
            with pytest.raises(PreviewGenerationError) as exc_info:
                generator.generate(backend=acquired, request=request, policy=_policy(cutoff=cutoff))
            assert exc_info.value.diagnostic.code == "preview_evidence_storage_unavailable"
            assert exc_info.value.diagnostic.retryable is False

        with provider.acquire_backend("production", tenant) as reacquired:
            assert reacquired is acquired

    factory.assert_called_once()
    assert factory.call_args.args == (tenant.storage,)
    assert isinstance(factory.call_args.kwargs["storage_module"], CCloudStorageModule)
    assert factory.call_args.kwargs["focus_preview_enabled"] is True
    provider.close()

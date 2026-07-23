from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

from core.api.app import create_app
from core.config.models import AppSettings, PreviewConfig, StorageConfig, TenantConfig
from core.plugin.protocols import CostAllocator, CostInput, EcosystemPlugin, ServiceHandler, StorageModule
from core.storage.interface import ReadOnlyUnitOfWork, StorageBackend, UnitOfWork
from plugins.confluent_cloud.storage.module import CCloudStorageModule

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource


class BodyError(RuntimeError):
    pass


def _fails(failure: str | set[str] | None, step: str) -> bool:
    return failure == step or isinstance(failure, set) and step in failure


@dataclass
class ControlledStore:
    events: list[str]
    failure: str | set[str] | None = None

    def cleanup_staging(self, owner: object) -> int:
        del owner
        self.events.append("store.cleanup_staging")
        if _fails(self.failure, "staging"):
            raise RuntimeError("staging cleanup failed")
        return 0

    def reconcile_finalized(
        self,
        *,
        owner: object,
        referenced_storage_keys: frozenset[str],
        is_referenced: object,
    ) -> int:
        del owner, referenced_storage_keys, is_referenced
        return 0

    def close(self) -> None:
        self.events.append("store.close")
        if _fails(self.failure, "store"):
            raise RuntimeError("store cleanup failed")


@dataclass
class ControlledRuntime:
    events: list[str]
    failure: str | set[str] | None = None
    store: ControlledStore | None = None
    record_recovery: bool = False

    def ensure_owner_recovered(
        self,
        *,
        backend: object,
        owner: object,
    ) -> None:
        del backend
        tenant_name = owner.tenant_name
        if self.record_recovery:
            assert self.store is not None
            self.store.cleanup_staging(owner)
            self.events.append(f"runtime.recover:{tenant_name}:{owner.ecosystem}:{owner.tenant_id}")
        if _fails(self.failure, tenant_name):
            raise RuntimeError(f"{tenant_name} recovery failed")

    def close(self, *, wait: bool = True) -> None:
        self.events.append(f"runtime.close(wait={wait!r})")
        if _fails(self.failure, "runtime"):
            raise RuntimeError("runtime cleanup failed")


@dataclass
class ControlledBackend:
    name: str
    events: list[str]
    failure: str | set[str] | None = None

    def create_tables(self) -> None:
        self.events.append(f"{self.name}.create_tables")

    def create_unit_of_work(self) -> UnitOfWork:
        self.events.append("orphan-cleanup")
        raise RuntimeError("startup cleanup failed")

    def create_read_only_unit_of_work(self) -> ReadOnlyUnitOfWork:
        raise AssertionError("read-only storage is outside this lifecycle test")

    def dispose(self) -> None:
        self.events.append(f"{self.name}.dispose")
        if _fails(self.failure, self.name):
            raise RuntimeError(f"{self.name} cleanup failed")


@dataclass
class ControlledPlugin:
    events: list[str]

    def initialize(self, config: dict[str, Any]) -> None:
        del config
        self.events.append("plugin.initialize")

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        return {}

    def get_cost_input(self) -> CostInput:
        return MagicMock(spec=CostInput)

    def get_metrics_source(self) -> MetricsSource | None:
        return None

    def get_fallback_allocator(self) -> CostAllocator | None:
        return None

    def build_shared_context(self, tenant_id: str) -> None:
        del tenant_id

    @property
    def ecosystem(self) -> str:
        return "confluent_cloud"

    def get_storage_module(self) -> StorageModule:
        return CCloudStorageModule()

    def close(self) -> None:
        self.events.append("plugin.close")


@dataclass
class ControlledRegistry:
    plugin: ControlledPlugin

    def create(self, ecosystem: str) -> ControlledPlugin:
        assert ecosystem == "confluent_cloud"
        return self.plugin


def test_lifespan_backend_and_plugin_doubles_satisfy_production_protocols() -> None:
    events: list[str] = []

    assert isinstance(ControlledBackend("backend", events), StorageBackend)
    assert isinstance(ControlledPlugin(events), EcosystemPlugin)


@dataclass
class ControlledProvider:
    events: list[str]
    backends: dict[str, object]

    @contextmanager
    def acquire_backend(self, tenant_name: str, tenant_config: TenantConfig) -> Any:
        del tenant_config
        self.events.append(f"provider.acquire:{tenant_name}")
        yield self.backends[tenant_name]

    def close(self) -> None:
        self.events.append("provider.close")


@dataclass
class ControlledRunner:
    events: list[str]
    failure: str | set[str] | None = None
    backends: dict[str, object] | None = None

    @contextmanager
    def acquire_backend(self, tenant_name: str, tenant_config: TenantConfig) -> Any:
        del tenant_config
        backend = (self.backends or {}).get(tenant_name, object())
        self.events.append(f"runner.acquire:{tenant_name}")
        yield backend

    def close(self) -> None:
        raise AssertionError("combined-mode lifespan must delegate closure to drain")

    def drain(self, timeout: float) -> None:
        self.events.append(f"runner.drain({timeout})")
        if _fails(self.failure, "runner"):
            raise RuntimeError("runner cleanup failed")


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings(
        preview=PreviewConfig(artifact_root=tmp_path / "artifacts", max_workers=1),
        tenants={
            "production": TenantConfig(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'preview.db'}"),
                focus_preview={
                    "commercial_profile": "direct_payg",
                    "billing_currency": "USD",
                    "effective_start_date": "2020-01-01",
                    "effective_end_date": "2030-01-01",
                },
            )
        },
    )


def _multi_tenant_settings(tmp_path: Path) -> AppSettings:
    shared = dict(
        tenant_id="shared-provider-tenant",
        focus_preview={
            "commercial_profile": "direct_payg",
            "billing_currency": "USD",
            "effective_start_date": "2020-01-01",
            "effective_end_date": "2030-01-01",
        },
    )
    return AppSettings(
        preview=PreviewConfig(artifact_root=tmp_path / "artifacts", max_workers=1),
        tenants={
            "tenant-a": TenantConfig(
                ecosystem="confluent_cloud",
                storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'a.db'}"),
                **shared,
            ),
            "tenant-b": TenantConfig(
                ecosystem="confluent_cloud",
                storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'b.db'}"),
                **shared,
            ),
            "unsupported": TenantConfig(
                ecosystem="other",
                tenant_id="other",
                storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'other.db'}"),
            ),
        },
    )


def _run[T](coroutine: Coroutine[Any, Any, T]) -> T:
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coroutine)
    finally:
        loop.close()


async def _run_inline(function: Any, *args: object, **kwargs: object) -> object:
    return function(*args, **kwargs)


def _patch_owned_resources(store: ControlledStore, runtime: ControlledRuntime) -> tuple[Any, Any]:
    import_module("core.api.routes.focus_preview")
    return (
        patch("core.preview.artifacts.LocalPreviewArtifactStore", return_value=store),
        patch("core.preview.service.PreviewRuntime", return_value=runtime),
    )


def test_lifespan_wires_borrowed_current_revision_reader_to_owned_api_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events)
    reader = object()
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with (
        store_patch,
        runtime_patch,
        patch("core.preview.revisions.PreviewRevisionReadService", return_value=reader) as reader_type,
    ):
        app = create_app(_settings(tmp_path))

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                assert app.state.preview_artifact_store is store
                assert app.state.preview_revision_reader is reader

        _run(exercise())

    reader_type.assert_called_once_with(artifact_store=store)
    assert events == ["runtime.close(wait=True)", "store.close"]


def test_lifespan_body_exception_propagates_after_exact_ordered_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events)
    runner = ControlledRunner(events)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                raise BodyError("body sentinel")

        with pytest.raises(BodyError, match="body sentinel"):
            _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "runner.drain(30)",
    ]


def test_lifespan_cancellation_propagates_after_exact_ordered_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events)
    runner = ControlledRunner(events)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                raise asyncio.CancelledError("cancel sentinel")

        with pytest.raises(asyncio.CancelledError, match="cancel sentinel"):
            _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "runner.drain(30)",
    ]


def test_lifespan_body_exception_survives_multiple_cleanup_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    failures = {"runtime", "store", "runner"}
    store = ControlledStore(events, failures)
    runtime = ControlledRuntime(events, failures)
    runner = ControlledRunner(events, failures)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                raise BodyError("body sentinel")

        with pytest.raises(BodyError, match="body sentinel"):
            _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "runner.drain(30)",
    ]


@pytest.mark.parametrize("failure", ["runtime", "store", "runner"])
def test_each_cleanup_failure_surfaces_and_all_later_cleanup_steps_are_attempted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    events: list[str] = []
    store = ControlledStore(events, failure)
    runtime = ControlledRuntime(events, failure)
    runner = ControlledRunner(events, failure)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                pass

        with pytest.raises(RuntimeError, match=rf"{failure} cleanup failed"):
            _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "runner.drain(30)",
    ]


def test_api_only_orphan_cleanup_failure_is_nonfatal_and_provider_closes_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events)
    backend = ControlledBackend("backend", events)
    plugin = ControlledPlugin(events)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with (
        store_patch,
        runtime_patch,
        patch("core.storage.backend_provider.create_storage_backend", return_value=backend),
    ):
        app = create_app(
            _settings(tmp_path),
            plugin_registry=ControlledRegistry(plugin),  # type: ignore[arg-type]
        )

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                events.append("body")

        _run(exercise())

    assert events[-5:] == [
        "body",
        "runtime.close(wait=True)",
        "store.close",
        "backend.dispose",
        "plugin.close",
    ]
    assert events.count("orphan-cleanup") == 1


def test_lifespan_normal_exit_keeps_exact_cleanup_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events)
    runner = ControlledRunner(events)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                assert not hasattr(app.state, "backends")

        _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "runner.drain(30)",
    ]


def test_combined_startup_recovers_each_enabled_owner_through_runner_leases_in_settings_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_module = import_module("core.api.app")
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events, store=store, record_recovery=True)
    backends = {
        "tenant-a": ControlledBackend("backend-a", events),
        "tenant-b": ControlledBackend("backend-b", events),
    }

    def recover_preview_owner(
        tenant_name: str,
        tenant_config: TenantConfig,
        backend_provider: ControlledRunner,
        preview_runtime: ControlledRuntime,
    ) -> None:
        with backend_provider.acquire_backend(tenant_name, tenant_config) as backend:
            artifacts = import_module("core.preview.artifacts")
            preview_runtime.ensure_owner_recovered(
                backend=backend,
                owner=artifacts.preview_artifact_owner(tenant_name, tenant_config),
            )

    monkeypatch.setattr(app_module, "recover_preview_owner", recover_preview_owner, raising=False)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    runner = ControlledRunner(events, backends=backends)
    with store_patch, runtime_patch:
        app = create_app(_multi_tenant_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                events.append("body")

        _run(exercise())

    assert events == [
        "runner.acquire:tenant-a",
        "store.cleanup_staging",
        "runtime.recover:tenant-a:confluent_cloud:shared-provider-tenant",
        "runner.acquire:tenant-b",
        "store.cleanup_staging",
        "runtime.recover:tenant-b:confluent_cloud:shared-provider-tenant",
        "body",
        "runtime.close(wait=True)",
        "store.close",
        "runner.drain(30)",
    ]


def test_one_owner_recovery_failure_is_nonfatal_and_does_not_skip_other_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = import_module("core.preview.service")
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events, store=store, record_recovery=True)
    original_recovery = runtime.ensure_owner_recovered

    def selective_recovery(**kwargs: object) -> None:
        if kwargs["owner"].tenant_name == "tenant-a":
            events.append("runtime.recover-failed:tenant-a")
            raise service.PreviewRecoveryUnavailable("recovery")
        original_recovery(**kwargs)

    runtime.ensure_owner_recovered = selective_recovery  # type: ignore[method-assign]
    runner = ControlledRunner(
        events,
        backends={"tenant-a": object(), "tenant-b": object()},
    )

    def recover_preview_owner(
        tenant_name: str,
        tenant_config: TenantConfig,
        backend_provider: ControlledRunner,
        preview_runtime: ControlledRuntime,
    ) -> None:
        with backend_provider.acquire_backend(tenant_name, tenant_config) as backend:
            artifacts = import_module("core.preview.artifacts")
            preview_runtime.ensure_owner_recovered(
                backend=backend,
                owner=artifacts.preview_artifact_owner(tenant_name, tenant_config),
            )

    monkeypatch.setattr("core.api.app.recover_preview_owner", recover_preview_owner)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_multi_tenant_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                events.append("body")

        _run(exercise())

    assert "runtime.recover-failed:tenant-a" in events
    assert "runtime.recover:tenant-b:confluent_cloud:shared-provider-tenant" in events
    assert "body" in events

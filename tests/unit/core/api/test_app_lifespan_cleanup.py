from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from core.api.app import create_app
from core.config.models import AppSettings, PreviewConfig, StorageConfig, TenantConfig


class BodyError(RuntimeError):
    pass


def _fails(failure: str | set[str] | None, step: str) -> bool:
    return failure == step or isinstance(failure, set) and step in failure


@dataclass
class ControlledStore:
    events: list[str]
    failure: str | set[str] | None = None

    def cleanup_staging(self) -> int:
        self.events.append("store.cleanup_staging")
        if _fails(self.failure, "staging"):
            raise RuntimeError("staging cleanup failed")
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

    def ensure_staging_recovered(self) -> None:
        if self.record_recovery:
            assert self.store is not None
            self.store.cleanup_staging()

    def ensure_owner_recovered(
        self,
        *,
        backend: object,
        tenant_name: str,
        ecosystem: str,
        tenant_id: str,
    ) -> None:
        del backend
        if self.record_recovery:
            self.events.append(f"runtime.recover:{tenant_name}:{ecosystem}:{tenant_id}")
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

    def dispose(self) -> None:
        self.events.append(f"{self.name}.dispose")
        if _fails(self.failure, self.name):
            raise RuntimeError(f"{self.name} cleanup failed")


@dataclass
class ControlledRunner:
    events: list[str]
    failure: str | set[str] | None = None

    def drain(self, timeout: float) -> None:
        self.events.append(f"runner.drain({timeout})")
        if _fails(self.failure, "runner"):
            raise RuntimeError("runner cleanup failed")


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings(preview=PreviewConfig(artifact_root=tmp_path / "artifacts", max_workers=1))


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
    backends = [ControlledBackend("backend-one", events), ControlledBackend("backend-two", events)]
    runner = ControlledRunner(events)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                app.state.backends = {"one": backends[0], "two": backends[1]}
                raise BodyError("body sentinel")

        with pytest.raises(BodyError, match="body sentinel"):
            _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "backend-one.dispose",
        "backend-two.dispose",
        "runner.drain(30)",
    ]


def test_lifespan_cancellation_propagates_after_exact_ordered_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events)
    backends = [ControlledBackend("backend-one", events), ControlledBackend("backend-two", events)]
    runner = ControlledRunner(events)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                app.state.backends = {"one": backends[0], "two": backends[1]}
                raise asyncio.CancelledError("cancel sentinel")

        with pytest.raises(asyncio.CancelledError, match="cancel sentinel"):
            _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "backend-one.dispose",
        "backend-two.dispose",
        "runner.drain(30)",
    ]


def test_lifespan_body_exception_survives_multiple_cleanup_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    failures = {"runtime", "store", "backend-one", "runner"}
    store = ControlledStore(events, failures)
    runtime = ControlledRuntime(events, failures)
    backends = [
        ControlledBackend("backend-one", events, failures),
        ControlledBackend("backend-two", events, failures),
    ]
    runner = ControlledRunner(events, failures)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                app.state.backends = {"one": backends[0], "two": backends[1]}
                raise BodyError("body sentinel")

        with pytest.raises(BodyError, match="body sentinel"):
            _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "backend-one.dispose",
        "backend-two.dispose",
        "runner.drain(30)",
    ]


@pytest.mark.parametrize("failure", ["runtime", "store", "backend-one", "backend-two", "runner"])
def test_each_cleanup_failure_surfaces_and_all_later_cleanup_steps_are_attempted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    events: list[str] = []
    store = ControlledStore(events, failure)
    runtime = ControlledRuntime(events, failure)
    backends = [
        ControlledBackend("backend-one", events, failure),
        ControlledBackend("backend-two", events, failure),
    ]
    runner = ControlledRunner(events, failure)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                app.state.backends = {"one": backends[0], "two": backends[1]}

        with pytest.raises(RuntimeError, match=rf"{failure} cleanup failed"):
            _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "backend-one.dispose",
        "backend-two.dispose",
        "runner.drain(30)",
    ]


def test_startup_orphan_cleanup_failure_closes_constructed_resources_and_propagates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events)

    def fail_startup(*_args: object, **_kwargs: object) -> None:
        events.append("startup.orphan-cleanup")
        raise RuntimeError("startup cleanup failed")

    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    monkeypatch.setattr("workflow_runner.cleanup_orphaned_runs_for_all_tenants", fail_startup)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path))

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                raise AssertionError("startup failure must prevent entering the lifespan body")

        with pytest.raises(RuntimeError, match="startup cleanup failed"):
            _run(exercise())

    assert events == ["startup.orphan-cleanup", "runtime.close(wait=True)", "store.close"]


def test_lifespan_normal_exit_keeps_exact_cleanup_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = ControlledStore(events)
    runtime = ControlledRuntime(events)
    backends = [ControlledBackend("backend-one", events), ControlledBackend("backend-two", events)]
    runner = ControlledRunner(events)
    monkeypatch.setattr("core.api.app.asyncio.to_thread", _run_inline)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_settings(tmp_path), workflow_runner=runner)  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                app.state.backends = {"one": backends[0], "two": backends[1]}

        _run(exercise())

    assert events == [
        "runtime.close(wait=True)",
        "store.close",
        "backend-one.dispose",
        "backend-two.dispose",
        "runner.drain(30)",
    ]


def test_normal_startup_offloads_global_then_each_supported_tenant_in_settings_order(
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

    async def recording_to_thread(function: Any, *args: object, **kwargs: object) -> object:
        tenant = f":{args[0]}" if function.__name__ == "recover_preview_owner" else ""
        events.append(f"to_thread:{function.__name__}{tenant}")
        return function(*args, **kwargs)

    def recover_preview_owner(
        tenant_name: str,
        tenant_config: TenantConfig,
        cache: dict[str, object],
        preview_runtime: ControlledRuntime,
    ) -> None:
        backend = backends[tenant_name]
        cache[tenant_name] = backend
        events.append(f"{backend.name}.create")
        preview_runtime.ensure_owner_recovered(
            backend=backend,
            tenant_name=tenant_name,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
        )

    def orphan_cleanup(*_args: object, **_kwargs: object) -> None:
        events.append("startup.orphan-cleanup")

    monkeypatch.setattr("core.api.app.asyncio.to_thread", recording_to_thread)
    monkeypatch.setattr("workflow_runner.cleanup_orphaned_runs_for_all_tenants", orphan_cleanup)
    monkeypatch.setattr(app_module, "recover_preview_owner", recover_preview_owner, raising=False)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_multi_tenant_settings(tmp_path))

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                events.append("body")

        _run(exercise())

    assert events == [
        "to_thread:ensure_staging_recovered",
        "store.cleanup_staging",
        "to_thread:recover_preview_owner:tenant-a",
        "backend-a.create",
        "runtime.recover:tenant-a:confluent_cloud:shared-provider-tenant",
        "to_thread:recover_preview_owner:tenant-b",
        "backend-b.create",
        "runtime.recover:tenant-b:confluent_cloud:shared-provider-tenant",
        "to_thread:orphan_cleanup",
        "startup.orphan-cleanup",
        "body",
        "runtime.close(wait=True)",
        "store.close",
        "backend-a.dispose",
        "backend-b.dispose",
    ]


def test_global_recovery_failure_is_nonfatal_and_skips_owner_attempts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = import_module("core.preview.service")
    events: list[str] = []
    store = ControlledStore(events, "staging")
    runtime = ControlledRuntime(events, store=store, record_recovery=True)
    original_staging = runtime.ensure_staging_recovered

    def translated_staging() -> None:
        try:
            original_staging()
        except RuntimeError as exc:
            raise service.PreviewRecoveryUnavailable("recovery") from exc

    runtime.ensure_staging_recovered = translated_staging  # type: ignore[method-assign]

    async def recording_to_thread(function: Any, *args: object, **kwargs: object) -> object:
        events.append(f"to_thread:{function.__name__}")
        return function(*args, **kwargs)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", recording_to_thread)
    store_patch, runtime_patch = _patch_owned_resources(store, runtime)
    with store_patch, runtime_patch:
        app = create_app(_multi_tenant_settings(tmp_path), workflow_runner=ControlledRunner(events))  # type: ignore[arg-type]

        async def exercise() -> None:
            async with app.router.lifespan_context(app):
                events.append("body")

        _run(exercise())

    assert events[:3] == ["to_thread:translated_staging", "store.cleanup_staging", "body"]
    assert not any("recover_preview_owner" in event for event in events)

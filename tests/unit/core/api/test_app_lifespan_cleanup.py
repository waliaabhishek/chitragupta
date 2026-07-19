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
from core.config.models import AppSettings, PreviewConfig


class BodyError(RuntimeError):
    pass


def _fails(failure: str | set[str] | None, step: str) -> bool:
    return failure == step or isinstance(failure, set) and step in failure


@dataclass
class ControlledStore:
    events: list[str]
    failure: str | set[str] | None = None

    def close(self) -> None:
        self.events.append("store.close")
        if _fails(self.failure, "store"):
            raise RuntimeError("store cleanup failed")


@dataclass
class ControlledRuntime:
    events: list[str]
    failure: str | set[str] | None = None

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

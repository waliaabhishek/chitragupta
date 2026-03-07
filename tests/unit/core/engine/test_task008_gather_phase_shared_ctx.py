"""TASK-008: GatherPhase two-phase gather — orchestrator tests.

Verifies that GatherPhase.run() calls build_shared_context once before the
handler loop, threads the result to all gather_resources calls, and propagates
exceptions from build_shared_context (not swallowing them).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.models.identity import Identity, IdentityResolution, IdentitySet
from core.models.resource import CoreResource, Resource

# ---------------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------------

ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


def _make_tenant_config(**overrides: Any) -> Any:
    from core.config.models import TenantConfig

    defaults: dict[str, Any] = {
        "ecosystem": ECOSYSTEM,
        "tenant_id": TENANT_ID,
        "lookback_days": 30,
        "cutoff_days": 5,
    }
    defaults.update(overrides)
    return TenantConfig(**defaults)


def _make_resource(resource_id: str = "r-1") -> Resource:
    return CoreResource(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        resource_type="kafka_cluster",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _empty_resolution() -> IdentityResolution:
    return IdentityResolution(
        resource_active=IdentitySet(),
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
    )


# ---------------------------------------------------------------------------
# Fake plugin and handler that record shared_ctx calls
# ---------------------------------------------------------------------------


class _RecordingHandler:
    """A ServiceHandler that records what shared_ctx it received per call."""

    def __init__(self, resources: list[Resource] | None = None) -> None:
        self._resources = resources or []
        self.received_shared_ctx: list[object] = []

    @property
    def service_type(self) -> str:
        return "recording"

    @property
    def handles_product_types(self) -> tuple[str, ...]:
        return ()

    def gather_resources(self, tenant_id: str, uow: Any, shared_ctx: object | None = None) -> Iterable[Resource]:
        self.received_shared_ctx.append(shared_ctx)
        return iter(self._resources)

    def gather_identities(self, tenant_id: str, uow: Any) -> Iterable[Identity]:
        return iter([])

    def resolve_identities(self, *args: Any, **kwargs: Any) -> IdentityResolution:
        return _empty_resolution()

    def get_metrics_for_product_type(self, product_type: str) -> list[Any]:
        return []

    def get_allocator(self, product_type: str) -> Any:
        raise ValueError(f"Unknown: {product_type}")


class _FakePlugin:
    """A minimal EcosystemPlugin that records build_shared_context calls."""

    def __init__(self, handlers: dict[str, _RecordingHandler], shared_ctx_value: object = None) -> None:
        self._handlers = handlers
        self._shared_ctx_value = shared_ctx_value
        self.build_shared_context_call_count = 0

    @property
    def ecosystem(self) -> str:
        return ECOSYSTEM

    def initialize(self, config: dict[str, Any]) -> None:
        pass

    def get_service_handlers(self) -> dict[str, _RecordingHandler]:
        return self._handlers

    def get_cost_input(self) -> Any:

        cost_input = MagicMock()
        cost_input.get_line_items.return_value = iter([])
        return cost_input

    def get_metrics_source(self) -> None:
        return None

    def build_shared_context(self, tenant_id: str) -> object:
        self.build_shared_context_call_count += 1
        return self._shared_ctx_value

    def close(self) -> None:
        pass


class _RaisingPlugin(_FakePlugin):
    """A plugin whose build_shared_context raises an exception."""

    def build_shared_context(self, tenant_id: str) -> object:
        self.build_shared_context_call_count += 1
        raise RuntimeError("CCloud API unavailable")


def _make_gather_phase(
    plugin: Any,
    uow_storage: Any | None = None,
) -> Any:
    from core.engine.orchestrator import GatherPhase
    from core.plugin.registry import EcosystemBundle

    bundle = EcosystemBundle.build(plugin)

    if uow_storage is None:
        uow_storage = MagicMock()
        uow_storage.__enter__ = MagicMock(return_value=MagicMock())
        uow_storage.__exit__ = MagicMock(return_value=False)

    tenant_config = _make_tenant_config()

    return GatherPhase(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        bundle=bundle,
        uow_storage=uow_storage,
        tenant_config=tenant_config,
        gather_failure_threshold=10,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGatherPhaseCallsBuildSharedContextOnce:
    """build_shared_context is called exactly once per gather cycle."""

    def test_build_shared_context_called_once_before_handler_loop(self) -> None:
        """GatherPhase.run() calls plugin.build_shared_context exactly once."""
        handler1 = _RecordingHandler()
        handler2 = _RecordingHandler()
        sentinel = object()

        plugin = _FakePlugin(
            handlers={"h1": handler1, "h2": handler2},
            shared_ctx_value=sentinel,
        )
        phase = _make_gather_phase(plugin)

        phase.run()

        assert plugin.build_shared_context_call_count == 1

    def test_shared_ctx_threaded_to_all_handlers(self) -> None:
        """Every handler's gather_resources receives the same shared_ctx object."""
        handler1 = _RecordingHandler()
        handler2 = _RecordingHandler()
        sentinel = object()

        plugin = _FakePlugin(
            handlers={"h1": handler1, "h2": handler2},
            shared_ctx_value=sentinel,
        )
        phase = _make_gather_phase(plugin)

        phase.run()

        assert len(handler1.received_shared_ctx) == 1
        assert handler1.received_shared_ctx[0] is sentinel
        assert len(handler2.received_shared_ctx) == 1
        assert handler2.received_shared_ctx[0] is sentinel

    def test_shared_ctx_same_object_for_all_handlers(self) -> None:
        """All handlers receive the identical shared_ctx object (identity check)."""
        handler_a = _RecordingHandler()
        handler_b = _RecordingHandler()
        handler_c = _RecordingHandler()
        sentinel = {"unique": True}

        plugin = _FakePlugin(
            handlers={"a": handler_a, "b": handler_b, "c": handler_c},
            shared_ctx_value=sentinel,
        )
        phase = _make_gather_phase(plugin)

        phase.run()

        assert handler_a.received_shared_ctx[0] is sentinel
        assert handler_b.received_shared_ctx[0] is sentinel
        assert handler_c.received_shared_ctx[0] is sentinel


class TestGatherPhaseBuildSharedContextFailurePropagates:
    """build_shared_context exception is NOT swallowed — it propagates to the outer handler."""

    def test_build_shared_context_exception_propagates(self) -> None:
        """When build_shared_context raises, GatherPhase.run() propagates the exception."""
        handler = _RecordingHandler()
        plugin = _RaisingPlugin(handlers={"h1": handler})
        phase = _make_gather_phase(plugin)

        with pytest.raises(RuntimeError, match="CCloud API unavailable"):
            phase.run()

    def test_handler_gather_not_called_when_build_shared_context_fails(self) -> None:
        """No handler gather_resources is called when Phase 1 fails."""
        handler = _RecordingHandler()
        plugin = _RaisingPlugin(handlers={"h1": handler})
        phase = _make_gather_phase(plugin)

        with contextlib.suppress(RuntimeError):
            phase.run()

        assert len(handler.received_shared_ctx) == 0

    def test_build_shared_context_called_before_handler_loop(self) -> None:
        """build_shared_context is called exactly once even when it raises."""
        handler = _RecordingHandler()
        plugin = _RaisingPlugin(handlers={"h1": handler})
        phase = _make_gather_phase(plugin)

        with contextlib.suppress(RuntimeError):
            phase.run()

        assert plugin.build_shared_context_call_count == 1

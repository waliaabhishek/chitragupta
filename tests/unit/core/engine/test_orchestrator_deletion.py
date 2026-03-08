from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from core.engine.orchestrator import ChargebackOrchestrator

if TYPE_CHECKING:
    import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NOW = datetime(2026, 3, 3, 12, 0, 0, tzinfo=UTC)
ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"
TENANT_NAME = "test-tenant"

# ---------------------------------------------------------------------------
# Minimal domain stubs
# ---------------------------------------------------------------------------


class _FakeResource:
    def __init__(self, resource_id: str) -> None:
        self.resource_id = resource_id
        self.deleted_at: datetime | None = None


class _FakeIdentity:
    def __init__(self, identity_id: str) -> None:
        self.identity_id = identity_id
        self.deleted_at: datetime | None = None
        self.identity_type = "user"
        self.display_name = f"User {identity_id}"


class _MockEntityRepo:
    """Minimal repo implementing _EntityRepo protocol for _detect_entity_deletions."""

    def __init__(self, active_ids: list[str]) -> None:
        self._active_ids = active_ids
        self.mark_deleted_calls: list[tuple[str, datetime]] = []

    def find_active_at(
        self, ecosystem: str, tenant_id: str, timestamp: datetime, **kwargs: Any
    ) -> tuple[list[_FakeResource], int]:
        entities = [_FakeResource(eid) for eid in self._active_ids]
        return entities, len(entities)

    def mark_deleted(self, ecosystem: str, tenant_id: str, entity_id: str, deleted_at: datetime) -> None:
        self.mark_deleted_calls.append((entity_id, deleted_at))


# ---------------------------------------------------------------------------
# Mock UoW / storage so orchestrator.__init__ succeeds
# ---------------------------------------------------------------------------


class _MockIdentityRepo:
    """Identity repo used by orchestrator.__init__ (UNALLOCATED upsert)."""

    def __init__(self) -> None:
        self._data: dict[str, _FakeIdentity] = {}
        self._deletions: list[tuple[str, datetime]] = []

    def upsert(self, identity: Any) -> Any:
        self._data[identity.identity_id] = identity
        return identity

    def get(self, ecosystem: str, tenant_id: str, identity_id: str) -> Any:
        return self._data.get(identity_id)

    def find_active_at(
        self, ecosystem: str, tenant_id: str, timestamp: datetime, **kwargs: Any
    ) -> tuple[list[Any], int]:
        items = [i for i in self._data.values() if i.deleted_at is None]
        return items, len(items)

    def mark_deleted(self, ecosystem: str, tenant_id: str, identity_id: str, deleted_at: datetime) -> None:
        self._deletions.append((identity_id, deleted_at))


class _MockResourceRepo:
    """Resource repo used by orchestrator.__init__."""

    def __init__(self) -> None:
        self._data: dict[str, _FakeResource] = {}
        self._deletions: list[tuple[str, datetime]] = []

    def upsert(self, resource: Any) -> Any:
        self._data[resource.resource_id] = resource
        return resource

    def find_active_at(
        self, ecosystem: str, tenant_id: str, timestamp: datetime, **kwargs: Any
    ) -> tuple[list[Any], int]:
        items = [r for r in self._data.values() if r.deleted_at is None]
        return items, len(items)

    def mark_deleted(self, ecosystem: str, tenant_id: str, resource_id: str, deleted_at: datetime) -> None:
        self._deletions.append((resource_id, deleted_at))


class _MockUoW:
    def __init__(self) -> None:
        self.resources = _MockResourceRepo()
        self.identities = _MockIdentityRepo()
        self.billing = MagicMock()
        self.chargebacks = MagicMock()
        self.pipeline_state = MagicMock()
        self.tags = MagicMock()

    def __enter__(self) -> _MockUoW:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


class _MockStorageBackend:
    def __init__(self) -> None:
        self._uow = _MockUoW()

    def create_unit_of_work(self) -> _MockUoW:
        return self._uow

    def create_tables(self) -> None:
        pass

    def dispose(self) -> None:
        pass


class _MockPlugin:
    @property
    def ecosystem(self) -> str:
        return ECOSYSTEM

    def initialize(self, config: dict[str, Any]) -> None:
        pass

    def get_service_handlers(self) -> dict[str, Any]:
        return {}

    def get_cost_input(self) -> Any:
        return MagicMock()

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


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


def _make_orchestrator(**config_overrides: Any) -> ChargebackOrchestrator:
    tc = _make_tenant_config(**config_overrides)
    storage = _MockStorageBackend()
    plugin = _MockPlugin()
    return ChargebackOrchestrator(TENANT_NAME, tc, plugin, storage)


# ---------------------------------------------------------------------------
# Tests: _zero_gather_counters attribute (replaces two separate counters)
# ---------------------------------------------------------------------------


class TestZeroGatherCountersAttribute:
    """_zero_gather_counters dict must replace the two separate counter attrs."""

    def test_zero_gather_counters_exists(self) -> None:
        orch = _make_orchestrator()
        assert hasattr(orch, "_zero_gather_counters"), "_zero_gather_counters dict not found; refactor not applied"

    def test_zero_gather_counters_has_resources_key(self) -> None:
        orch = _make_orchestrator()
        assert "resources" in orch._zero_gather_counters  # type: ignore[attr-defined]

    def test_zero_gather_counters_has_identities_key(self) -> None:
        orch = _make_orchestrator()
        assert "identities" in orch._zero_gather_counters  # type: ignore[attr-defined]

    def test_zero_gather_counters_initial_values_zero(self) -> None:
        orch = _make_orchestrator()
        assert orch._zero_gather_counters == {"resources": 0, "identities": 0}  # type: ignore[attr-defined]

    def test_old_consecutive_resource_attr_gone(self) -> None:
        orch = _make_orchestrator()
        assert not hasattr(orch, "_consecutive_zero_resource_gathers"), (
            "Old _consecutive_zero_resource_gathers attr still present; not refactored"
        )

    def test_old_consecutive_identity_attr_gone(self) -> None:
        orch = _make_orchestrator()
        assert not hasattr(orch, "_consecutive_zero_identity_gathers"), (
            "Old _consecutive_zero_identity_gathers attr still present; not refactored"
        )


# ---------------------------------------------------------------------------
# Tests: _detect_entity_deletions method exists
# ---------------------------------------------------------------------------


class TestDetectEntityDeletionsExists:
    def test_method_exists(self) -> None:
        orch = _make_orchestrator()
        assert hasattr(orch, "_detect_entity_deletions"), (
            "_detect_entity_deletions method not found; refactor not applied"
        )

    def test_method_is_callable(self) -> None:
        orch = _make_orchestrator()
        assert callable(orch._detect_entity_deletions)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Tests: zero-gather below threshold — counter increments, no deletions
# ---------------------------------------------------------------------------


class TestZeroGatherBelowThreshold:
    """Zero gathered + active entities + consecutive < threshold → skip deletions."""

    def test_resources_counter_increments(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=3)
        repo = _MockEntityRepo(active_ids=["r1", "r2"])
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        assert orch._zero_gather_counters["resources"] == 1  # type: ignore[attr-defined]

    def test_no_mark_deleted_called_below_threshold(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=3)
        repo = _MockEntityRepo(active_ids=["r1"])
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        assert repo.mark_deleted_calls == []

    def test_identity_counter_unaffected_by_resource_zero_gather(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=3)
        repo = _MockEntityRepo(active_ids=["r1"])
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        assert orch._zero_gather_counters["identities"] == 0  # type: ignore[attr-defined]

    def test_counter_increments_across_calls(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=5)
        repo = _MockEntityRepo(active_ids=["r1"])
        for _ in range(3):
            orch._detect_entity_deletions(  # type: ignore[attr-defined]
                repo=repo,
                gathered_ids=set(),
                entity_name="resources",
                id_getter=lambda e: e.resource_id,
                now=NOW,
            )
        assert orch._zero_gather_counters["resources"] == 3  # type: ignore[attr-defined]

    def test_skip_log_uses_plural_entity_name(self, caplog: pytest.LogCaptureFixture) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=3)
        repo = _MockEntityRepo(active_ids=["r1"])
        with caplog.at_level(logging.WARNING, logger="core.engine.orchestrator"):
            orch._detect_entity_deletions(  # type: ignore[attr-defined]
                repo=repo,
                gathered_ids=set(),
                entity_name="resources",
                id_getter=lambda e: e.resource_id,
                now=NOW,
            )
        # New plural format: "skipping resources deletion" (not "skipping resource deletion")
        assert any("skipping resources deletion" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Tests: zero-gather at or above threshold → deletions triggered
# ---------------------------------------------------------------------------


class TestZeroGatherAtThreshold:
    """Zero gathered + consecutive >= threshold → mark_deleted called, counter resets."""

    def test_mark_deleted_called_at_threshold(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=2)
        repo = _MockEntityRepo(active_ids=["r1", "r2"])
        # Prime counter to threshold - 1
        orch._zero_gather_counters["resources"] = 1  # type: ignore[attr-defined]
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        deleted_ids = [call[0] for call in repo.mark_deleted_calls]
        assert set(deleted_ids) == {"r1", "r2"}

    def test_counter_resets_to_zero_after_deletion(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=2)
        repo = _MockEntityRepo(active_ids=["r1"])
        orch._zero_gather_counters["resources"] = 1  # type: ignore[attr-defined]
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        assert orch._zero_gather_counters["resources"] == 0  # type: ignore[attr-defined]

    def test_only_absent_entities_deleted_at_threshold(self) -> None:
        """Entities in gathered_ids must NOT be deleted even at threshold."""
        # Note: zero-gather path requires gathered_ids == empty, so this scenario
        # is for the normal-gather path. But at threshold with gathered_ids empty,
        # ALL active entities are absent → all deleted.
        orch = _make_orchestrator(zero_gather_deletion_threshold=1)
        repo = _MockEntityRepo(active_ids=["r1", "r2", "r3"])
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        deleted_ids = {call[0] for call in repo.mark_deleted_calls}
        assert deleted_ids == {"r1", "r2", "r3"}

    def test_proceeding_log_uses_plural_entity_name(self, caplog: pytest.LogCaptureFixture) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=1)
        repo = _MockEntityRepo(active_ids=["r1"])
        with caplog.at_level(logging.WARNING, logger="core.engine.orchestrator"):
            orch._detect_entity_deletions(  # type: ignore[attr-defined]
                repo=repo,
                gathered_ids=set(),
                entity_name="resources",
                id_getter=lambda e: e.resource_id,
                now=NOW,
            )
        # New plural format: "Zero resources gathered for..."
        assert any("Zero resources gathered for" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Tests: threshold == -1 → never delete on zero gather
# ---------------------------------------------------------------------------


class TestThresholdDisabled:
    """When zero_gather_deletion_threshold == -1, zero gather never triggers deletion."""

    def test_no_deletion_regardless_of_consecutive_count(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=-1)
        repo = _MockEntityRepo(active_ids=["r1", "r2"])
        # Simulate many consecutive zero-gather runs
        for _ in range(100):
            orch._detect_entity_deletions(  # type: ignore[attr-defined]
                repo=repo,
                gathered_ids=set(),
                entity_name="resources",
                id_getter=lambda e: e.resource_id,
                now=NOW,
            )
        assert repo.mark_deleted_calls == []

    def test_counter_still_increments_with_threshold_minus_one(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=-1)
        repo = _MockEntityRepo(active_ids=["r1"])
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        assert orch._zero_gather_counters["resources"] == 1  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Tests: normal gather (non-empty gathered_ids)
# ---------------------------------------------------------------------------


class TestNormalGather:
    """Non-empty gathered_ids → only absent entities marked deleted, counter resets."""

    def test_only_absent_entities_deleted(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=3)
        repo = _MockEntityRepo(active_ids=["r1", "r2", "r3"])
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids={"r1"},  # r2, r3 absent
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        deleted_ids = {call[0] for call in repo.mark_deleted_calls}
        assert deleted_ids == {"r2", "r3"}

    def test_counter_resets_on_normal_gather(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=3)
        repo = _MockEntityRepo(active_ids=["r1"])
        # Prime counter
        orch._zero_gather_counters["resources"] = 2  # type: ignore[attr-defined]
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids={"r1"},  # non-empty
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        assert orch._zero_gather_counters["resources"] == 0  # type: ignore[attr-defined]

    def test_gathered_entity_not_deleted(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=3)
        repo = _MockEntityRepo(active_ids=["r1", "r2"])
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids={"r1", "r2"},  # all present
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        assert repo.mark_deleted_calls == []

    def test_all_gathered_no_active_nothing_deleted(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=3)
        repo = _MockEntityRepo(active_ids=[])  # nothing active
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=repo,
            gathered_ids={"r1"},
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        assert repo.mark_deleted_calls == []


# ---------------------------------------------------------------------------
# Tests: counters are independent
# ---------------------------------------------------------------------------


class TestCountersAreIndependent:
    """Resource counter reaching threshold must not affect identity counter."""

    def test_resource_at_threshold_identity_counter_unchanged(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=2)
        resource_repo = _MockEntityRepo(active_ids=["r1"])
        # Prime resource counter to threshold - 1
        orch._zero_gather_counters["resources"] = 1  # type: ignore[attr-defined]
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=resource_repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        # Resources should have triggered deletion and reset
        assert orch._zero_gather_counters["resources"] == 0  # type: ignore[attr-defined]
        # Identities counter must remain untouched
        assert orch._zero_gather_counters["identities"] == 0  # type: ignore[attr-defined]

    def test_identity_counter_increments_independently(self) -> None:
        orch = _make_orchestrator(zero_gather_deletion_threshold=5)
        resource_repo = _MockEntityRepo(active_ids=["r1"])
        identity_repo = _MockEntityRepo(active_ids=["i1"])
        # Advance resource counter by 2
        for _ in range(2):
            orch._detect_entity_deletions(  # type: ignore[attr-defined]
                repo=resource_repo,
                gathered_ids=set(),
                entity_name="resources",
                id_getter=lambda e: e.resource_id,
                now=NOW,
            )
        # Advance identity counter by 1
        orch._detect_entity_deletions(  # type: ignore[attr-defined]
            repo=identity_repo,
            gathered_ids=set(),
            entity_name="identities",
            id_getter=lambda e: e.resource_id,
            now=NOW,
        )
        assert orch._zero_gather_counters["resources"] == 2  # type: ignore[attr-defined]
        assert orch._zero_gather_counters["identities"] == 1  # type: ignore[attr-defined]

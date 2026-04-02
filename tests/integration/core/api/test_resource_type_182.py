"""Integration tests for TASK-182: resource_type filtering mandatory.

Tests 5, 6, 7 from the design doc Verification section.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from core.models.resource import CoreResource, ResourceStatus

if TYPE_CHECKING:
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

ECOSYSTEM = "test-eco"
TENANT_ID = "test-tenant"
TENANT_NAME = "test-tenant"
NOW = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)


def _make_resource(**overrides: Any) -> CoreResource:
    defaults: dict[str, Any] = dict(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id="r1",
        resource_type="kafka_cluster",
        status=ResourceStatus.ACTIVE,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        metadata={},
    )
    defaults.update(overrides)
    return CoreResource(**defaults)


def _make_tenant_config(**overrides: Any) -> Any:
    from core.config.models import TenantConfig

    defaults: dict[str, Any] = {
        "ecosystem": ECOSYSTEM,
        "tenant_id": TENANT_ID,
        "lookback_days": 30,
        "cutoff_days": 5,
        "zero_gather_deletion_threshold": 0,
    }
    defaults.update(overrides)
    return TenantConfig(**defaults)


class _StubPlugin:
    @property
    def ecosystem(self) -> str:
        return ECOSYSTEM

    def initialize(self, config: dict[str, Any]) -> None:
        pass

    def get_service_handlers(self) -> dict[str, Any]:
        return {}

    def get_cost_input(self) -> Any:
        from unittest.mock import MagicMock

        return MagicMock()

    def get_metrics_source(self) -> None:
        return None

    def get_fallback_allocator(self) -> None:
        return None

    def build_shared_context(self, tenant_id: str) -> None:
        return None

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Test 5: Integration — deletion detection with resource_type filter
# ---------------------------------------------------------------------------


class TestResourceDeletionIntegration:
    """Seeded DB with topic + kafka_cluster; deletion detection with kafka_cluster filter
    must NOT mark topics deleted."""

    def test_topics_not_deleted_when_gathering_kafka_clusters(self, in_memory_backend: SQLModelBackend) -> None:
        from core.engine.orchestrator import ChargebackOrchestrator

        # Seed DB with a kafka_cluster and a topic resource
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(_make_resource(resource_id="kc-1", resource_type="kafka_cluster"))
            uow.resources.upsert(_make_resource(resource_id="topic-1", resource_type="topic"))
            uow.commit()

        # Create orchestrator with stub plugin
        tc = _make_tenant_config()
        orch = ChargebackOrchestrator(TENANT_NAME, tc, _StubPlugin(), in_memory_backend)

        # Call _detect_resource_deletions scoped to kafka_cluster only
        # kc-1 was gathered; topic-1 is never in gathered_ids (overlay type)
        with in_memory_backend.create_unit_of_work() as uow:
            orch._gather_phase._detect_resource_deletions(  # type: ignore[attr-defined]
                repo=uow.resources,
                gathered_ids={"kc-1"},
                now=NOW,
                resource_types=["kafka_cluster"],
            )
            uow.commit()

        # topic-1 must NOT be marked deleted
        with in_memory_backend.create_unit_of_work() as uow:
            topic = uow.resources.get(ECOSYSTEM, TENANT_ID, "topic-1")
            assert topic is not None
            assert topic.deleted_at is None, "topic was falsely marked deleted"

        # kc-1 was gathered, must also not be deleted
        with in_memory_backend.create_unit_of_work() as uow:
            kc = uow.resources.get(ECOSYSTEM, TENANT_ID, "kc-1")
            assert kc is not None
            assert kc.deleted_at is None

    def test_absent_kafka_cluster_is_deleted_not_topic(self, in_memory_backend: SQLModelBackend) -> None:
        """kafka_cluster not in gathered_ids → marked deleted. topic untouched."""
        from core.engine.orchestrator import ChargebackOrchestrator

        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(_make_resource(resource_id="kc-stale", resource_type="kafka_cluster"))
            uow.resources.upsert(_make_resource(resource_id="topic-alive", resource_type="topic"))
            uow.commit()

        tc = _make_tenant_config()
        orch = ChargebackOrchestrator(TENANT_NAME, tc, _StubPlugin(), in_memory_backend)

        with in_memory_backend.create_unit_of_work() as uow:
            orch._gather_phase._detect_resource_deletions(  # type: ignore[attr-defined]
                repo=uow.resources,
                gathered_ids=set(),  # nothing gathered → kc-stale should be marked deleted
                now=NOW,
                resource_types=["kafka_cluster"],
            )
            uow.commit()

        with in_memory_backend.create_unit_of_work() as uow:
            # topic untouched
            topic = uow.resources.get(ECOSYSTEM, TENANT_ID, "topic-alive")
            assert topic is not None
            assert topic.deleted_at is None, "topic falsely deleted"

            # kc-stale marked deleted (gathered_ids was empty, threshold=0 default)
            kc = uow.resources.get(ECOSYSTEM, TENANT_ID, "kc-stale")
            assert kc is not None
            assert kc.deleted_at == NOW, "stale kafka_cluster not marked deleted"


# ---------------------------------------------------------------------------
# Test 6: Integration — ResourceRowFetcher with resource_types returns only matching rows
# ---------------------------------------------------------------------------


class TestResourceRowFetcherIntegration:
    """ResourceRowFetcher(storage, ["kafka_cluster"]).fetch_by_date returns only kafka_cluster rows."""

    def test_fetch_by_date_returns_only_specified_type(self, in_memory_backend: SQLModelBackend) -> None:
        from core.emitters.sources import ResourceRowFetcher

        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(_make_resource(resource_id="kc-1", resource_type="kafka_cluster"))
            uow.resources.upsert(_make_resource(resource_id="topic-1", resource_type="topic"))
            uow.commit()

        fetcher = ResourceRowFetcher(in_memory_backend, ["kafka_cluster"])
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, date(2026, 6, 1))

        resource_types_returned = {r.resource_type for r in rows}
        assert "topic" not in resource_types_returned
        assert "kafka_cluster" in resource_types_returned
        assert len(rows) == 1

    def test_fetch_by_date_no_topics_in_results(self, in_memory_backend: SQLModelBackend) -> None:
        from core.emitters.sources import ResourceRowFetcher

        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(3):
                uow.resources.upsert(_make_resource(resource_id=f"topic-{i}", resource_type="topic"))
            uow.resources.upsert(_make_resource(resource_id="kc-1", resource_type="kafka_cluster"))
            uow.commit()

        fetcher = ResourceRowFetcher(in_memory_backend, ["kafka_cluster"])
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, date(2026, 6, 1))

        assert all(r.resource_type == "kafka_cluster" for r in rows)
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# Test 7: Integration — _build_resource_cache loads only billing types
# ---------------------------------------------------------------------------


class TestBuildResourceCacheIntegration:
    """_build_resource_cache must scope find_by_period to billing types, excluding topics."""

    def test_build_resource_cache_excludes_topics(self, in_memory_backend: SQLModelBackend) -> None:
        """Cache built with kafka_cluster billing types must not contain topic entries."""
        from unittest.mock import patch

        from core.engine.orchestrator import ChargebackOrchestrator

        created_at = datetime(2026, 1, 1, tzinfo=UTC)
        b_start = datetime(2026, 2, 1, tzinfo=UTC)
        b_end = datetime(2026, 3, 1, tzinfo=UTC)

        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                _make_resource(
                    resource_id="kc-1",
                    resource_type="kafka_cluster",
                    created_at=created_at,
                )
            )
            uow.resources.upsert(
                _make_resource(
                    resource_id="topic-1",
                    resource_type="topic",
                    created_at=created_at,
                )
            )
            uow.commit()

        tc = _make_tenant_config()
        orch = ChargebackOrchestrator(TENANT_NAME, tc, _StubPlugin(), in_memory_backend)

        windows: set[tuple[datetime, datetime]] = {(b_start, b_end)}

        with (
            in_memory_backend.create_unit_of_work() as uow,
            patch.object(
                type(orch._gather_phase._bundle),
                "billing_resource_types",
                new_callable=lambda: property(lambda self: ["kafka_cluster"]),
            ),
        ):
            cache = orch._calculate_phase._build_resource_cache(uow, windows)

        assert (b_start, b_end) in cache
        resource_ids = set(cache[(b_start, b_end)].keys())
        assert "kc-1" in resource_ids
        assert "topic-1" not in resource_ids, "topic was included in billing resource cache"

    def test_build_resource_cache_passes_resource_type_to_find_by_period(
        self, in_memory_backend: SQLModelBackend
    ) -> None:
        """Spy that billing_resource_types is forwarded as resource_type to find_by_period."""
        from unittest.mock import patch

        from core.engine.orchestrator import ChargebackOrchestrator

        b_start = datetime(2026, 2, 1, tzinfo=UTC)
        b_end = datetime(2026, 3, 1, tzinfo=UTC)

        tc = _make_tenant_config()
        orch = ChargebackOrchestrator(TENANT_NAME, tc, _StubPlugin(), in_memory_backend)

        windows: set[tuple[datetime, datetime]] = {(b_start, b_end)}

        captured_kwargs: list[dict[str, Any]] = []

        with in_memory_backend.create_unit_of_work() as uow:
            original = uow.resources.find_by_period

            def spy_find_by_period(*args: Any, **kwargs: Any) -> Any:
                captured_kwargs.append(kwargs)
                return original(*args, **kwargs)

            uow.resources.find_by_period = spy_find_by_period  # type: ignore[method-assign]

            with patch.object(
                type(orch._gather_phase._bundle),
                "billing_resource_types",
                new_callable=lambda: property(lambda self: ["kafka_cluster"]),
            ):
                orch._calculate_phase._build_resource_cache(uow, windows)

        assert len(captured_kwargs) == 1
        assert captured_kwargs[0].get("resource_type") == ["kafka_cluster"]

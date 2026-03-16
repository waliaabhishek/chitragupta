from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

from core.models import SENTINEL_IDENTITY_TYPES
from core.models.identity import CoreIdentity, Identity, IdentityResolution, IdentitySet

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


def _make_id(identity_id: str, **kw: object) -> Identity:
    defaults = {
        "ecosystem": "confluent",
        "tenant_id": "t-001",
        "identity_id": identity_id,
        "identity_type": "user",
    }
    defaults.update(kw)
    return CoreIdentity(**defaults)  # type: ignore[arg-type] -- dict[str, object] from **kw merge


class TestIdentity:
    def test_construction(self) -> None:
        i = CoreIdentity(
            ecosystem="confluent",
            tenant_id="t-001",
            identity_id="u-1",
            identity_type="user",
            display_name="Alice",
            created_at=_NOW,
            deleted_at=None,
            last_seen_at=_NOW,
            metadata={"role": "admin"},
        )
        assert i.ecosystem == "confluent"
        assert i.identity_id == "u-1"
        assert i.display_name == "Alice"
        assert i.metadata == {"role": "admin"}

    def test_asdict_round_trip(self) -> None:
        i = _make_id("u-1", display_name="Bob", created_at=_NOW)
        d = asdict(i)
        i2 = CoreIdentity(**d)
        assert i == i2


class TestIdentitySet:
    def test_add_and_get(self) -> None:
        s = IdentitySet()
        i = _make_id("u-1")
        s.add(i)
        assert s.get("u-1") is i

    def test_get_missing(self) -> None:
        s = IdentitySet()
        assert s.get("nonexistent") is None

    def test_ids(self) -> None:
        s = IdentitySet()
        s.add(_make_id("u-1"))
        s.add(_make_id("u-2"))
        assert s.ids() == frozenset({"u-1", "u-2"})

    def test_ids_returns_frozenset(self) -> None:
        s = IdentitySet()
        s.add(_make_id("u-1"))
        result = s.ids()
        assert isinstance(result, frozenset)

    def test_len(self) -> None:
        s = IdentitySet()
        assert len(s) == 0
        s.add(_make_id("u-1"))
        assert len(s) == 1

    def test_iter(self) -> None:
        s = IdentitySet()
        i1 = _make_id("u-1")
        i2 = _make_id("u-2")
        s.add(i1)
        s.add(i2)
        items = list(s)
        assert len(items) == 2
        assert i1 in items
        assert i2 in items

    def test_bool_empty(self) -> None:
        s = IdentitySet()
        assert not s
        assert bool(s) is False

    def test_bool_nonempty(self) -> None:
        s = IdentitySet()
        s.add(_make_id("u-1"))
        assert s
        assert bool(s) is True

    def test_contains(self) -> None:
        s = IdentitySet()
        s.add(_make_id("u-1"))
        assert "u-1" in s
        assert "u-2" not in s

    def test_duplicate_add_last_write_wins(self) -> None:
        s = IdentitySet()
        first = _make_id("u-1", display_name="First")
        second = _make_id("u-1", display_name="Second")
        s.add(first)
        s.add(second)
        assert len(s) == 1
        assert s.get("u-1") is second
        assert s.get("u-1").display_name == "Second"

    def test_empty_state(self) -> None:
        s = IdentitySet()
        assert len(s) == 0
        assert not s
        assert list(s) == []
        assert s.ids() == frozenset()


class TestIdentitySetIdsByType:
    def test_ids_by_type_single_type_matches(self) -> None:
        s = IdentitySet()
        s.add(_make_id("sa-1", identity_type="service_account"))
        s.add(_make_id("u-1", identity_type="user"))
        result = s.ids_by_type("service_account")
        assert result == frozenset({"sa-1"})

    def test_ids_by_type_multiple_types(self) -> None:
        s = IdentitySet()
        s.add(_make_id("sa-1", identity_type="service_account"))
        s.add(_make_id("u-1", identity_type="user"))
        s.add(_make_id("key-1", identity_type="api_key"))
        result = s.ids_by_type("service_account", "user")
        assert result == frozenset({"sa-1", "u-1"})

    def test_ids_by_type_no_matches_returns_empty(self) -> None:
        s = IdentitySet()
        s.add(_make_id("sa-1", identity_type="service_account"))
        result = s.ids_by_type("identity_pool")
        assert result == frozenset()

    def test_ids_by_type_excludes_api_key_and_system(self) -> None:
        s = IdentitySet()
        s.add(_make_id("sa-1", identity_type="service_account"))
        s.add(_make_id("pool-1", identity_type="identity_pool"))
        s.add(_make_id("key-1", identity_type="api_key"))
        s.add(_make_id("sys-1", identity_type="system"))
        result = s.ids_by_type("service_account", "user", "identity_pool")
        assert result == frozenset({"sa-1", "pool-1"})

    def test_ids_by_type_returns_frozenset(self) -> None:
        s = IdentitySet()
        s.add(_make_id("sa-1", identity_type="service_account"))
        result = s.ids_by_type("service_account")
        assert isinstance(result, frozenset)

    def test_ids_by_type_empty_set_returns_empty(self) -> None:
        s = IdentitySet()
        result = s.ids_by_type("service_account")
        assert result == frozenset()


class TestIdentityResolution:
    def test_construction(self) -> None:
        ra = IdentitySet()
        md = IdentitySet()
        tp = IdentitySet()
        ir = IdentityResolution(
            resource_active=ra,
            metrics_derived=md,
            tenant_period=tp,
            context={"source": "test"},
        )
        assert ir.resource_active is ra
        assert ir.metrics_derived is md
        assert ir.tenant_period is tp
        assert ir.context == {"source": "test"}

    def test_context_default(self) -> None:
        ir = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        assert ir.context == {}

    def test_merged_active_disjoint(self) -> None:
        ra = IdentitySet()
        md = IdentitySet()
        ra.add(_make_id("u-1"))
        md.add(_make_id("u-2"))
        ir = IdentityResolution(
            resource_active=ra,
            metrics_derived=md,
            tenant_period=IdentitySet(),
        )
        merged = ir.merged_active
        assert len(merged) == 2
        assert "u-1" in merged
        assert "u-2" in merged

    def test_merged_active_overlapping_metrics_wins(self) -> None:
        ra = IdentitySet()
        md = IdentitySet()
        ra_identity = _make_id("u-1", display_name="FromResource")
        md_identity = _make_id("u-1", display_name="FromMetrics")
        ra.add(ra_identity)
        md.add(md_identity)
        ir = IdentityResolution(
            resource_active=ra,
            metrics_derived=md,
            tenant_period=IdentitySet(),
        )
        merged = ir.merged_active
        assert len(merged) == 1
        assert merged.get("u-1").display_name == "FromMetrics"

    def test_merged_active_empty(self) -> None:
        ir = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        merged = ir.merged_active
        assert len(merged) == 0
        assert not merged

    def test_merged_active_returns_new_set(self) -> None:
        ra = IdentitySet()
        ra.add(_make_id("u-1"))
        ir = IdentityResolution(
            resource_active=ra,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        m1 = ir.merged_active
        m2 = ir.merged_active
        assert m1 is not m2
        assert m1 is not ir.resource_active


class TestSentinelIdentityTypes:
    def test_sentinel_identity_types_contains_system(self) -> None:
        assert "system" in SENTINEL_IDENTITY_TYPES

    def test_sentinel_identity_types_is_tuple(self) -> None:
        assert isinstance(SENTINEL_IDENTITY_TYPES, tuple)

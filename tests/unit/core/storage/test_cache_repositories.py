from __future__ import annotations

import time
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.identity import CoreIdentity, Identity
from core.models.resource import CoreResource, Resource, ResourceStatus
from core.storage.backends.sqlmodel.repositories import (
    SQLModelIdentityRepository,
    SQLModelResourceRepository,
)


@pytest.fixture
def session() -> Generator[Session]:
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose(close=True)


def _make_identity(**overrides: Any) -> Identity:
    defaults = dict(
        ecosystem="eco",
        tenant_id="t1",
        identity_id="sa-001",
        identity_type="service_account",
        display_name="Test SA",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        metadata={},
    )
    defaults.update(overrides)
    return CoreIdentity(**defaults)


def _make_resource(**overrides: Any) -> Resource:
    defaults = dict(
        ecosystem="eco",
        tenant_id="t1",
        resource_id="lkc-001",
        resource_type="kafka",
        status=ResourceStatus.ACTIVE,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        metadata={},
    )
    defaults.update(overrides)
    return CoreResource(**defaults)


# ---------------------------------------------------------------------------
# Identity repository cache tests
# ---------------------------------------------------------------------------
# Strategy: expire_all() clears SQLAlchemy's session identity map, forcing
# any subsequent session.get() to actually query the DB. If the repo-level
# TTLCache is present, session.get() is bypassed entirely (call_count == 0).
# Without TTLCache, session.get() is always called (call_count >= 1).
# ---------------------------------------------------------------------------


class TestIdentityRepositoryCache:
    def test_identity_get_caches_db_call(self, session: Session) -> None:
        """After first get(), a second get() for the same key must NOT hit the DB."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(_make_identity())
        session.commit()

        # Populate the repo-level cache
        result1 = repo.get("eco", "t1", "sa-001")
        assert result1 is not None

        # Expire SQLAlchemy identity map so the only cache is the repo-level TTLCache
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            result2 = repo.get("eco", "t1", "sa-001")
            # With no repo-level cache: session.get() would be called (count == 1). RED state.
            assert mock_get.call_count == 0  # cache hit — DB must not be queried

        assert result2 is not None
        assert result2.identity_id == "sa-001"

    def test_identity_get_nonexistent_caches_none(self, session: Session) -> None:
        """None result for a missing key must be cached — second call must not hit the DB."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)

        # First get — None should be cached
        result1 = repo.get("eco", "t1", "nonexistent")
        assert result1 is None

        with patch.object(session, "get", wraps=session.get) as mock_get:
            result2 = repo.get("eco", "t1", "nonexistent")
            # Without cache: always hits DB (count == 1). RED state.
            assert mock_get.call_count == 0

        assert result2 is None

    def test_identity_upsert_invalidates_cache(self, session: Session) -> None:
        """upsert() must invalidate the cache so the next get() returns fresh DB data."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(_make_identity(display_name="Original"))
        session.commit()

        # Populate cache
        repo.get("eco", "t1", "sa-001")
        session.expire_all()

        # Verify it IS cached (pre-condition — cache hit)
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 0  # RED without cache

        # upsert must invalidate the cache entry
        repo.upsert(_make_identity(display_name="Updated"))
        session.commit()
        session.expire_all()

        # After invalidation: must re-query DB
        with patch.object(session, "get", wraps=session.get) as mock_get:
            result = repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # cache was invalidated — must hit DB

        assert result is not None
        assert result.display_name == "Updated"

    def test_identity_mark_deleted_invalidates_cache(self, session: Session) -> None:
        """mark_deleted() must invalidate the cache so the next get() returns fresh DB data."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(_make_identity())
        session.commit()

        # Populate cache
        repo.get("eco", "t1", "sa-001")
        session.expire_all()

        # Verify it IS cached (pre-condition)
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 0  # RED without cache

        # mark_deleted must invalidate the cache entry
        repo.mark_deleted("eco", "t1", "sa-001", datetime(2026, 2, 1, tzinfo=UTC))
        session.commit()
        session.expire_all()

        # After invalidation: must re-query DB
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # cache was invalidated — must hit DB

    def test_identity_cache_ttl_expires(self, session: Session) -> None:
        """After TTL expiry, get() must re-query the DB."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=0.05)
        repo.upsert(_make_identity())
        session.commit()

        # Populate cache
        repo.get("eco", "t1", "sa-001")
        session.expire_all()

        # Within TTL: cache hit
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 0  # RED without cache

        # Wait for TTL to expire
        time.sleep(0.15)

        # After expiry: must re-query DB
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # TTL expired — must hit DB

    def test_identity_cache_lru_eviction(self, session: Session) -> None:
        """When cache is full, LRU entry is evicted and re-queried on next get()."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=2, cache_ttl_seconds=300.0)

        for i in range(1, 4):
            repo.upsert(_make_identity(identity_id=f"sa-{i:03d}"))
        session.commit()

        # Fill cache: sa-001 first (LRU), then sa-002 (MRU)
        repo.get("eco", "t1", "sa-001")
        repo.get("eco", "t1", "sa-002")
        session.expire_all()

        # Both should be in cache (maxsize=2, both fit)
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            repo.get("eco", "t1", "sa-002")
            assert mock_get.call_count == 0  # RED without cache

        # Access sa-003 — cache is full, evicts LRU (sa-001, accessed least recently)
        repo.get("eco", "t1", "sa-003")
        session.expire_all()

        # sa-001 must have been evicted — must re-query DB
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # evicted — DB hit required

    def test_two_identity_repos_have_independent_caches(self, session: Session) -> None:
        """Two repository instances must NOT share cache state."""
        repo1 = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo2 = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)

        repo1.upsert(_make_identity())
        session.commit()

        # repo1 populates its own cache
        repo1.get("eco", "t1", "sa-001")
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            # repo2 has no cache entry — must query DB
            repo2.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1

            # repo1 cache still intact — must NOT query DB
            repo1.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # still 1 — repo1 used its own cache


# ---------------------------------------------------------------------------
# Resource repository cache tests
# ---------------------------------------------------------------------------


class TestResourceRepositoryCache:
    def test_resource_get_caches_db_call(self, session: Session) -> None:
        """Second get() for same resource key must NOT hit the DB (cache hit)."""
        repo = SQLModelResourceRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(_make_resource())
        session.commit()

        # Populate cache
        result1 = repo.get("eco", "t1", "lkc-001")
        assert result1 is not None

        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            result2 = repo.get("eco", "t1", "lkc-001")
            assert mock_get.call_count == 0  # RED without cache

        assert result2 is not None
        assert result2.resource_id == "lkc-001"

    def test_resource_get_nonexistent_caches_none(self, session: Session) -> None:
        """None result for a missing resource key must be cached."""
        repo = SQLModelResourceRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)

        repo.get("eco", "t1", "nonexistent")

        with patch.object(session, "get", wraps=session.get) as mock_get:
            result = repo.get("eco", "t1", "nonexistent")
            assert result is None
            assert mock_get.call_count == 0  # RED without cache

    def test_resource_upsert_invalidates_cache(self, session: Session) -> None:
        """After upsert(), get() must return fresh DB data (cache invalidated)."""
        repo = SQLModelResourceRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(_make_resource(display_name="Original"))
        session.commit()

        repo.get("eco", "t1", "lkc-001")
        session.expire_all()

        # Confirm cache is active
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "lkc-001")
            assert mock_get.call_count == 0  # RED without cache

        repo.upsert(_make_resource(display_name="Updated"))
        session.commit()
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            result = repo.get("eco", "t1", "lkc-001")
            assert mock_get.call_count == 1  # invalidated — must hit DB

        assert result is not None
        assert result.display_name == "Updated"

    def test_resource_mark_deleted_invalidates_cache(self, session: Session) -> None:
        """After mark_deleted(), get() must re-query the DB (cache invalidated)."""
        repo = SQLModelResourceRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(_make_resource())
        session.commit()

        repo.get("eco", "t1", "lkc-001")
        session.expire_all()

        # Confirm cache is active
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "lkc-001")
            assert mock_get.call_count == 0  # RED without cache

        repo.mark_deleted("eco", "t1", "lkc-001", datetime(2026, 2, 1, tzinfo=UTC))
        session.commit()
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "lkc-001")
            assert mock_get.call_count == 1  # invalidated — must hit DB

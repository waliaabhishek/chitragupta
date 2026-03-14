from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import core.api.routes.readiness as readiness_module
from core.api.app import create_app
from core.api.schemas import ReadinessResponse
from core.config.models import AppSettings, StorageConfig, TenantConfig


def _make_settings() -> AppSettings:
    return AppSettings(
        tenants={
            "acme": TenantConfig(
                tenant_id="t-001",
                ecosystem="ccloud",
                storage=StorageConfig(connection_string="sqlite:///:memory:"),
            )
        }
    )


def _make_backend() -> MagicMock:
    mock_uow = MagicMock()
    mock_uow.pipeline_runs.get_latest_run.return_value = None
    mock_uow.pipeline_state.count_calculated.return_value = 1
    mock_uow.__enter__ = MagicMock(return_value=mock_uow)
    mock_uow.__exit__ = MagicMock(return_value=False)
    mock_backend = MagicMock()
    mock_backend.create_read_only_unit_of_work.return_value = mock_uow
    return mock_backend


@pytest.fixture(autouse=True)
def reset_readiness_cache() -> Generator[None]:
    """Reset the module-level TTL cache before and after each test."""
    readiness_module._readiness_cache = None  # type: ignore[attr-defined]
    yield
    readiness_module._readiness_cache = None  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# TTL constant
# ---------------------------------------------------------------------------


class TestReadinessCacheTTLConstant:
    def test_ttl_constant_is_2_seconds(self) -> None:
        """_READINESS_CACHE_TTL must be exactly 2.0 seconds."""
        assert readiness_module._READINESS_CACHE_TTL == 2.0  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# First call populates cache
# ---------------------------------------------------------------------------


class TestReadinessCacheFirstCall:
    def test_first_call_populates_cache(self) -> None:
        """After the first request, _readiness_cache must be a (ReadinessResponse, float) tuple."""
        settings = _make_settings()
        app = create_app(settings, workflow_runner=None, mode="api")
        backend = _make_backend()

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.routes.readiness.get_or_create_backend", return_value=backend),TestClient(app) as client
        ):
            client.get("/api/v1/readiness")

        # Must be set — not None
        assert readiness_module._readiness_cache is not None  # type: ignore[attr-defined]
        cached_response, cached_timestamp = readiness_module._readiness_cache  # type: ignore[attr-defined]
        assert isinstance(cached_response, ReadinessResponse)
        assert isinstance(cached_timestamp, float)

    def test_first_call_queries_db(self) -> None:
        """First request must call get_or_create_backend exactly once (no prior cache)."""
        settings = _make_settings()
        app = create_app(settings, workflow_runner=None, mode="api")
        mock_get_backend = MagicMock(return_value=_make_backend())

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.routes.readiness.get_or_create_backend", mock_get_backend),TestClient(app) as client
        ):
            client.get("/api/v1/readiness")

        assert mock_get_backend.call_count == 1


# ---------------------------------------------------------------------------
# Cache hit within TTL
# ---------------------------------------------------------------------------


class TestReadinessCacheHit:
    def test_second_call_within_ttl_skips_db(self) -> None:
        """Two rapid consecutive requests must result in exactly 1 DB call (cache hit on second)."""
        settings = _make_settings()
        app = create_app(settings, workflow_runner=None, mode="api")
        mock_get_backend = MagicMock(return_value=_make_backend())

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.routes.readiness.get_or_create_backend", mock_get_backend),TestClient(app) as client
        ):
            # Both requests happen well within the 2s TTL window
            client.get("/api/v1/readiness")
            client.get("/api/v1/readiness")

        # Second request must use the cache — DB invoked only once
        assert mock_get_backend.call_count == 1

    def test_many_rapid_calls_result_in_single_db_query(self) -> None:
        """10 back-to-back requests within 2s must produce exactly 1 DB call."""
        settings = _make_settings()
        app = create_app(settings, workflow_runner=None, mode="api")
        mock_get_backend = MagicMock(return_value=_make_backend())

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.routes.readiness.get_or_create_backend", mock_get_backend),TestClient(app) as client
        ):
            for _ in range(10):
                response = client.get("/api/v1/readiness")
                assert response.status_code == 200

        assert mock_get_backend.call_count == 1


# ---------------------------------------------------------------------------
# Cache expiry
# ---------------------------------------------------------------------------


class TestReadinessCacheExpiry:
    def test_call_after_ttl_expires_recomputes(self) -> None:
        """A request arriving after the TTL window must bypass cache and query DB again.

        Approach: pre-seed _readiness_cache with a timestamp in the far past (t=0),
        then make a request at real wall time. Since now - 0 >> 2s, the cache is stale
        and must be recomputed.
        """
        settings = _make_settings()
        app = create_app(settings, workflow_runner=None, mode="api")
        mock_get_backend = MagicMock(return_value=_make_backend())

        # Seed a stale cache entry (timestamp 0.0 is always expired)
        stale_response = ReadinessResponse(status="ready", version="1.0.0", mode="api", tenants=[])
        readiness_module._readiness_cache = (stale_response, 0.0)  # type: ignore[attr-defined]

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.routes.readiness.get_or_create_backend", mock_get_backend),TestClient(app) as client
        ):
            client.get("/api/v1/readiness")

        # Stale cache must trigger a DB recompute
        assert mock_get_backend.call_count == 1
        # Cache must be updated with a fresh entry (not the seeded stale one)
        assert readiness_module._readiness_cache is not None  # type: ignore[attr-defined]
        fresh_response, fresh_ts = readiness_module._readiness_cache  # type: ignore[attr-defined]
        assert fresh_ts > 0.0  # must be a real current timestamp, not the seeded 0.0

from __future__ import annotations

import contextlib
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from core.api.dependencies import (
    TemporalParams,
    get_or_create_backend,
    get_settings,
    get_storage_backend,
    get_tenant_config,
    resolve_date_range,
    utc_today,
    validate_datetime_param,
    validate_temporal_params,
)
from core.config.models import AppSettings, StorageConfig, TenantConfig


def _make_request(settings: AppSettings) -> MagicMock:
    request = MagicMock()
    request.app.state.settings = settings
    request.app.state.backends = {}
    return request


class TestGetSettings:
    def test_returns_settings_from_state(self) -> None:
        settings = AppSettings()
        request = _make_request(settings)
        result = get_settings(request)
        assert result is settings


class TestGetTenantConfig:
    def test_success(self) -> None:
        tc = TenantConfig(ecosystem="eco", tenant_id="t1")
        settings = AppSettings(tenants={"my-tenant": tc})
        result = get_tenant_config("my-tenant", settings)
        assert result is tc

    def test_not_found_raises_404(self) -> None:
        settings = AppSettings(tenants={})
        with pytest.raises(HTTPException) as exc_info:
            get_tenant_config("missing", settings)
        assert exc_info.value.status_code == 404
        assert "missing" in str(exc_info.value.detail)


class TestValidateDatetimeParam:
    def test_none_returns_none(self) -> None:
        assert validate_datetime_param(None, "test") is None

    def test_aware_datetime_returns_utc(self) -> None:
        dt = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        result = validate_datetime_param(dt, "test")
        assert result is not None
        assert result.tzinfo is not None

    def test_naive_datetime_raises_400(self) -> None:
        dt = datetime(2026, 1, 15, 12, 0, 0)
        with pytest.raises(HTTPException) as exc_info:
            validate_datetime_param(dt, "active_at")
        assert exc_info.value.status_code == 400
        assert "active_at" in str(exc_info.value.detail)


class TestValidateTemporalParams:
    def test_validate_temporal_params_all_none(self) -> None:
        result = validate_temporal_params(None, None, None)
        assert isinstance(result, TemporalParams)
        assert result.active_at is None
        assert result.period_start is None
        assert result.period_end is None

    def test_validate_temporal_params_active_at_only(self) -> None:
        active_at = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        result = validate_temporal_params(active_at, None, None)
        assert isinstance(result, TemporalParams)
        assert result.active_at is not None
        assert result.active_at.tzinfo is not None
        assert result.period_start is None
        assert result.period_end is None

    def test_validate_temporal_params_period_only(self) -> None:
        period_start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        period_end = datetime(2026, 1, 31, 23, 59, 59, tzinfo=UTC)
        result = validate_temporal_params(None, period_start, period_end)
        assert isinstance(result, TemporalParams)
        assert result.active_at is None
        assert result.period_start is not None
        assert result.period_end is not None

    def test_validate_temporal_params_active_at_with_period_start_raises(self) -> None:
        active_at = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        period_start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        with pytest.raises(HTTPException) as exc_info:
            validate_temporal_params(active_at, period_start, None)
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "Cannot combine active_at with period_start/period_end"

    def test_validate_temporal_params_active_at_with_period_end_raises(self) -> None:
        active_at = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        period_end = datetime(2026, 1, 31, 23, 59, 59, tzinfo=UTC)
        with pytest.raises(HTTPException) as exc_info:
            validate_temporal_params(active_at, None, period_end)
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "Cannot combine active_at with period_start/period_end"

    def test_validate_temporal_params_period_start_gt_end_raises(self) -> None:
        period_start = datetime(2026, 1, 31, 0, 0, 0, tzinfo=UTC)
        period_end = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        with pytest.raises(HTTPException) as exc_info:
            validate_temporal_params(None, period_start, period_end)
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "period_start must be <= period_end"

    def test_validate_temporal_params_naive_datetime_raises(self) -> None:
        naive_dt = datetime(2026, 1, 15, 12, 0, 0)
        with pytest.raises(HTTPException) as exc_info:
            validate_temporal_params(naive_dt, None, None)
        assert exc_info.value.status_code == 400
        assert "must include timezone" in exc_info.value.detail

    def test_validate_temporal_params_converts_to_utc(self) -> None:
        # IST = UTC+5:30; 17:30 IST == 12:00 UTC
        from datetime import timedelta, timezone

        ist_tz = timezone(timedelta(hours=5, minutes=30))
        dt_ist = datetime(2026, 1, 15, 17, 30, 0, tzinfo=ist_tz)
        result = validate_temporal_params(dt_ist, None, None)
        assert result.active_at is not None
        assert result.active_at.tzinfo == UTC
        assert result.active_at == datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)


class TestUtcToday:
    def test_returns_utc_date_not_local(self) -> None:
        """utc_today() should use datetime.now(UTC).date(), not date.today()."""
        # Simulate a server at UTC-5 where local time is 23:00 on March 3
        # but UTC is already March 4
        fake_utc_now = datetime(2026, 3, 4, 4, 0, 0, tzinfo=UTC)
        with patch("core.api.dependencies.datetime") as mock_dt:
            mock_dt.now.return_value = fake_utc_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = utc_today()
        assert result == date(2026, 3, 4)
        mock_dt.now.assert_called_once_with(UTC)

    def test_returns_date_type(self) -> None:
        result = utc_today()
        assert isinstance(result, date)


class TestUoWDependencies:
    def test_get_unit_of_work_yields_read_only_uow(self, tmp_path: object) -> None:
        """get_unit_of_work must yield a ReadOnlySQLModelUnitOfWork instance."""
        from core.api.dependencies import get_unit_of_work
        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from core.storage.backends.sqlmodel.unit_of_work import (
            ReadOnlySQLModelUnitOfWork,  # ImportError = red
            SQLModelBackend,
        )

        db_path = tmp_path / "test.db"  # type: ignore[operator]
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        gen = get_unit_of_work(backend)
        uow = next(gen)
        assert isinstance(uow, ReadOnlySQLModelUnitOfWork)
        # Context manager must be entered — session is active
        assert uow._session is not None  # type: ignore[attr-defined]

        with contextlib.suppress(StopIteration):
            next(gen)

        backend.dispose()

    def test_get_write_unit_of_work_yields_plain_uow(self, tmp_path: object) -> None:
        """get_write_unit_of_work must yield a plain SQLModelUnitOfWork (not the read-only subclass)."""
        from core.api.dependencies import get_write_unit_of_work  # ImportError = red
        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend, SQLModelUnitOfWork

        db_path = tmp_path / "test.db"  # type: ignore[operator]
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        gen = get_write_unit_of_work(backend)
        uow = next(gen)
        assert type(uow) is SQLModelUnitOfWork

        with contextlib.suppress(StopIteration):
            next(gen)

        backend.dispose()

    def test_get_unit_of_work_session_closed_after_exit(self, tmp_path: object) -> None:
        """get_unit_of_work must close the session when the generator exits."""
        from core.api.dependencies import get_unit_of_work
        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from core.storage.backends.sqlmodel.unit_of_work import (
            ReadOnlySQLModelUnitOfWork,  # ImportError = red
            SQLModelBackend,
        )

        db_path = tmp_path / "test.db"  # type: ignore[operator]
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        gen = get_unit_of_work(backend)
        uow = next(gen)
        assert isinstance(uow, ReadOnlySQLModelUnitOfWork)
        assert uow._session is not None  # type: ignore[attr-defined]

        with contextlib.suppress(StopIteration):
            next(gen)

        # __exit__ must have been called — session is None after cleanup
        assert uow._session is None  # type: ignore[attr-defined]
        backend.dispose()

    def test_get_write_unit_of_work_session_closed_after_exit(self, tmp_path: object) -> None:
        """get_write_unit_of_work must close the session when the generator exits."""
        from core.api.dependencies import get_write_unit_of_work  # ImportError = red
        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend, SQLModelUnitOfWork

        db_path = tmp_path / "test.db"  # type: ignore[operator]
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        gen = get_write_unit_of_work(backend)
        uow = next(gen)
        assert type(uow) is SQLModelUnitOfWork
        assert uow._session is not None  # type: ignore[attr-defined]

        with contextlib.suppress(StopIteration):
            next(gen)

        assert uow._session is None  # type: ignore[attr-defined]
        backend.dispose()


class TestGetOrCreateBackend:
    def test_first_call_creates_and_caches_backend(self) -> None:
        """get_or_create_backend creates a backend on first call and stores it in the dict."""
        mock_backend = MagicMock()
        storage_config = StorageConfig(connection_string="sqlite:///:memory:")
        backends: dict[str, object] = {}

        with (
            patch("core.api.dependencies.get_storage_module_for_ecosystem", return_value=MagicMock()),
            patch("core.api.dependencies.create_storage_backend", return_value=mock_backend),
        ):
            result = get_or_create_backend(backends, "acme", storage_config, "ccloud")

        assert result is mock_backend
        assert "acme" in backends
        assert backends["acme"] is mock_backend
        mock_backend.create_tables.assert_called_once()

    def test_second_call_returns_cached_backend(self) -> None:
        """get_or_create_backend returns the same object on repeated calls without re-creating."""
        mock_backend = MagicMock()
        storage_config = StorageConfig(connection_string="sqlite:///:memory:")
        backends: dict[str, object] = {}

        with (
            patch("core.api.dependencies.get_storage_module_for_ecosystem", return_value=MagicMock()),
            patch("core.api.dependencies.create_storage_backend", return_value=mock_backend) as mock_create,
        ):
            first = get_or_create_backend(backends, "acme", storage_config, "ccloud")
            second = get_or_create_backend(backends, "acme", storage_config, "ccloud")

        assert first is second
        # create_storage_backend called only once — second call hit the cache
        mock_create.assert_called_once()


class TestGetStorageBackend:
    def test_lazy_init_backends_dict_on_app_state(self) -> None:
        """get_storage_backend initialises app.state.backends if absent."""
        mock_backend = MagicMock()
        tc = TenantConfig(
            ecosystem="ccloud", tenant_id="t1", storage=StorageConfig(connection_string="sqlite:///:memory:")
        )
        request = MagicMock()
        del request.app.state.backends  # ensure attribute is absent

        with (
            patch("core.api.dependencies.get_storage_module_for_ecosystem", return_value=MagicMock()),
            patch("core.api.dependencies.create_storage_backend", return_value=mock_backend),
        ):
            result = get_storage_backend(request, "acme", tc)

        assert result is mock_backend
        assert hasattr(request.app.state, "backends")

    def test_delegates_to_get_or_create_backend(self) -> None:
        """get_storage_backend delegates to get_or_create_backend with the right args."""
        mock_backend = MagicMock()
        tc = TenantConfig(
            ecosystem="ccloud", tenant_id="t1", storage=StorageConfig(connection_string="sqlite:///:memory:")
        )
        request = MagicMock()
        request.app.state.backends = {}

        with patch("core.api.dependencies.get_or_create_backend", return_value=mock_backend) as mock_goc:
            result = get_storage_backend(request, "acme", tc)

        assert result is mock_backend
        mock_goc.assert_called_once_with(request.app.state.backends, "acme", tc.storage, tc.ecosystem)


class TestResolveDateRange:
    _FAKE_TODAY = date(2026, 3, 5)

    def test_defaults_return_30_day_window(self) -> None:
        with patch("core.api.dependencies.utc_today", return_value=self._FAKE_TODAY):
            start_dt, end_dt = resolve_date_range(None, None)
        assert start_dt == datetime(2026, 2, 3, tzinfo=UTC)
        assert end_dt == datetime(2026, 3, 6, tzinfo=UTC)

    def test_explicit_dates_convert_to_utc_midnight(self) -> None:
        start_dt, end_dt = resolve_date_range(date(2026, 1, 1), date(2026, 1, 31))
        assert start_dt == datetime(2026, 1, 1, tzinfo=UTC)
        assert end_dt == datetime(2026, 2, 1, tzinfo=UTC)

    def test_start_after_end_raises_400(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            resolve_date_range(date(2026, 2, 1), date(2026, 1, 1))
        assert exc_info.value.status_code == 400

    def test_partial_default_end_uses_tomorrow(self) -> None:
        with patch("core.api.dependencies.utc_today", return_value=self._FAKE_TODAY):
            start_dt, end_dt = resolve_date_range(date(2026, 1, 15), None)
        assert start_dt == datetime(2026, 1, 15, tzinfo=UTC)
        assert end_dt == datetime(2026, 3, 6, tzinfo=UTC)

    def test_same_day_start_end_is_valid(self) -> None:
        start_dt, end_dt = resolve_date_range(date(2026, 1, 1), date(2026, 1, 1))
        assert start_dt == datetime(2026, 1, 1, tzinfo=UTC)
        assert end_dt == datetime(2026, 1, 2, tzinfo=UTC)

    def test_utc_default_unchanged(self) -> None:
        start_dt, end_dt = resolve_date_range(date(2026, 1, 1), date(2026, 1, 31), timezone="UTC")
        assert start_dt == datetime(2026, 1, 1, tzinfo=UTC)
        assert end_dt == datetime(2026, 2, 1, tzinfo=UTC)

    def test_denver_end_date_boundary(self) -> None:
        _start_dt, end_dt = resolve_date_range(date(2025, 12, 1), date(2025, 12, 31), timezone="America/Denver")
        assert end_dt == datetime(2026, 1, 1, 7, 0, 0, tzinfo=UTC)

    def test_denver_start_date_boundary(self) -> None:
        start_dt, _end_dt = resolve_date_range(date(2025, 12, 1), date(2025, 12, 31), timezone="America/Denver")
        assert start_dt == datetime(2025, 12, 1, 7, 0, 0, tzinfo=UTC)

    def test_invalid_timezone_raises_400(self) -> None:
        with (
            patch("core.api.dependencies.utc_today", return_value=self._FAKE_TODAY),
            pytest.raises(HTTPException) as exc_info,
        ):
            resolve_date_range(None, None, timezone="Not/ATimezone")
        assert exc_info.value.status_code == 400
        assert "Not/ATimezone" in exc_info.value.detail

    def test_empty_string_timezone_treated_as_utc(self) -> None:
        _start_dt, end_dt = resolve_date_range(date(2026, 1, 1), date(2026, 1, 1), timezone="")
        assert end_dt == datetime(2026, 1, 2, tzinfo=UTC)

    def test_dst_spring_forward_denver(self) -> None:
        start_dt, end_dt = resolve_date_range(date(2026, 3, 8), date(2026, 3, 8), timezone="America/Denver")
        assert start_dt == datetime(2026, 3, 8, 7, 0, 0, tzinfo=UTC)
        assert end_dt == datetime(2026, 3, 9, 6, 0, 0, tzinfo=UTC)

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from core.api.dependencies import (
    get_settings,
    get_tenant_config,
    validate_datetime_param,
)
from core.config.models import AppSettings, TenantConfig


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

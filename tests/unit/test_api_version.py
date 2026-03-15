from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient


class TestGetVersion:
    """TASK-106: get_version() unit tests."""

    def test_get_version_returns_string(self) -> None:
        from core.api import get_version

        result = get_version()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_get_version_package_not_found_returns_dev(self) -> None:
        from importlib.metadata import PackageNotFoundError

        from core.api import get_version

        with patch("core.api._pkg_version", side_effect=PackageNotFoundError("chitragupt")):
            result = get_version()
        assert result == "0.0.0-dev"


class TestHealthVersionIntegration:
    """TASK-106: GET /health version field matches get_version() output."""

    def test_health_returns_get_version_output(self) -> None:
        from core.api import get_version
        from core.api.app import create_app
        from core.config.models import ApiConfig, AppSettings, LoggingConfig

        settings = AppSettings(
            api=ApiConfig(host="127.0.0.1", port=8080),
            logging=LoggingConfig(),
            tenants={},
        )
        app = create_app(settings)
        client = TestClient(app)

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["version"] == get_version()

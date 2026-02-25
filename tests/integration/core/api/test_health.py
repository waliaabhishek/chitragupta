from __future__ import annotations

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, LoggingConfig


def _make_settings() -> AppSettings:
    return AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        logging=LoggingConfig(),
        tenants={},
    )


class TestHealthEndpoint:
    def test_health_endpoint_returns_ok(self) -> None:
        settings = _make_settings()
        app = create_app(settings)
        client = TestClient(app)

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["version"] == "1.0.0"

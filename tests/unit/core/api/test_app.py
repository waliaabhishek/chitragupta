from __future__ import annotations

from fastapi import FastAPI

from core.api import get_version
from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig


def _make_settings(enable_cors: bool = False, cors_origins: list[str] | None = None) -> AppSettings:
    return AppSettings(
        api=ApiConfig(
            host="127.0.0.1",
            port=8080,
            enable_cors=enable_cors,
            cors_origins=cors_origins or [],
        ),
        logging=LoggingConfig(),
        tenants={
            "test": TenantConfig(
                tenant_id="t-123",
                ecosystem="test-eco",
                storage=StorageConfig(connection_string="sqlite:///:memory:"),
            )
        },
    )


class TestCreateApp:
    def test_create_app_returns_fastapi_instance(self) -> None:
        settings = _make_settings()
        app = create_app(settings)
        assert isinstance(app, FastAPI)

    def test_app_has_title_and_version(self) -> None:
        settings = _make_settings()
        app = create_app(settings)
        assert app.title == "Chitragupta API"
        assert app.version == get_version()

    def test_app_has_all_routers_registered(self) -> None:
        settings = _make_settings()
        app = create_app(settings)

        routes = [r.path for r in app.routes]
        assert "/health" in routes
        assert "/api/v1/tenants" in routes
        assert "/api/v1/tenants/{tenant_name}/status" in routes
        assert "/api/v1/tenants/{tenant_name}/billing" in routes
        assert "/api/v1/tenants/{tenant_name}/chargebacks" in routes
        assert "/api/v1/tenants/{tenant_name}/resources" in routes
        assert "/api/v1/tenants/{tenant_name}/identities" in routes
        # Chunk 3.2 routes
        assert "/api/v1/tenants/{tenant_name}/chargebacks/{dimension_id}" in routes
        assert "/api/v1/tenants/{tenant_name}/entities/{entity_type}/{entity_id}/tags" in routes
        assert "/api/v1/tenants/{tenant_name}/tags" in routes
        assert "/api/v1/tenants/{tenant_name}/pipeline/run" in routes
        assert "/api/v1/tenants/{tenant_name}/pipeline/status" in routes
        assert "/api/v1/tenants/{tenant_name}/chargebacks/aggregate" in routes
        assert "/api/v1/tenants/{tenant_name}/export" in routes

    def test_cors_disabled_by_default(self) -> None:
        settings = _make_settings(enable_cors=False)
        app = create_app(settings)
        # Check no CORS middleware
        middleware_classes = [m.cls.__name__ for m in app.user_middleware]
        assert "CORSMiddleware" not in middleware_classes

    def test_cors_enabled_adds_middleware(self) -> None:
        settings = _make_settings(enable_cors=True, cors_origins=["http://localhost:3000"])
        app = create_app(settings)
        middleware_classes = [m.cls.__name__ for m in app.user_middleware]
        assert "CORSMiddleware" in middleware_classes

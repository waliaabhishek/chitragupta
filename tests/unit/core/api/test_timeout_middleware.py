from __future__ import annotations

import asyncio

from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.config.models import ApiConfig, AppSettings

# ---------------------------------------------------------------------------
# Middleware unit tests
# ---------------------------------------------------------------------------


class TestRequestTimeoutMiddlewareUnit:
    def test_request_completing_within_timeout_returns_normally(self) -> None:
        """A fast async route must pass through the middleware unaffected."""
        from core.api.app import RequestTimeoutMiddleware  # ImportError → red until implemented

        app = FastAPI()

        @app.get("/fast")
        async def fast_endpoint() -> dict[str, bool]:
            return {"ok": True}

        app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=5)

        with TestClient(app) as client:
            response = client.get("/fast")

        assert response.status_code == 200
        assert response.json() == {"ok": True}

    def test_request_exceeding_timeout_returns_504(self) -> None:
        """An async route that exceeds timeout_seconds must receive a 504 response."""
        from core.api.app import RequestTimeoutMiddleware  # ImportError → red until implemented

        app = FastAPI()

        @app.get("/slow")
        async def slow_endpoint() -> dict[str, bool]:
            await asyncio.sleep(10)
            return {"ok": True}  # pragma: no cover

        app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=1)

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/slow")

        assert response.status_code == 504
        body = response.json()
        assert "detail" in body
        assert "1" in body["detail"]  # timeout_seconds value appears in message

    def test_504_detail_mentions_timeout(self) -> None:
        """The 504 response detail must describe the timeout (not a generic error)."""
        from core.api.app import RequestTimeoutMiddleware  # ImportError → red until implemented

        app = FastAPI()

        @app.get("/stuck")
        async def stuck_endpoint() -> dict[str, bool]:
            await asyncio.sleep(10)
            return {"ok": True}  # pragma: no cover

        app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=1)

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/stuck")

        assert response.status_code == 504
        assert "timeout" in response.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Wiring into create_app
# ---------------------------------------------------------------------------


class TestRequestTimeoutMiddlewareWiring:
    def test_timeout_middleware_present_in_create_app(self) -> None:
        """create_app must register RequestTimeoutMiddleware."""
        from core.api.app import create_app

        settings = AppSettings(
            api=ApiConfig(request_timeout_seconds=30),
            tenants={},
        )
        app = create_app(settings)

        middleware_classes = [m.cls.__name__ for m in app.user_middleware]
        assert "RequestTimeoutMiddleware" in middleware_classes

    def test_timeout_middleware_uses_settings_timeout_seconds(self) -> None:
        """Middleware timeout_seconds must match settings.api.request_timeout_seconds."""
        from core.api.app import create_app

        settings = AppSettings(
            api=ApiConfig(request_timeout_seconds=45),
            tenants={},
        )
        app = create_app(settings)

        timeout_mw = next(
            (m for m in app.user_middleware if m.cls.__name__ == "RequestTimeoutMiddleware"),
            None,
        )
        assert timeout_mw is not None
        assert timeout_mw.kwargs.get("timeout_seconds") == 45

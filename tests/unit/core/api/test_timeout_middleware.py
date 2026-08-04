from __future__ import annotations

import asyncio
from uuid import UUID

from fastapi import FastAPI, Request
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

    def test_create_app_sets_request_id_on_request_state(self) -> None:
        """Production create_app must install request correlation before handlers run."""
        from core.api.app import create_app

        settings = AppSettings(
            api=ApiConfig(request_timeout_seconds=30),
            tenants={},
        )
        app = create_app(settings)

        @app.get("/request-id")
        async def request_id_endpoint(request: Request) -> dict[str, str | None]:
            return {"request_id": getattr(request.state, "request_id", None)}

        with TestClient(app) as client:
            response = client.get("/request-id")

        assert response.status_code == 200
        request_id = response.json()["request_id"]
        assert request_id is not None
        assert UUID(hex=request_id).hex == request_id

    def test_request_context_skips_debug_formatting_when_debug_is_disabled(self) -> None:
        """INFO-level requests must not eagerly build discarded DEBUG context."""
        from unittest.mock import patch

        from core.api.app import create_app

        settings = AppSettings(
            api=ApiConfig(request_timeout_seconds=30),
            tenants={},
        )
        app = create_app(settings)

        @app.get("/debug-context")
        async def debug_context_endpoint() -> dict[str, bool]:
            return {"ok": True}

        with (
            patch("core.api.app.logger.isEnabledFor", return_value=False),
            patch("core.api.app.safe_log_context") as render_context,
            TestClient(app) as client,
        ):
            response = client.get("/debug-context")

        assert response.status_code == 200
        render_context.assert_not_called()

    def test_timeout_log_includes_request_id_and_response_body_stays_unchanged(self) -> None:
        """Timeout conversion must log once with request_id and keep the existing 504 body."""
        from unittest.mock import patch

        from core.api.app import create_app

        settings = AppSettings(
            api=ApiConfig(request_timeout_seconds=1),
            tenants={},
        )
        app = create_app(settings)

        @app.get("/slow")
        async def slow_endpoint(request: Request) -> dict[str, str]:
            del request
            await asyncio.sleep(10)
            return {"ok": "nope"}  # pragma: no cover

        with (
            patch("core.api.app.logger.warning") as warning_log,
            TestClient(
                app,
                raise_server_exceptions=False,
            ) as client,
        ):
            response = client.get("/slow")

        assert response.status_code == 504
        assert response.json() == {"detail": "Request exceeded 1s timeout"}
        warning_log.assert_called_once()
        assert "request_id" in str(warning_log.call_args)

from __future__ import annotations

import uuid
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig


def _make_settings() -> AppSettings:
    return AppSettings(
        api=ApiConfig(
            host="127.0.0.1",
            port=8080,
            enable_cors=False,
            cors_origins=[],
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


def _make_test_client() -> TestClient:
    """Create a TestClient with routes that trigger exceptions."""
    settings = _make_settings()
    app = create_app(settings)

    @app.get("/test/runtime-error")
    def raise_runtime_error() -> None:
        raise RuntimeError("boom")

    @app.get("/test/http-exception")
    def raise_http_exception() -> None:
        raise HTTPException(status_code=404, detail="not found")

    return TestClient(app, raise_server_exceptions=False)


class TestGlobalExceptionHandler:
    def test_unhandled_exception_returns_500_structured_json(self) -> None:
        client = _make_test_client()
        response = client.get("/test/runtime-error")
        assert response.status_code == 500
        body = response.json()
        assert body["detail"] == "Internal server error"
        assert "error_id" in body
        assert "Traceback" not in response.text
        assert "RuntimeError" not in response.text

    def test_traceback_in_logs_not_in_response(self) -> None:
        client = _make_test_client()
        with patch("core.api.exception_handler.logger") as mock_logger:
            response = client.get("/test/runtime-error")
        assert response.status_code == 500
        body = response.json()
        mock_logger.exception.assert_called_once()
        call_kwargs = mock_logger.exception.call_args
        assert body["error_id"] in str(call_kwargs)
        assert "Traceback" not in response.text

    def test_error_id_is_valid_uuid(self) -> None:
        client = _make_test_client()
        response = client.get("/test/runtime-error")
        assert response.status_code == 500
        error_id = response.json()["error_id"]
        parsed = uuid.UUID(error_id)  # raises ValueError if not a valid UUID
        assert str(parsed) == error_id

    def test_http_exception_passes_through_unaffected(self) -> None:
        client = _make_test_client()
        response = client.get("/test/http-exception")
        assert response.status_code == 404
        assert response.json() == {"detail": "not found"}

    def test_sequential_requests_produce_unique_error_ids(self) -> None:
        client = _make_test_client()
        r1 = client.get("/test/runtime-error")
        r2 = client.get("/test/runtime-error")
        assert r1.status_code == 500
        assert r2.status_code == 500
        id1 = r1.json()["error_id"]
        id2 = r2.json()["error_id"]
        assert id1 != id2

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

import anyio.to_thread
import pytest

from tests.integration.core.api.test_focus_preview import _client, _settings


@pytest.fixture(autouse=True)
def _inline_mocked_startup_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run_inline(function: Callable[..., object], *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Callable[..., object], *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", run_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)


@pytest.mark.parametrize(
    "month",
    ["0000-01", "2026-1", "2026-13", "9999-12", "２０２６-０７"],
)
def test_monthly_semantic_errors_are_exact_400_before_runtime_or_backend(tmp_path: Path, month: str) -> None:
    route = importlib.import_module("core.api.routes.focus_preview")
    app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "_runtime") as runtime,
        patch.object(route, "_backend") as backend,
        client,
    ):
        response = client.post(
            "/api/v1/tenants/production/focus-preview/requests",
            json={"grain": "monthly", "month": month, "column_profile": "full"},
        )

    assert response.status_code == 400
    assert response.json() == {"detail": "month must use YYYY-MM"}
    runtime.assert_not_called()
    backend.assert_not_called()


@pytest.mark.parametrize(
    "body",
    [
        {
            "grain": "monthly",
            "month": "2026-07",
            "start_date": "2026-07-01",
            "end_date": "2026-08-01",
            "column_profile": "full",
        },
        {
            "grain": "daily",
            "month": "2026-07",
            "start_date": "2026-07-01",
            "end_date": "2026-07-02",
            "column_profile": "full",
        },
        {"grain": "monthly", "column_profile": "full"},
    ],
)
def test_discriminated_request_bodies_forbid_cross_grain_or_missing_fields(
    tmp_path: Path,
    body: dict[str, str],
) -> None:
    _app, client = _client(_settings(tmp_path))
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        response = client.post("/api/v1/tenants/production/focus-preview/requests", json=body)

    assert response.status_code == 422
    assert isinstance(response.json()["detail"], list)


@pytest.mark.parametrize("profile", ["full", "summary"])
def test_explicit_columns_for_noncustom_profile_are_exact_400_before_runtime(
    tmp_path: Path,
    profile: str,
) -> None:
    route = importlib.import_module("core.api.routes.focus_preview")
    _app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "_runtime") as runtime,
        client,
    ):
        response = client.post(
            "/api/v1/tenants/production/focus-preview/requests",
            json={
                "grain": "daily",
                "start_date": "2026-07-01",
                "end_date": "2026-07-02",
                "column_profile": profile,
                "columns": ["BilledCost"],
            },
        )

    assert response.status_code == 400
    assert response.json() == {"detail": "columns may be supplied only when column_profile is custom"}
    runtime.assert_not_called()


def test_all_invalid_custom_logs_every_unknown_before_exact_400(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    route = importlib.import_module("core.api.routes.focus_preview")
    _app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "_runtime") as runtime,
        caplog.at_level(logging.WARNING, logger=route.__name__),
        client,
    ):
        response = client.post(
            "/api/v1/tenants/production/focus-preview/requests",
            json={
                "grain": "daily",
                "start_date": "2026-07-01",
                "end_date": "2026-07-02",
                "column_profile": "custom",
                "columns": ["Unknown", "Unknown"],
            },
        )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Custom column selection must contain at least one supported Full-profile column"
    }
    assert (
        caplog.messages.count(
            "FOCUS Mapping Preview ignored unsupported Custom column tenant=production column='Unknown'"
        )
        == 2
    )
    runtime.assert_not_called()


def test_successful_custom_normalization_logs_unknown_and_duplicate_before_runtime_503(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    route = importlib.import_module("core.api.routes.focus_preview")
    app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        caplog.at_level(logging.WARNING, logger=route.__name__),
        client,
    ):
        app.state.preview_runtime = None
        response = client.post(
            "/api/v1/tenants/production/focus-preview/requests",
            json={
                "grain": "daily",
                "start_date": "2026-07-01",
                "end_date": "2026-07-02",
                "column_profile": "custom",
                "columns": ["Unknown", "BilledCost", "BilledCost", "Tags"],
            },
        )

    assert response.status_code == 503
    assert caplog.messages == [
        "FOCUS Mapping Preview ignored unsupported Custom column tenant=production column='Unknown'",
        "FOCUS Mapping Preview ignored duplicate Custom column tenant=production column='BilledCost'",
    ]


def test_unexpected_normalization_exception_is_not_converted_to_semantic_400(tmp_path: Path) -> None:
    route = importlib.import_module("core.api.routes.focus_preview")
    _app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "normalize_column_selection", side_effect=RuntimeError("unexpected sentinel")),
        client,
        pytest.raises(RuntimeError, match="unexpected sentinel"),
    ):
        client.post(
            "/api/v1/tenants/production/focus-preview/requests",
            json={
                "grain": "daily",
                "start_date": "2026-07-01",
                "end_date": "2026-07-02",
                "column_profile": "full",
            },
        )


def test_tenant_and_ecosystem_precede_semantic_month_validation(tmp_path: Path) -> None:
    invalid = {"grain": "monthly", "month": "9999-12", "column_profile": "full"}
    _app, client = _client(_settings(tmp_path, ecosystem="test-eco"))
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        unknown = client.post("/api/v1/tenants/unknown/focus-preview/requests", json=invalid)
        unsupported = client.post("/api/v1/tenants/production/focus-preview/requests", json=invalid)

    assert unknown.status_code == 404
    assert unsupported.status_code == 400
    assert unsupported.json() == {"detail": "FOCUS Mapping Preview currently supports only Confluent Cloud tenants"}


def test_profile_endpoint_is_static_code_owned_and_uses_no_runtime_backend_or_database(tmp_path: Path) -> None:
    route = importlib.import_module("core.api.routes.focus_preview")
    mapping = importlib.import_module("core.preview.mapping")
    _app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "_runtime", side_effect=AssertionError("runtime accessed")),
        patch.object(route, "_backend", side_effect=AssertionError("backend accessed")),
        client,
    ):
        response = client.get("/api/v1/tenants/production/focus-preview/profile")

    assert response.status_code == 200
    assert response.json() == {
        "mapping_profile_version": "focus-1.4-preview-v5",
        "full_columns": list(mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS),
        "summary_columns": list(mapping.FOCUS_1_4_SUMMARY_COLUMNS),
    }


def test_profile_endpoint_preserves_tenant_and_ecosystem_errors_without_route_shadowing(tmp_path: Path) -> None:
    _app, client = _client(_settings(tmp_path, ecosystem="test-eco"))
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        unknown = client.get("/api/v1/tenants/unknown/focus-preview/profile")
        unsupported = client.get("/api/v1/tenants/production/focus-preview/profile")

    assert unknown.status_code == 404
    assert unsupported.status_code == 400
    assert unsupported.json() == {"detail": "FOCUS Mapping Preview currently supports only Confluent Cloud tenants"}


def test_openapi_exposes_both_grains_all_profiles_month_and_effective_columns(tmp_path: Path) -> None:
    app, _client_value = _client(_settings(tmp_path))
    schema = app.openapi()
    post = schema["paths"]["/api/v1/tenants/{tenant_name}/focus-preview/requests"]["post"]
    request_schema = post["requestBody"]["content"]["application/json"]["schema"]
    response_schema = schema["components"]["schemas"]["FocusPreviewResponse"]

    assert request_schema["discriminator"]["propertyName"] == "grain"
    assert {"month", "effective_columns"} <= response_schema["properties"].keys()
    assert "/api/v1/tenants/{tenant_name}/focus-preview/profile" in schema["paths"]

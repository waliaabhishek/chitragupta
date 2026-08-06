from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from inspect import signature
from pathlib import Path
from typing import get_type_hints
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, Request
from pydantic import TypeAdapter, ValidationError

from core.api.app import create_app, recover_preview_owner
from core.api.routes import focus_preview
from core.api.schemas import FocusPreviewRequestBody
from core.config.models import ApiConfig, AppSettings, PreviewConfig, StorageConfig, TenantConfig
from core.storage.backend_provider import ApiTenantBackendProvider, TenantBackendProvider
from core.storage.interface import StorageBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

DISABLED_DETAIL = {
    "detail": {
        "code": "preview_commercial_profile_unavailable",
        "message": "An explicit Direct-billed PAYG profile does not cover the requested interval.",
        "retryable": False,
    }
}


class _NoBackendProvider:
    def __init__(self) -> None:
        self.acquire_count = 0
        self.close_count = 0

    @contextmanager
    def acquire_backend(self, tenant_name: str, tenant_config: TenantConfig) -> Iterator[StorageBackend]:
        del tenant_name, tenant_config
        self.acquire_count += 1
        raise AssertionError("disabled or rejected Preview request acquired a backend")
        yield MagicMock(spec=StorageBackend)

    def close(self) -> None:
        self.close_count += 1


def test_no_backend_provider_double_satisfies_production_protocol() -> None:
    assert isinstance(_NoBackendProvider(), TenantBackendProvider)


def _tenant(
    tmp_path: Path,
    *,
    tenant_id: str,
    ecosystem: str = "confluent_cloud",
    enabled: bool = False,
) -> TenantConfig:
    focus_preview = None
    if enabled:
        focus_preview = {
            "commercial_profile": "direct_payg",
            "billing_currency": "USD",
            "effective_start_date": "2020-01-01",
            "effective_end_date": "2030-01-01",
        }
    return TenantConfig(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / f'{tenant_id}.db'}"),
        focus_preview=focus_preview,
    )


def _settings(tmp_path: Path, *, enabled: bool = False, mixed: bool = False) -> AppSettings:
    tenants = {
        "disabled": _tenant(tmp_path, tenant_id="disabled-id"),
    }
    if enabled or mixed:
        tenants["enabled"] = _tenant(tmp_path, tenant_id="enabled-id", enabled=True)
    return AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        preview=PreviewConfig(artifact_root=tmp_path / "artifacts", max_workers=1),
        tenants=tenants,
    )


def _route_context(settings: AppSettings) -> tuple[Request, _NoBackendProvider]:
    app = create_app(settings)
    provider = _NoBackendProvider()
    app.state.settings = settings
    app.state.backend_provider = provider
    app.state.preview_runtime = None
    app.state.preview_revision_reader = None
    return Request({"type": "http", "app": app}), provider


def _disabled_route(
    name: str,
    *,
    request: Request,
    settings: AppSettings,
    provider: _NoBackendProvider,
) -> object:
    common = {"request": request, "tenant_name": "disabled", "settings": settings, "provider": provider}
    if name == "profile":
        return focus_preview.get_profile(tenant_name="disabled", settings=settings)
    if name == "submit":
        return focus_preview.submit_preview(
            **common,
            body=TypeAdapter(FocusPreviewRequestBody).validate_python(
                {
                    "grain": "daily",
                    "start_date": "2026-07-01",
                    "end_date": "2026-07-02",
                    "column_profile": "full",
                }
            ),
        )
    if name == "list_requests":
        return focus_preview.list_previews(**common, limit=20, cursor=None)
    if name == "get_request":
        return focus_preview.get_preview(**common, request_id="missing")
    if name == "request_manifest":
        return focus_preview.get_manifest(**common, request_id="missing")
    if name == "request_file":
        return focus_preview.get_file(**common, request_id="missing", file_name="data.csv")
    if name == "request_archive":
        return focus_preview.get_archive(**common, request_id="missing")
    if name == "list_revisions":
        return focus_preview.list_revisions(**common, month="2026-07", limit=20, cursor=None)
    if name == "current":
        return focus_preview.get_current_revision(**common, month="2026-07", revision_id=None)
    if name == "current_manifest":
        return focus_preview.get_current_revision_manifest(**common, month="2026-07", revision_id="rev-1")
    if name == "current_file":
        return focus_preview.get_current_revision_file(
            **common, month="2026-07", revision_id="rev-1", file_name="data.csv"
        )
    if name == "current_archive":
        return focus_preview.get_current_revision_archive(**common, month="2026-07", revision_id="rev-1")
    if name == "revision":
        return focus_preview.get_revision(**common, revision_id="rev-1")
    if name == "revision_manifest":
        return focus_preview.get_revision_manifest(**common, revision_id="rev-1")
    if name == "revision_file":
        return focus_preview.get_revision_file(**common, revision_id="rev-1", file_name="data.csv")
    if name == "revision_archive":
        return focus_preview.get_revision_archive(**common, revision_id="rev-1")
    raise AssertionError(f"unknown route case: {name}")


def test_canonical_focus_preview_enablement_is_derived_from_tenants(tmp_path: Path) -> None:
    disabled = _settings(tmp_path)
    mixed = _settings(tmp_path, mixed=True)
    unsupported = _tenant(tmp_path, tenant_id="generic", ecosystem="generic_metrics_only", enabled=True)

    assert disabled.tenants["disabled"].focus_preview_enabled is False
    assert disabled.focus_preview_enabled is False
    assert mixed.tenants["enabled"].focus_preview_enabled is True
    assert mixed.focus_preview_enabled is True
    assert unsupported.focus_preview_enabled is False


def test_all_disabled_api_lifespan_never_touches_preview_artifact_root(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    artifact_root = settings.preview.artifact_root
    artifact_root.write_bytes(b"not a directory")
    app = create_app(settings)

    async def enter_and_exit() -> None:
        async with app.router.lifespan_context(app):
            assert app.state.preview_artifact_store is None
            assert app.state.preview_runtime is None
            assert app.state.preview_revision_reader is None

    import asyncio

    asyncio.run(enter_and_exit())
    assert artifact_root.read_bytes() == b"not a directory"


def test_disabled_worker_construction_does_not_construct_preview_objects(tmp_path: Path) -> None:
    from main import _create_runner

    settings = _settings(tmp_path)
    with (
        patch("core.preview.artifacts.LocalPreviewArtifactStore") as artifact_store,
        patch("core.preview.generator.PreviewPackageGenerator") as package_generator,
        patch("core.preview.revisions.PreviewRevisionService") as revision_service,
    ):
        runner = _create_runner(settings)

    artifact_store.assert_not_called()
    package_generator.assert_not_called()
    revision_service.assert_not_called()
    runner.close()
    assert not settings.preview.artifact_root.exists()


def test_all_disabled_combined_mode_uses_no_preview_artifact_state(tmp_path: Path) -> None:
    from main import _create_runner

    settings = _settings(tmp_path)
    artifact_root = settings.preview.artifact_root
    artifact_root.write_bytes(b"not a directory")
    runner = _create_runner(settings)
    app = create_app(settings, workflow_runner=runner, mode="both")

    async def enter_and_exit() -> None:
        async with app.router.lifespan_context(app):
            assert app.state.preview_artifact_store is None
            assert app.state.preview_runtime is None
            assert app.state.preview_revision_reader is None

    import asyncio

    asyncio.run(enter_and_exit())
    assert artifact_root.read_bytes() == b"not a directory"


def test_enabled_api_lifespan_owns_one_provider_and_recovery_is_idempotent(tmp_path: Path) -> None:
    import asyncio

    settings = _settings(tmp_path, enabled=True)
    disabled_plugin = MagicMock()
    disabled_plugin.get_storage_module.return_value = CCloudStorageModule()
    enabled_plugin = MagicMock()
    enabled_plugin.get_storage_module.return_value = CCloudStorageModule()
    registry = MagicMock()
    registry.create.side_effect = [enabled_plugin, disabled_plugin]
    app = create_app(settings, mode="api", plugin_registry=registry)

    async def enter_recover_and_exit() -> None:
        async with app.router.lifespan_context(app):
            provider = app.state.backend_provider
            assert isinstance(provider, ApiTenantBackendProvider)
            runtime = app.state.preview_runtime
            tenant = settings.tenants["enabled"]
            constructed = registry.create.call_count
            recover_preview_owner("enabled", tenant, provider, runtime)
            recover_preview_owner("enabled", tenant, provider, runtime)
            with (
                provider.acquire_backend("enabled", tenant) as first,
                provider.acquire_backend("enabled", tenant) as second,
            ):
                assert first is second
                assert first.preview_evidence_availability.state.value == "ready"
            assert registry.create.call_count == constructed == 2

    asyncio.run(enter_recover_and_exit())
    disabled_plugin.close.assert_called_once_with()
    enabled_plugin.close.assert_called_once_with()


@pytest.mark.parametrize(
    "route_name",
    [
        "profile",
        "list_requests",
        "get_request",
        "request_manifest",
        "request_file",
        "request_archive",
        "list_revisions",
        "current",
        "current_manifest",
        "current_file",
        "current_archive",
        "revision",
        "revision_manifest",
        "revision_file",
        "revision_archive",
        "submit",
    ],
)
def test_valid_disabled_preview_routes_return_canonical_409_without_backend(
    tmp_path: Path,
    route_name: str,
) -> None:
    settings = _settings(tmp_path)
    request, provider = _route_context(settings)

    with pytest.raises(HTTPException) as exc_info:
        _disabled_route(route_name, request=request, settings=settings, provider=provider)

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == DISABLED_DETAIL["detail"]
    assert provider.acquire_count == 0


def test_framework_validation_precedes_unknown_tenant_and_backend(tmp_path: Path) -> None:
    _request_value, provider = _route_context(_settings(tmp_path))

    with pytest.raises(ValidationError):
        TypeAdapter(FocusPreviewRequestBody).validate_python(
            {
                "grain": "hourly",
                "start_date": "not-a-date",
                "end_date": "2026-07-02",
                "column_profile": "unknown",
            }
        )
    assert provider.acquire_count == 0


def test_unknown_tenant_precedes_structurally_valid_post_semantics(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    request, provider = _route_context(settings)
    body = TypeAdapter(FocusPreviewRequestBody).validate_python(
        {
            "grain": "daily",
            "start_date": "2026-07-02",
            "end_date": "2026-07-01",
            "column_profile": "full",
        }
    )

    with pytest.raises(HTTPException) as exc_info:
        focus_preview.submit_preview(
            request=request,
            tenant_name="unknown",
            body=body,
            settings=settings,
            provider=provider,
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Tenant 'unknown' not found"
    assert provider.acquire_count == 0


def test_invalid_revision_month_precedes_unknown_tenant_and_backend(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    request, provider = _route_context(settings)

    with pytest.raises(HTTPException) as exc_info:
        focus_preview.list_revisions(
            request=request,
            tenant_name="unknown",
            month="2026-13",
            settings=settings,
            provider=provider,
            limit=20,
            cursor=None,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "month must use YYYY-MM"
    assert provider.acquire_count == 0


def test_framework_query_validation_precedes_unknown_tenant_and_backend(tmp_path: Path) -> None:
    _request_value, provider = _route_context(_settings(tmp_path))
    annotation = get_type_hints(focus_preview.list_revisions, include_extras=True)["limit"]
    assert "limit" in signature(focus_preview.list_revisions).parameters

    with pytest.raises(ValidationError):
        TypeAdapter(annotation).validate_python(0)
    assert provider.acquire_count == 0


def test_enabled_missing_runtime_keeps_existing_503_contract(tmp_path: Path) -> None:
    settings = _settings(tmp_path, enabled=True)
    request, provider = _route_context(settings)
    body = TypeAdapter(FocusPreviewRequestBody).validate_python(
        {
            "grain": "daily",
            "start_date": "2026-07-01",
            "end_date": "2026-07-02",
            "column_profile": "full",
        }
    )

    with pytest.raises(HTTPException) as exc_info:
        focus_preview.submit_preview(
            request=request,
            tenant_name="enabled",
            body=body,
            settings=settings,
            provider=provider,
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "FOCUS Mapping Preview runtime is unavailable"
    assert provider.acquire_count == 0


def test_enabled_missing_revision_reader_keeps_existing_503_contract(tmp_path: Path) -> None:
    settings = _settings(tmp_path, enabled=True)
    request, provider = _route_context(settings)

    with pytest.raises(HTTPException) as exc_info:
        focus_preview.get_current_revision(
            request=request,
            tenant_name="enabled",
            month="2026-07",
            settings=settings,
            provider=provider,
            revision_id=None,
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "FOCUS Mapping Preview revision service is unavailable"
    assert provider.acquire_count == 0

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import time
import zipfile
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import anyio.to_thread
import httpx
import pytest
from sqlalchemy import text
from sqlmodel import Session

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, StorageConfig, TenantConfig
from core.models.identity import CoreIdentity
from core.models.pipeline import PipelineState
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.test_service import _aggregate, _allocation, _seed, _source

if TYPE_CHECKING:
    from plugins.confluent_cloud.models.billing import CCloudCostSourceRecord


@pytest.fixture(autouse=True)
def _inline_mocked_startup_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid the Python 3.14.6 asyncio.to_thread shutdown fault in isolated API runs."""

    async def run_inline(function: Callable[..., object], *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Callable[..., object], *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", run_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)


class SameThreadApiClient:
    """Drive ASGI and lifespan on one loop; avoids the sandbox's broken cross-thread portal."""

    def __init__(self, app: object) -> None:
        self._app = app
        self._loop = asyncio.new_event_loop()
        self._lifespan: object | None = None
        self._client: httpx.AsyncClient | None = None

    def __enter__(self) -> SameThreadApiClient:
        self._lifespan = self._app.router.lifespan_context(self._app)  # type: ignore[attr-defined]
        self._loop.run_until_complete(self._lifespan.__aenter__())  # type: ignore[attr-defined]
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self._app),  # type: ignore[arg-type]
            base_url="http://testserver",
        )
        self._loop.run_until_complete(self._client.__aenter__())
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        assert self._client is not None
        assert self._lifespan is not None
        self._loop.run_until_complete(self._client.__aexit__(exc_type, exc_value, traceback))
        self._loop.run_until_complete(self._lifespan.__aexit__(exc_type, exc_value, traceback))  # type: ignore[attr-defined]
        self._loop.close()

    def get(self, url: str, **kwargs: object) -> httpx.Response:
        assert self._client is not None
        return self._loop.run_until_complete(self._client.get(url, **kwargs))  # type: ignore[arg-type]

    def post(self, url: str, **kwargs: object) -> httpx.Response:
        assert self._client is not None
        return self._loop.run_until_complete(self._client.post(url, **kwargs))  # type: ignore[arg-type]


class SameThreadCliClient:
    """Adapt the same-thread ASGI client to the synchronous CLI client contract."""

    def __init__(self, api_client: SameThreadApiClient) -> None:
        self._api_client = api_client
        self.submitted_request_id: str | None = None

    def __enter__(self) -> SameThreadCliClient:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def get(self, url: str, **kwargs: object) -> httpx.Response:
        return self._api_client.get(url, **kwargs)

    def post(self, url: str, **kwargs: object) -> httpx.Response:
        response = self._api_client.post(url, **kwargs)
        if response.status_code == 202:
            self.submitted_request_id = response.json()["request_id"]
        return response

    @contextmanager
    def stream(self, method: str, url: str, **kwargs: object) -> Iterator[httpx.Response]:
        assert method == "GET"
        yield self.get(url, **kwargs)


def _settings(tmp_path: Path, *, ecosystem: str = "confluent_cloud") -> AppSettings:
    config = import_module("core.config.models")
    return AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        preview=config.PreviewConfig(artifact_root=tmp_path / "artifacts", max_workers=1),
        tenants={
            "production": TenantConfig(
                ecosystem=ecosystem,
                tenant_id="tenant-1",
                storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'preview.db'}"),
                focus_preview={
                    "commercial_profile": "direct_payg",
                    "billing_currency": "USD",
                    "effective_start_date": "2020-01-01",
                    "effective_end_date": "2030-01-01",
                },
            )
        },
    )


def _client(settings: AppSettings):
    app = create_app(settings)
    return app, SameThreadApiClient(app)


def _body() -> dict[str, str]:
    return {
        "grain": "daily",
        "start_date": "2026-07-01",
        "end_date": "2026-07-02",
        "column_profile": "full",
    }


def _wait_for_terminal(
    client: SameThreadApiClient,
    request_id: str,
    *,
    tenant_name: str = "production",
) -> dict[str, object]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/tenants/{tenant_name}/focus-preview/requests/{request_id}")
        body = response.json()
        if body["status"] in {"ready", "failed", "expired"}:
            return body
        time.sleep(0.01)
    pytest.fail("preview request did not reach a terminal state")


def _assert_terminal_failure(
    body: dict[str, object],
    *,
    code: str,
    message: str,
    retryable: bool,
    correlation_count: int = 0,
) -> None:
    assert body["status"] == "failed"
    diagnostic = body["diagnostic"]
    assert isinstance(diagnostic, dict)
    expected: dict[str, object] = {"code": code, "message": message, "retryable": retryable}
    if correlation_count:
        correlations = diagnostic["source_correlation_ids"]
        assert isinstance(correlations, list)
        assert len(correlations) == correlation_count
        assert all(isinstance(value, str) and value.startswith("src:v1:") for value in correlations)
        expected["source_correlation_ids"] = correlations
    assert diagnostic == expected
    assert body["source_snapshot"] is None
    assert body["package"] is None


class BlockingExecutor:
    def __init__(self) -> None:
        self.task: Callable[[], None] | None = None

    def submit(self, task: Callable[[], None]) -> Future[None]:
        self.task = task
        return Future()

    def run(self) -> None:
        assert self.task is not None
        self.task()

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        del wait, cancel_futures


@pytest.mark.parametrize(
    ("body", "status", "detail"),
    [
        (
            {"grain": "daily", "start_date": "2026-07-02", "end_date": "2026-07-02", "column_profile": "full"},
            400,
            "start_date must be before end_date",
        ),
        (
            {"grain": "daily", "start_date": "2026-07-02", "end_date": "2026-07-01", "column_profile": "full"},
            400,
            "start_date must be before end_date",
        ),
        (
            {"grain": "daily", "start_date": "2026-07-31", "end_date": "2026-08-02", "column_profile": "full"},
            400,
            "Daily preview range must stay within one UTC calendar month",
        ),
    ],
)
def test_post_validates_dates_before_backend_creation(
    tmp_path: Path,
    body: dict[str, str],
    status: int,
    detail: str,
) -> None:
    settings = _settings(tmp_path)
    route = import_module("core.api.routes.focus_preview")
    app, client = _client(settings)
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "get_or_create_backend") as backend_factory,
        client,
    ):
        response = client.post("/api/v1/tenants/production/focus-preview/requests", json=body)

    assert response.status_code == status
    assert response.json() == {"detail": detail}
    backend_factory.assert_not_called()


def test_invalid_body_uses_fastapi_422_contract(tmp_path: Path) -> None:
    app, client = _client(_settings(tmp_path))
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        response = client.post(
            "/api/v1/tenants/production/focus-preview/requests",
            json={"grain": "hourly", "start_date": "bad", "end_date": "2026-07-02", "column_profile": "thin"},
        )

    assert response.status_code == 422
    assert isinstance(response.json()["detail"], list)


def test_unknown_tenant_and_unsupported_ecosystem_are_cheap_exact_errors(tmp_path: Path) -> None:
    route = import_module("core.api.routes.focus_preview")
    app, client = _client(_settings(tmp_path, ecosystem="test-eco"))
    body = {"grain": "daily", "start_date": "2026-07-01", "end_date": "2026-07-02", "column_profile": "full"}
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "get_or_create_backend") as backend_factory,
        client,
    ):
        unknown = client.post("/api/v1/tenants/unknown/focus-preview/requests", json=body)
        unsupported = client.post("/api/v1/tenants/production/focus-preview/requests", json=body)

    assert unknown.status_code == 404
    assert unknown.json() == {"detail": "Tenant 'unknown' not found"}
    assert unsupported.status_code == 400
    assert unsupported.json() == {"detail": "FOCUS Mapping Preview currently supports only Confluent Cloud tenants"}
    backend_factory.assert_not_called()


@pytest.mark.parametrize(
    "suffix",
    ["", "/request-1", "/request-1/manifest", "/request-1/files/cost-and-usage.csv", "/request-1/archive"],
)
def test_unknown_tenant_and_unsupported_ecosystem_are_exact_for_every_get_endpoint(
    tmp_path: Path,
    suffix: str,
) -> None:
    route = import_module("core.api.routes.focus_preview")
    app, client = _client(_settings(tmp_path, ecosystem="test-eco"))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "get_or_create_backend") as backend_factory,
        client,
    ):
        unknown = client.get(f"/api/v1/tenants/unknown/focus-preview/requests{suffix}")
        unsupported = client.get(f"/api/v1/tenants/production/focus-preview/requests{suffix}")

    assert unknown.status_code == 404
    assert unknown.json() == {"detail": "Tenant 'unknown' not found"}
    assert unsupported.status_code == 400
    assert unsupported.json() == {"detail": "FOCUS Mapping Preview currently supports only Confluent Cloud tenants"}
    backend_factory.assert_not_called()


def test_post_runtime_unavailable_precedes_backend_creation(tmp_path: Path) -> None:
    route = import_module("core.api.routes.focus_preview")
    app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "get_or_create_backend") as backend_factory,
        client,
    ):
        app.state.preview_runtime = None
        response = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview runtime is unavailable"}
    backend_factory.assert_not_called()


@pytest.mark.parametrize(
    "suffix",
    [
        "",
        "/manifest",
        "/files/cost-and-usage.csv",
        "/archive",
    ],
)
def test_get_runtime_unavailable_precedes_storage_and_not_found(tmp_path: Path, suffix: str) -> None:
    route = import_module("core.api.routes.focus_preview")
    app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "get_or_create_backend") as backend_factory,
        client,
    ):
        app.state.preview_runtime = None
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/missing{suffix}")

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview runtime is unavailable"}
    backend_factory.assert_not_called()


@pytest.mark.parametrize("suffix", ["", "/manifest", "/files/cost-and-usage.csv", "/archive"])
def test_storage_unavailable_precedes_request_lookup(tmp_path: Path, suffix: str) -> None:
    route = import_module("core.api.routes.focus_preview")
    app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "get_or_create_backend", return_value=object()) as backend_factory,
        client,
    ):
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/missing{suffix}")

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview storage is unavailable"}
    backend_factory.assert_called_once()


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/api/v1/tenants/production/focus-preview/requests"),
        ("get", "/api/v1/tenants/production/focus-preview/requests/missing"),
        ("get", "/api/v1/tenants/production/focus-preview/requests/missing/manifest"),
        ("get", "/api/v1/tenants/production/focus-preview/requests/missing/files/cost-and-usage.csv"),
        ("get", "/api/v1/tenants/production/focus-preview/requests/missing/archive"),
    ],
)
def test_backend_construction_exception_is_exact_storage_503(
    tmp_path: Path,
    method: str,
    path: str,
) -> None:
    route = import_module("core.api.routes.focus_preview")
    app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "get_or_create_backend", side_effect=RuntimeError("database password sentinel")),
        client,
    ):
        response = getattr(client, method)(path, json=_body()) if method == "post" else getattr(client, method)(path)

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview storage is unavailable"}
    assert "sentinel" not in response.text


def test_post_worker_unavailable_has_exact_503_body(tmp_path: Path) -> None:
    service = import_module("core.preview.service")
    app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        client,
        patch.object(
            app.state.preview_runtime,
            "submit",
            side_effect=service.PreviewWorkerUnavailable("scheduler sentinel"),
        ),
    ):
        response = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview worker is unavailable"}
    assert "sentinel" not in response.text


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/api/v1/tenants/production/focus-preview/requests"),
        ("get", "/api/v1/tenants/production/focus-preview/requests"),
        ("get", "/api/v1/tenants/production/focus-preview/requests/missing"),
        ("get", "/api/v1/tenants/production/focus-preview/requests/missing/manifest"),
        ("get", "/api/v1/tenants/production/focus-preview/requests/missing/files/unknown.csv"),
        ("get", "/api/v1/tenants/production/focus-preview/requests/missing/archive"),
    ],
)
def test_recovery_unavailable_precedes_create_lookup_cursor_state_and_bytes(
    tmp_path: Path,
    method: str,
    path: str,
) -> None:
    service = import_module("core.preview.service")
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        with patch.object(
            app.state.preview_runtime,
            "ensure_owner_recovered",
            side_effect=service.PreviewRecoveryUnavailable("database sentinel"),
        ):
            response = client.post(path, json=_body()) if method == "post" else client.get(path)
        with backend.create_preview_read_unit_of_work() as uow:
            persisted_items = uow.requests.list_recent_for_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                limit=20,
                cursor_request_id=None,
            ).items

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview recovery is unavailable"}
    assert "sentinel" not in response.text
    assert persisted_items == ()


def test_focus_preview_status_routes_publish_typed_openapi_response_contract(tmp_path: Path) -> None:
    app, _client_value = _client(_settings(tmp_path))
    schema = app.openapi()
    path = "/api/v1/tenants/{tenant_name}/focus-preview/requests"
    status_path = f"{path}/{{request_id}}"

    post_schema = schema["paths"][path]["post"]["responses"]["202"]["content"]["application/json"]["schema"]
    get_schema = schema["paths"][status_path]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert post_schema == {"$ref": "#/components/schemas/FocusPreviewResponse"}
    assert get_schema == post_schema
    response_schema = schema["components"]["schemas"]["FocusPreviewResponse"]
    assert set(response_schema["required"]) == {
        "request_id",
        "tenant_name",
        "grain",
        "start_date",
        "end_date",
        "month",
        "column_profile",
        "effective_columns",
        "status",
        "created_at",
        "started_at",
        "completed_at",
        "expires_at",
        "diagnostic",
        "source_snapshot",
        "package",
    }
    diagnostic_schema = schema["components"]["schemas"]["FocusPreviewDiagnosticResponse"]
    assert diagnostic_schema["properties"]["source_correlation_ids"] == {
        "items": {"type": "string"},
        "title": "Source Correlation Ids",
        "type": "array",
    }


def test_primary_api_seam_serializes_safe_diagnostic_correlations_and_no_internal_fields(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(malformed=True, diagnostics=("provider secret diagnostic",)),
        aggregate=_aggregate(),
        allocation=_allocation(),
    )
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        assert submitted.status_code == 202
        body = _wait_for_terminal(client, submitted.json()["request_id"])

    assert body["status"] == "failed"
    assert body["diagnostic"]["code"] == "preview_source_record_malformed"
    assert len(body["diagnostic"]["source_correlation_ids"]) == 1
    assert body["diagnostic"]["source_correlation_ids"][0].startswith("src:v1:")
    assert "provider secret" not in str(body)
    assert "source_record_id" not in str(body)
    assert "storage_key" not in str(body)
    assert body["source_snapshot"] is None
    assert body["package"] is None


@pytest.mark.parametrize(
    ("account_state", "code", "message"),
    [
        (
            "missing",
            "preview_billing_account_unavailable",
            "Authoritative Confluent Cloud organization evidence is unavailable for this tenant.",
        ),
        (
            "conflicting",
            "preview_billing_account_conflicting",
            "Persisted Confluent Cloud organization evidence conflicts for this tenant.",
        ),
    ],
)
def test_primary_api_seam_transports_exact_billing_account_diagnostic(
    tmp_path: Path,
    account_state: str,
    code: str,
    message: str,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    with backend.create_unit_of_work() as uow:
        if account_state == "missing":
            uow.resources.mark_deleted(
                "confluent_cloud",
                "tenant-1",
                "11111111-2222-4333-8444-555555555555",
                datetime(2026, 7, 3, tzinfo=UTC),
            )
        else:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    resource_id="22222222-3333-4444-8555-666666666666",
                    resource_type="organization",
                    display_name="Conflicting provider organization",
                    status=ResourceStatus.ACTIVE,
                    metadata={"organization_binding_state": "conflicting_observation"},
                )
            )
        uow.commit()
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        body = _wait_for_terminal(client, submitted.json()["request_id"])

    _assert_terminal_failure(
        body,
        code=code,
        message=message,
        retryable=False,
        correlation_count=1,
    )


def test_primary_api_seam_transports_exact_row_validation_diagnostic(tmp_path: Path) -> None:
    generator = import_module("core.preview.generator")
    mapping = import_module("core.preview.mapping")
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    validation_error = mapping.PreviewRowValidationError(mapping.PreviewRowRuleId.TYPE, column="BillingAccountId")
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(generator, "build_preview_data_package", side_effect=validation_error),
        client,
    ):
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        body = _wait_for_terminal(client, submitted.json()["request_id"])

    _assert_terminal_failure(
        body,
        code="preview_mapping_validation_failed",
        message="The generated row does not satisfy the Daily Full mapping profile.",
        retryable=False,
        correlation_count=1,
    )


def _mapper_backed_malformed_source() -> CCloudCostSourceRecord:
    cost_input = import_module("plugins.confluent_cloud.cost_input")
    candidate_type = cost_input._SourceCandidate
    mapper = cost_input._map_source_record
    candidate = candidate_type(
        raw_payload={
            "id": "malformed-provider-cost",
            "start_date": "not-a-date",
            "end_date": "2026-07-02",
            "product": "KAFKA",
            "line_type": "KAFKA_STORAGE",
            "amount": "8",
            "original_amount": "10",
            "discount_amount": "2",
            "price": "2",
            "quantity": "5",
            "unit": "GB",
            "description": "Kafka storage usage",
            "resource": {"id": "lkc-1", "environment": {"id": "env-1"}},
        },
        collection_window_start=datetime(2026, 7, 1, tzinfo=UTC),
        collection_window_end=datetime(2026, 7, 2, tzinfo=UTC),
        ordinal=0,
        billing_key=(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 7, 1, tzinfo=UTC),
            "env-1",
            "lkc-1",
            "KAFKA_STORAGE",
            "KAFKA",
        ),
    )
    mapped: CCloudCostSourceRecord = mapper(
        candidate,
        "tenant-1",
        "provider:malformed-provider-cost",
        "provider_cost_id",
        "malformed-provider-cost",
    )
    return mapped


@pytest.mark.parametrize(
    ("source_changes", "expected_code"),
    [
        (
            {
                "source_period_start": datetime(2026, 6, 30, tzinfo=UTC),
                "allocation_timestamp": datetime(2026, 6, 30, tzinfo=UTC),
                "retention_timestamp": datetime(2026, 6, 30, tzinfo=UTC),
            },
            "preview_source_scope_unsupported",
        ),
        ({"description": "Prior period refund"}, "preview_charge_classification_ambiguous"),
        ({"line_type": ""}, "preview_source_line_type_unknown"),
        ({"line_type": "FUTURE_LINE"}, "preview_source_line_type_unsupported"),
        ({"line_type": "SUPPORT"}, "preview_charge_classification_ambiguous"),
        (
            {"line_type": "SUPPORT", "product": "SUPPORT_CLOUD_BUSINESS", "description": "Support subscription"},
            "preview_source_coverage_incomplete",
        ),
        ({"resource_id": None}, "preview_source_record_incomplete"),
        ({"amount": 0}, "preview_source_economics_unsupported"),
        ({"amount": 7}, "preview_source_reconciliation_failed"),
    ],
)
def test_primary_api_seam_persists_each_source_eligibility_category(
    tmp_path: Path,
    source_changes: dict[str, object],
    expected_code: str,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(**source_changes),
        aggregate=_aggregate(),
        allocation=_allocation(),
    )
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        body = _wait_for_terminal(client, submitted.json()["request_id"])

    assert body["status"] == "failed"
    assert body["diagnostic"]["code"] == expected_code
    assert len(body["diagnostic"]["source_correlation_ids"]) == 1
    assert body["source_snapshot"] is None
    assert body["package"] is None


def test_primary_api_seam_uses_real_source_mapper_for_malformed_diagnostic(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_mapper_backed_malformed_source(),
        aggregate=_aggregate(),
        allocation=_allocation(),
    )
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        body = _wait_for_terminal(client, submitted.json()["request_id"])

    assert body["status"] == "failed"
    assert body["diagnostic"]["code"] == "preview_source_record_malformed"
    assert len(body["diagnostic"]["source_correlation_ids"]) == 1


def test_primary_api_unknown_unsupported_line_precedes_valid_kafka_streams(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, aggregate=_aggregate(), allocation=_allocation())
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 3, tzinfo=UTC),
            [
                _source(source_record_id="provider:future", provider_cost_id="future", line_type="FUTURE_LINE"),
                _source(source_record_id="provider:streams", provider_cost_id="streams", line_type="KAFKA_STREAMS"),
            ],
        )
        uow.commit()
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        body = _wait_for_terminal(client, submitted.json()["request_id"])

    assert body["status"] == "failed"
    assert body["diagnostic"]["code"] == "preview_source_line_type_unsupported"
    assert len(body["diagnostic"]["source_correlation_ids"]) == 1


def test_primary_api_seam_missing_focus_preview_fails_only_requested_scope(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.tenants["production"] = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'preview.db'}"),
    )
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        generic_before = client.post(
            "/api/v1/tenants/production/export",
            json={"start_date": "2026-07-01", "end_date": "2026-07-02"},
        )
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        body = _wait_for_terminal(client, submitted.json()["request_id"])
        generic_after = client.post(
            "/api/v1/tenants/production/export",
            json={"start_date": "2026-07-01", "end_date": "2026-07-02"},
        )

    assert body["status"] == "failed"
    assert body["diagnostic"] == {
        "code": "preview_commercial_profile_unavailable",
        "message": "An explicit Direct-billed PAYG profile does not cover the requested interval.",
        "retryable": False,
    }
    assert generic_before.status_code == generic_after.status_code == 200
    assert generic_after.content == generic_before.content


@pytest.mark.parametrize(
    ("tracking_date", "code", "message", "retryable"),
    [
        (
            date(2026, 6, 23),
            "calculation_before_acquisition_lookback",
            "Required retained calculation evidence is unavailable outside the current acquisition window.",
            False,
        ),
        (
            date(2026, 6, 29),
            "calculation_pending_cutoff_window",
            "One or more requested dates are still inside the configured acquisition cutoff window; "
            "wait for the dates to enter the acquisition window, run the pipeline, and retry.",
            True,
        ),
    ],
)
def test_primary_api_calculation_window_failure_reaches_exact_terminal_status(
    tmp_path: Path,
    tracking_date: date,
    code: str,
    message: str,
    retryable: bool,
) -> None:
    settings = _settings(tmp_path)
    tenant = settings.tenants["production"]
    settings.tenants["production"] = tenant.model_copy(update={"lookback_days": 10, "cutoff_days": 5})
    backend = SQLModelBackend(
        tenant.storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(
        backend,
        state=PipelineState(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tracking_date=tracking_date,
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=False,
            calculation_id=None,
            calculation_completed_at=None,
            calculation_run_id=None,
        ),
    )
    app, client = _client(settings)
    body = {
        **_body(),
        "start_date": tracking_date.isoformat(),
        "end_date": tracking_date.replace(day=tracking_date.day + 1).isoformat(),
    }
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        with patch.object(app.state.preview_runtime, "_clock", lambda: datetime(2026, 7, 4, tzinfo=UTC)):
            submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=body)
            assert submitted.status_code == 202
            terminal = _wait_for_terminal(client, submitted.json()["request_id"])

    _assert_terminal_failure(terminal, code=code, message=message, retryable=retryable)


def test_primary_api_unknown_persisted_currency_reaches_exact_terminal_status(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(currency=""), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        assert submitted.status_code == 202
        terminal = _wait_for_terminal(client, submitted.json()["request_id"])

    _assert_terminal_failure(
        terminal,
        code="preview_billing_currency_unknown",
        message="Persisted billing currency evidence is unknown for one or more source records.",
        retryable=False,
        correlation_count=1,
    )


def test_primary_api_multiple_valid_sources_reach_exact_mapping_scope_terminal_status(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, aggregate=_aggregate(), allocation=_allocation())
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 3, tzinfo=UTC),
            [
                _source(),
                _source(source_record_id="provider:cost-2", provider_cost_id="cost-2"),
            ],
        )
        uow.commit()
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        assert submitted.status_code == 202
        terminal = _wait_for_terminal(client, submitted.json()["request_id"])

    _assert_terminal_failure(
        terminal,
        code="preview_mapping_scope_unsupported",
        message="The complete source set exceeds the current Daily Full mapping scope.",
        retryable=False,
        correlation_count=2,
    )
    assert "provider:cost-1" not in str(terminal)
    assert "provider:cost-2" not in str(terminal)


def test_primary_api_non_usd_configuration_fails_without_provider_or_artifact(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    tenant = settings.tenants["production"]
    settings.tenants["production"] = TenantConfig(
        ecosystem=tenant.ecosystem,
        tenant_id=tenant.tenant_id,
        storage=tenant.storage,
        focus_preview={
            "commercial_profile": "direct_payg",
            "billing_currency": "EUR",
            "effective_start_date": "2020-01-01",
            "effective_end_date": "2030-01-01",
        },
    )
    backend = SQLModelBackend(
        tenant.storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        body = _wait_for_terminal(client, submitted.json()["request_id"])

    assert body["status"] == "failed"
    assert body["diagnostic"] == {
        "code": "preview_billing_currency_unsupported",
        "message": "FOCUS Mapping Preview currently supports only USD billing currency.",
        "retryable": False,
    }
    assert body["source_snapshot"] is None
    assert body["package"] is None


def test_primary_api_failure_isolated_across_tenant_databases_and_non_overlapping_interval(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.tenants["sandbox"] = TenantConfig.model_validate(
        {
            "ecosystem": "confluent_cloud",
            "tenant_id": "tenant-2",
            "storage": StorageConfig(connection_string=f"sqlite:///{tmp_path / 'sandbox-preview.db'}"),
            "focus_preview": {
                "commercial_profile": "direct_payg",
                "billing_currency": "USD",
                "effective_start_date": "2020-01-01",
                "effective_end_date": "2030-01-01",
            },
        }
    )
    production_backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    sandbox_backend = SQLModelBackend(
        settings.tenants["sandbox"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    production_backend.create_tables()
    sandbox_backend.create_tables()
    _seed(
        production_backend,
        source=_source(malformed=True, diagnostics=("provider malformed",)),
        aggregate=_aggregate(),
        allocation=_allocation(),
    )
    sandbox_source = _source(
        tenant_id="tenant-2",
        source_record_id="provider:cost-2",
        provider_cost_id="cost-2",
        source_period_start=datetime(2026, 7, 2, tzinfo=UTC),
        source_period_end=datetime(2026, 7, 3, tzinfo=UTC),
        collection_window_start=datetime(2026, 7, 1, tzinfo=UTC),
        collection_window_end=datetime(2026, 7, 4, tzinfo=UTC),
        evidence_scope_start=datetime(2026, 7, 2, tzinfo=UTC),
        evidence_scope_end=datetime(2026, 7, 3, tzinfo=UTC),
        allocation_timestamp=datetime(2026, 7, 2, tzinfo=UTC),
        retention_timestamp=datetime(2026, 7, 2, tzinfo=UTC),
    )
    with sandbox_backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="tenant-2",
                resource_id="22222222-3333-4444-8555-666666666666",
                resource_type="organization",
                display_name="Sandbox provider organization",
                status=ResourceStatus.ACTIVE,
                metadata={"organization_binding_state": "bound"},
            )
        )
        uow.resources.upsert(
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="tenant-2",
                resource_id="env-1",
                resource_type="environment",
                display_name="Sandbox production",
                status=ResourceStatus.ACTIVE,
            )
        )
        uow.resources.upsert(
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="tenant-2",
                resource_id="lkc-1",
                resource_type="kafka_cluster",
                display_name="Sandbox orders",
                parent_id="env-1",
                status=ResourceStatus.ACTIVE,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={
                    "cloud": "aws",
                    "region": "us-east-1",
                    "provider_cloud": "AWS",
                    "provider_region": "us-east-1",
                },
            )
        )
        uow.identities.upsert(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="tenant-2",
                identity_id="sa-1",
                identity_type="service_account",
                display_name="Sandbox service",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-2",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [sandbox_source],
        )
        uow.billing.upsert(
            _aggregate(
                tenant_id="tenant-2",
                timestamp=datetime(2026, 7, 2, tzinfo=UTC),
            )
        )
        uow.chargebacks.upsert_batch(
            [
                _allocation(
                    tenant_id="tenant-2",
                    timestamp=datetime(2026, 7, 2, tzinfo=UTC),
                )
            ]
        )
        uow.pipeline_state.upsert(
            PipelineState(
                ecosystem="confluent_cloud",
                tenant_id="tenant-2",
                tracking_date=date(2026, 7, 2),
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
                calculation_id="calculation-2",
                calculation_completed_at=datetime(2026, 7, 3, 3, tzinfo=UTC),
                calculation_run_id=None,
            )
        )
        uow.commit()
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = production_backend
        app.state.backends["sandbox"] = sandbox_backend
        first = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        first_body = _wait_for_terminal(client, first.json()["request_id"])
        second = client.post(
            "/api/v1/tenants/sandbox/focus-preview/requests",
            json={**_body(), "start_date": "2026-07-02", "end_date": "2026-07-03"},
        )
        second_body = _wait_for_terminal(client, second.json()["request_id"], tenant_name="sandbox")
        cross_tenant = client.get(f"/api/v1/tenants/sandbox/focus-preview/requests/{first.json()['request_id']}")

    assert first_body["status"] == "failed"
    assert first_body["diagnostic"]["code"] == "preview_source_record_malformed"
    assert second_body["status"] == "failed"
    assert second_body["diagnostic"]["code"] == "preview_allocation_lineage_incomplete"
    assert second_body["package"] is None
    assert cross_tenant.status_code == 404


def test_missing_request_is_tenant_scoped_404(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        response = client.get("/api/v1/tenants/production/focus-preview/requests/missing")

    assert response.status_code == 404
    assert response.json() == {"detail": "Preview request 'missing' not found"}


@pytest.mark.parametrize("limit", [0, 101])
def test_recent_request_limit_validation_is_framework_422_before_backend(
    tmp_path: Path,
    limit: int,
) -> None:
    route = import_module("core.api.routes.focus_preview")
    app, client = _client(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        patch.object(route, "get_or_create_backend") as backend_factory,
        client,
    ):
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests?limit={limit}")

    assert response.status_code == 422
    assert isinstance(response.json()["detail"], list)
    backend_factory.assert_not_called()


def test_recent_request_missing_and_foreign_cursors_share_exact_400(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    queued_factory = import_module("tests.unit.core.preview.test_persistence")._queued_request
    with backend.create_preview_write_unit_of_work() as uow:
        uow.requests.create_queued(queued_factory("foreign", "tenant-2"))
        uow.commit()
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        responses = [
            client.get(f"/api/v1/tenants/production/focus-preview/requests?cursor={cursor}")
            for cursor in ("absent", "foreign")
        ]

    assert [(response.status_code, response.json()) for response in responses] == [
        (400, {"detail": "Preview request cursor is invalid"}),
        (400, {"detail": "Preview request cursor is invalid"}),
    ]


def test_real_startup_cleans_staging_and_fails_strictly_older_pending_rows(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    queued_factory = import_module("tests.unit.core.preview.test_persistence")._queued_request
    with backend.create_preview_write_unit_of_work() as uow:
        uow.requests.create_queued(queued_factory("queued-before"))
        uow.requests.create_queued(queued_factory("running-before"))
        uow.commit()
    with backend.create_preview_write_unit_of_work() as uow:
        assert uow.requests.mark_running("running-before", datetime(2026, 7, 3, 1, tzinfo=UTC)) is not None
        uow.commit()
    backend.dispose()
    staging = settings.preview.artifact_root / f".{('a' * 32)}.staging"
    staging.mkdir(parents=True)

    app = create_app(settings)
    with SameThreadApiClient(app) as client:
        recent = client.get("/api/v1/tenants/production/focus-preview/requests?limit=20")

    assert recent.status_code == 200
    assert {item["request_id"]: item["status"] for item in recent.json()["items"]} == {
        "queued-before": "failed",
        "running-before": "failed",
    }
    assert all(
        item["diagnostic"]
        == {
            "code": "preview_generation_interrupted",
            "message": "FOCUS Mapping Preview generation was interrupted before completion.",
            "retryable": True,
        }
        for item in recent.json()["items"]
    )
    assert not staging.exists()


def test_transient_startup_recovery_failure_blocks_then_later_route_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    queued_factory = import_module("tests.unit.core.preview.test_persistence")._queued_request
    with backend.create_preview_write_unit_of_work() as uow:
        uow.requests.create_queued(queued_factory("queued-before"))
        uow.commit()
    backend.dispose()
    persistence = import_module("core.preview.persistence")
    original = persistence.SQLModelPreviewRequestRepository.fail_interrupted_before
    calls = 0

    def transient(repository: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise OSError("database sentinel")
        return original(repository, **kwargs)

    monkeypatch.setattr(
        persistence.SQLModelPreviewRequestRepository,
        "fail_interrupted_before",
        transient,
    )
    app = create_app(settings)
    with SameThreadApiClient(app) as client:
        blocked = client.get("/api/v1/tenants/production/focus-preview/requests?limit=20")
        recovered = client.get("/api/v1/tenants/production/focus-preview/requests?limit=20")

    assert blocked.status_code == 503
    assert blocked.json() == {"detail": "FOCUS Mapping Preview recovery is unavailable"}
    assert "sentinel" not in blocked.text
    assert recovered.status_code == 200
    assert recovered.json()["items"][0]["status"] == "failed"
    assert calls == 3


def test_real_lifespan_isolates_recovery_for_distinct_sqlite_backends_with_shared_provider_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_id = "shared-provider-tenant"
    database_paths = {
        "tenant-a": tmp_path / "tenant-a.db",
        "tenant-b": tmp_path / "tenant-b.db",
    }
    tenants = {
        tenant_name: TenantConfig(
            ecosystem="confluent_cloud",
            tenant_id=provider_id,
            storage=StorageConfig(connection_string=f"sqlite:///{database_path}"),
            focus_preview={
                "commercial_profile": "direct_payg",
                "billing_currency": "USD",
                "effective_start_date": "2020-01-01",
                "effective_end_date": "2030-01-01",
            },
        )
        for tenant_name, database_path in database_paths.items()
    }
    settings = AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        preview={"artifact_root": tmp_path / "artifacts", "max_workers": 1},
        tenants=tenants,
    )
    queued_factory = import_module("tests.unit.core.preview.test_persistence")._queued_request
    for tenant_name, tenant_config in tenants.items():
        backend = SQLModelBackend(
            tenant_config.storage.connection_string.get_secret_value(),
            CCloudStorageModule(),
            use_migrations=False,
        )
        backend.create_tables()
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(
                replace(
                    queued_factory(f"{tenant_name}-queued", provider_id),
                    tenant_name=tenant_name,
                    created_at=datetime(2026, 7, 3, tzinfo=UTC),
                )
            )
            uow.commit()
        backend.dispose()

    persistence = import_module("core.preview.persistence")
    original = persistence.SQLModelPreviewRequestRepository.fail_interrupted_before
    calls = {"tenant-a": 0, "tenant-b": 0}

    def transient(repository: object, **kwargs: object) -> object:
        database = Path(repository._session.get_bind().url.database).name  # type: ignore[attr-defined]
        tenant_name = database.removesuffix(".db")
        calls[tenant_name] += 1
        if tenant_name == "tenant-a" and calls[tenant_name] == 1:
            raise OSError("tenant-a transient recovery failure")
        return original(repository, **kwargs)

    monkeypatch.setattr(
        persistence.SQLModelPreviewRequestRepository,
        "fail_interrupted_before",
        transient,
    )
    app = create_app(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
        assert calls == {"tenant-a": 1, "tenant-b": 1}

        tenant_b = client.get("/api/v1/tenants/tenant-b/focus-preview/requests?limit=20")
        assert tenant_b.status_code == 200
        assert [(item["request_id"], item["status"]) for item in tenant_b.json()["items"]] == [
            ("tenant-b-queued", "failed")
        ]
        assert calls == {"tenant-a": 1, "tenant-b": 1}

        tenant_a = client.get("/api/v1/tenants/tenant-a/focus-preview/requests?limit=20")
        assert tenant_a.status_code == 200
        assert [(item["request_id"], item["status"]) for item in tenant_a.json()["items"]] == [
            ("tenant-a-queued", "failed")
        ]
        assert calls == {"tenant-a": 2, "tenant-b": 1}

        repeated_b = client.get("/api/v1/tenants/tenant-b/focus-preview/requests?limit=20")
        assert repeated_b.status_code == 200
        assert calls == {"tenant-a": 2, "tenant-b": 1}


def test_real_lifespan_retries_protected_foreign_leases_against_current_clock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = import_module("core.preview.service")
    startup_at = datetime(2026, 7, 20, 12, 0, 0, tzinfo=UTC)
    clock = [startup_at]
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    queued_factory = import_module("tests.unit.core.preview.test_persistence")._queued_request
    with backend.create_preview_write_unit_of_work() as uow:
        for request_id, worker_id in (
            ("expiring-request", "foreign-expiring"),
            ("renewed-request", "foreign-renewed"),
        ):
            uow.requests.create_queued(
                replace(
                    queued_factory(request_id),
                    created_at=startup_at - timedelta(minutes=1),
                ),
                worker_id=worker_id,
                lease_expires_at=startup_at + timedelta(seconds=10),
            )
        uow.requests.create_queued(
            replace(
                queued_factory("same-second-request"),
                created_at=startup_at.replace(microsecond=500_000),
            )
        )
        uow.commit()
    backend.dispose()

    original_runtime_init = service.PreviewRuntime.__init__

    def controlled_runtime_init(runtime: object, **kwargs: Any) -> None:
        original_runtime_init(
            runtime,
            **kwargs,
            startup_at=startup_at,
            clock=lambda: clock[0],
            lease_owner_id="recovering-runtime",
        )

    monkeypatch.setattr(service.PreviewRuntime, "__init__", controlled_runtime_init)
    app = create_app(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
        initially_protected = client.get("/api/v1/tenants/production/focus-preview/requests?limit=20")
        assert initially_protected.status_code == 200, initially_protected.json()
        assert {
            item["request_id"]: (item["status"], item["diagnostic"]) for item in initially_protected.json()["items"]
        } == {
            "expiring-request": ("queued", None),
            "renewed-request": ("queued", None),
            "same-second-request": ("queued", None),
        }

        with app.state.backends["production"].create_preview_write_unit_of_work() as uow:
            assert uow.requests.renew_lease(
                "renewed-request",
                "foreign-renewed",
                startup_at + timedelta(seconds=30),
            )
            uow.commit()
        clock[0] = startup_at + timedelta(seconds=11)

        expired_foreign_lease = client.get("/api/v1/tenants/production/focus-preview/requests/expiring-request")
        after_recovery = client.get("/api/v1/tenants/production/focus-preview/requests?limit=20")

        assert expired_foreign_lease.status_code == 200
        assert expired_foreign_lease.json()["status"] == "failed"
        assert expired_foreign_lease.json()["diagnostic"] == {
            "code": "preview_generation_interrupted",
            "message": "FOCUS Mapping Preview generation was interrupted before completion.",
            "retryable": True,
        }
        assert {
            item["request_id"]: (item["status"], item["diagnostic"]) for item in after_recovery.json()["items"]
        } == {
            "expiring-request": ("failed", expired_foreign_lease.json()["diagnostic"]),
            "renewed-request": ("queued", None),
            "same-second-request": ("queued", None),
        }


def test_two_live_runtimes_reap_only_expired_foreign_post_start_leases_through_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = import_module("core.preview.service")
    artifacts = import_module("core.preview.artifacts")
    mapping = import_module("core.preview.mapping")
    immediate_executor = import_module("tests.unit.core.preview.test_service_delivery_v6").ImmediateExecutor
    startup_at = datetime(2026, 7, 20, 12, 0, 0, tzinfo=UTC)
    app_clock = [startup_at]
    peer_clock = [startup_at + timedelta(seconds=1)]
    settings = _settings(tmp_path)
    connection_string = settings.tenants["production"].storage.connection_string.get_secret_value()
    peer_backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
    )
    peer_backend.create_tables()
    peer_store = artifacts.LocalPreviewArtifactStore(tmp_path / "peer-artifacts")
    monkeypatch.setattr(service, "_PREVIEW_HEARTBEAT_INTERVAL_SECONDS", 3600)
    peer_runtime = service.PreviewRuntime(
        artifact_store=peer_store,
        max_workers=1,
        clock=lambda: peer_clock[0],
        request_id_factory=lambda: "peer-request",
        executor=immediate_executor(),
        lease_owner_id="peer-worker",
    )

    original_runtime_init = service.PreviewRuntime.__init__

    def controlled_runtime_init(runtime: object, **kwargs: Any) -> None:
        original_runtime_init(
            runtime,
            **kwargs,
            startup_at=startup_at,
            clock=lambda: app_clock[0],
            request_id_factory=lambda: "own-request",
            executor=immediate_executor(),
            lease_owner_id="app-worker",
        )

    monkeypatch.setattr(service.PreviewRuntime, "__init__", controlled_runtime_init)
    app = create_app(settings)
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
            own_submitted = client.post(
                "/api/v1/tenants/production/focus-preview/requests",
                json=_body(),
            )
            assert own_submitted.status_code == 202
            assert own_submitted.json()["request_id"] == "own-request"

            peer = peer_runtime.submit(
                tenant_name="production",
                tenant_config=settings.tenants["production"],
                backend=peer_backend,
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 2),
                grain="daily",
                column_profile="full",
                effective_columns=mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS,
            )
            assert peer.request_id == "peer-request"
            queued_factory = import_module("tests.unit.core.preview.test_persistence")._queued_request
            with peer_backend.create_preview_write_unit_of_work() as uow:
                uow.requests.create_queued(
                    replace(
                        queued_factory("same-second-missing-lease"),
                        created_at=startup_at.replace(microsecond=500_000),
                    )
                )
                uow.commit()

            initially_live = client.get("/api/v1/tenants/production/focus-preview/requests?limit=20")
            assert initially_live.status_code == 200
            assert {item["request_id"]: item["status"] for item in initially_live.json()["items"]} == {
                "own-request": "queued",
                "peer-request": "queued",
                "same-second-missing-lease": "queued",
            }

            with peer_backend.create_preview_write_unit_of_work() as uow:
                assert uow.requests.renew_lease(
                    "peer-request",
                    "peer-worker",
                    startup_at + timedelta(seconds=40),
                )
                uow.commit()
            app_clock[0] = startup_at + timedelta(seconds=31)
            renewed_peer = client.get("/api/v1/tenants/production/focus-preview/requests/peer-request")
            assert renewed_peer.status_code == 200
            assert renewed_peer.json()["status"] == "queued"

            peer_runtime.close()
            app_clock[0] = startup_at + timedelta(seconds=41)
            reaped_peer = client.get("/api/v1/tenants/production/focus-preview/requests/peer-request")
            after_crash = client.get("/api/v1/tenants/production/focus-preview/requests?limit=20")

            assert reaped_peer.status_code == 200
            assert reaped_peer.json()["status"] == "failed"
            assert reaped_peer.json()["diagnostic"] == {
                "code": "preview_generation_interrupted",
                "message": "FOCUS Mapping Preview generation was interrupted before completion.",
                "retryable": True,
            }
            assert {item["request_id"]: item["status"] for item in after_crash.json()["items"]} == {
                "own-request": "queued",
                "peer-request": "failed",
                "same-second-missing-lease": "queued",
            }
            with peer_backend._engine.connect() as connection:
                ownership = {
                    request_id: (worker_id, lease_expires_at)
                    for request_id, worker_id, lease_expires_at in connection.exec_driver_sql(
                        "SELECT request_id, worker_id, lease_expires_at FROM preview_requests "
                        "WHERE request_id IN "
                        "('own-request', 'peer-request', 'same-second-missing-lease')"
                    )
                }
            assert ownership["own-request"][0] == "app-worker"
            assert ownership["peer-request"] == (None, None)
            assert ownership["same-second-missing-lease"] == (None, None)
    finally:
        peer_runtime.close()
        peer_store.close()
        peer_backend.dispose()


def test_production_app_default_runtime_serves_exact_stored_ready_package_without_paths(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    body = {"grain": "daily", "start_date": "2026-07-01", "end_date": "2026-07-02", "column_profile": "full"}
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        export_request = {"start_date": "2026-07-01", "end_date": "2026-07-02"}
        generic_export_before = client.post("/api/v1/tenants/production/export", json=export_request)
        assert generic_export_before.status_code == 200
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=body)
        assert submitted.status_code == 202
        request_id = submitted.json()["request_id"]
        statuses = [submitted.json()["status"]]
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
            statuses.append(status.json()["status"])
            if status.json()["status"] in {"ready", "failed"}:
                break
            time.sleep(0.01)

        body_json = status.json()
        assert body_json["status"] == "ready"
        assert "queued" in statuses
        assert body_json["diagnostic"] is None
        assert body_json["expires_at"] is not None
        assert "storage_key" not in str(body_json)
        assert str(tmp_path) not in str(body_json)
        assert body_json["package"]["manifest"]["download_url"].startswith("/api/v1/")
        manifest = client.get(body_json["package"]["manifest"]["download_url"])
        csv_response = client.get(body_json["package"]["files"][0]["download_url"])
        assert manifest.status_code == 200
        assert manifest.content.startswith(b'{"')
        assert csv_response.status_code == 200
        assert csv_response.content.startswith(b"AllocatedMethodId,")
        assert body_json["package"]["manifest"]["sha256"]
        assert body_json["package"]["files"][0]["sha256"]
        assert body_json["package"]["download_all_name"] == f"focus-mapping-preview-{request_id}.zip"
        assert body_json["package"]["download_all_url"].endswith(f"/{request_id}/archive")
        manifest_json = manifest.json()
        assert hashlib.sha256(manifest.content).hexdigest() == body_json["package"]["manifest"]["sha256"]
        assert hashlib.sha256(csv_response.content).hexdigest() == manifest_json["files"][0]["sha256"]
        archive = client.get(body_json["package"]["download_all_url"])
        assert archive.status_code == 200
        assert archive.headers["content-type"].startswith("application/zip")
        with zipfile.ZipFile(io.BytesIO(archive.content)) as package_archive:
            assert package_archive.namelist() == [
                "manifest.json",
                *[item["name"] for item in manifest_json["files"]],
            ]
            assert package_archive.read("manifest.json") == manifest.content
            assert package_archive.read(manifest_json["files"][0]["name"]) == csv_response.content
        recent = client.get("/api/v1/tenants/production/focus-preview/requests?limit=20")
        assert recent.status_code == 200
        assert [item["request_id"] for item in recent.json()["items"]] == [request_id]
        assert recent.json()["next_cursor"] is None
        generic_export_after = client.post("/api/v1/tenants/production/export", json=export_request)
        assert generic_export_after.status_code == 200
        assert generic_export_after.content == generic_export_before.content

        unlisted = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}/files/unlisted.csv")
        assert unlisted.status_code == 404
        assert unlisted.json() == {"detail": f"Preview file 'unlisted.csv' not found for request '{request_id}'"}


def test_exact_expiry_transitions_before_status_and_blocks_every_download(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        ready = _wait_for_terminal(client, request_id)
        assert ready["status"] == "ready"
        package = ready["package"]
        assert isinstance(package, dict)
        with backend.create_preview_read_unit_of_work() as uow:
            persisted = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert persisted is not None and persisted.expires_at is not None and persisted.storage_key is not None
        storage_path = settings.preview.artifact_root / persisted.storage_key
        app.state.preview_runtime._clock = lambda: persisted.expires_at

        status = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
        responses = [
            client.get(package["manifest"]["download_url"]),
            client.get(package["files"][0]["download_url"]),
            client.get(package["download_all_url"]),
        ]

        assert status.status_code == 200
        assert status.json()["status"] == "expired"
        assert status.json()["expires_at"] == ready["expires_at"]
        assert status.json()["package"] is None
        assert [(response.status_code, response.json()) for response in responses] == [
            (410, {"detail": f"Preview request '{request_id}' expired at {ready['expires_at']}"}),
        ] * 3
        assert not storage_path.exists()
        with backend.create_preview_read_unit_of_work() as uow:
            expired = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert expired is not None and expired.storage_key is None


def test_expiry_deletion_failure_never_restores_downloadability_and_later_retry_cleans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        ready = _wait_for_terminal(client, request_id)
        with backend.create_preview_read_unit_of_work() as uow:
            persisted = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert persisted is not None and persisted.expires_at is not None and persisted.storage_key is not None
        app.state.preview_runtime._clock = lambda: persisted.expires_at
        store = app.state.preview_artifact_store
        real_delete = store.delete_package
        monkeypatch.setattr(store, "delete_package", lambda **_kwargs: (_ for _ in ()).throw(OSError("busy")))

        first = client.get(ready["package"]["manifest"]["download_url"])
        assert first.status_code == 410
        with backend.create_preview_read_unit_of_work() as uow:
            retained = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert retained is not None and retained.status.value == "expired" and retained.storage_key is not None

        monkeypatch.setattr(store, "delete_package", real_delete)
        second = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
        assert second.status_code == 200 and second.json()["status"] == "expired"
        with backend.create_preview_read_unit_of_work() as uow:
            cleaned = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert cleaned is not None and cleaned.storage_key is None


def test_exact_expired_request_cleanup_retry_is_not_starved_by_more_than_one_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        ready = _wait_for_terminal(client, request_id)
        with backend.create_preview_read_unit_of_work() as uow:
            persisted = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert persisted is not None and persisted.expires_at is not None and persisted.storage_key is not None
        app.state.preview_runtime._clock = lambda: persisted.expires_at
        store = app.state.preview_artifact_store
        real_delete = store.delete_package
        monkeypatch.setattr(store, "delete_package", lambda **_kwargs: (_ for _ in ()).throw(OSError("busy")))
        assert client.get(ready["package"]["manifest"]["download_url"]).status_code == 410
        with backend.create_preview_read_unit_of_work() as uow:
            retained = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert retained is not None and retained.status.value == "expired" and retained.storage_key is not None

        persistence = import_module("core.preview.persistence")
        with Session(backend._engine) as session:
            for index in range(101):
                earlier = replace(
                    retained,
                    request_id=f"000-earlier-{index:03d}",
                    created_at=retained.created_at,
                    storage_key=f"stale-{index:03d}",
                )
                session.add(persistence.request_to_table(earlier))
            session.commit()

        monkeypatch.setattr(store, "delete_package", real_delete)
        retried = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
        assert retried.status_code == 200 and retried.json()["status"] == "expired"
        with backend.create_preview_read_unit_of_work() as uow:
            cleaned = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert cleaned is not None and cleaned.storage_key is None


def test_artifact_failure_logs_only_stable_identifiers_and_exception_type(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        ready = _wait_for_terminal(client, request_id)
        store = app.state.preview_artifact_store
        filesystem_secret = str(settings.preview.artifact_root / "secret-artifact")
        database_secret = settings.tenants["production"].storage.connection_string.get_secret_value()
        exception_detail = "private exception detail"

        def fail_with_chained_secret(*_args: object, **_kwargs: object) -> None:
            try:
                raise ValueError(database_secret)
            except ValueError as cause:
                raise OSError(f"{exception_detail}: {filesystem_secret}") from cause

        cases = [
            ("read_manifest", ready["package"]["manifest"]["download_url"]),
            ("read_file", ready["package"]["files"][0]["download_url"]),
            ("open_archive", ready["package"]["download_all_url"]),
        ]
        for method_name, url in cases:
            caplog.clear()
            with (
                caplog.at_level(logging.ERROR, logger="core.preview.service"),
                patch.object(store, method_name, side_effect=fail_with_chained_secret),
            ):
                response = client.get(url)
            assert response.status_code == 500
            records = [record for record in caplog.records if request_id in record.getMessage()]
            assert len(records) == 1
            message = records[0].getMessage()
            assert "production" in message
            assert "OSError" in message
            assert records[0].exc_info is None
            assert filesystem_secret not in caplog.text
            assert database_secret not in caplog.text
            assert exception_detail not in caplog.text
            assert "Traceback" not in caplog.text
            assert "The above exception was the direct cause" not in caplog.text


@pytest.mark.parametrize(
    "failure_point",
    ["transition", "expired-key-lookup", "key-clear", "transition-commit", "key-clear-commit"],
)
def test_expiry_database_failures_are_recovery_unavailable_and_log_no_sensitive_exception_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure_point: str,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        ready = _wait_for_terminal(client, request_id)
        with backend.create_preview_read_unit_of_work() as uow:
            persisted = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert persisted is not None and persisted.expires_at is not None
        app.state.preview_runtime._clock = lambda: persisted.expires_at
        store = app.state.preview_artifact_store

        if failure_point in {"expired-key-lookup", "key-clear", "key-clear-commit"}:
            real_delete = store.delete_package
            monkeypatch.setattr(
                store,
                "delete_package",
                lambda **_kwargs: (_ for _ in ()).throw(OSError("prepare retained expiry")),
            )
            assert client.get(ready["package"]["manifest"]["download_url"]).status_code == 410
            monkeypatch.setattr(store, "delete_package", real_delete)

        persistence = import_module("core.preview.persistence")
        unit_of_work = import_module("core.storage.backends.sqlmodel.unit_of_work")
        database_secret = settings.tenants["production"].storage.connection_string.get_secret_value()
        path_secret = str(tmp_path / "private-database-path")
        exception_detail = "private expiry database exception"

        def raise_chained_secret(*_args: object, **_kwargs: object) -> None:
            try:
                raise ValueError(database_secret)
            except ValueError as cause:
                raise OSError(f"{exception_detail}: {path_secret}") from cause

        if failure_point == "transition":
            monkeypatch.setattr(
                persistence.SQLModelPreviewRequestRepository,
                "expire_ready_request",
                raise_chained_secret,
            )
        elif failure_point == "expired-key-lookup":
            monkeypatch.setattr(
                persistence.SQLModelPreviewRequestRepository,
                "expire_ready_request",
                lambda *_args, **_kwargs: None,
            )
            monkeypatch.setattr(persistence.SQLModelPreviewRequestRepository, "get_for_owner", raise_chained_secret)
        elif failure_point == "key-clear":
            monkeypatch.setattr(
                persistence.SQLModelPreviewRequestRepository,
                "clear_expired_storage_key",
                raise_chained_secret,
            )
        else:
            real_commit = unit_of_work.PreviewWriteSQLModelUnitOfWork.commit
            commits = 0

            def fail_selected_commit(uow: object) -> None:
                nonlocal commits
                commits += 1
                selected = 1 if failure_point == "transition-commit" else 2
                if commits == selected:
                    raise_chained_secret()
                real_commit(uow)

            monkeypatch.setattr(unit_of_work.PreviewWriteSQLModelUnitOfWork, "commit", fail_selected_commit)

        caplog.clear()
        with caplog.at_level(logging.ERROR, logger="core.preview.service"):
            response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")

        assert response.status_code == 503
        assert response.json() == {"detail": "FOCUS Mapping Preview recovery is unavailable"}
        records = [record for record in caplog.records if request_id in record.getMessage()]
        assert len(records) == 1
        message = records[0].getMessage()
        assert "ecosystem=confluent_cloud" in message
        assert "tenant_id=tenant-1" in message
        assert "error_type=OSError" in message
        assert records[0].exc_info is None
        assert database_secret not in caplog.text
        assert path_secret not in caplog.text
        assert exception_detail not in caplog.text
        assert "Traceback" not in caplog.text
        assert "The above exception was the direct cause" not in caplog.text


def test_archive_rejects_manifest_declarations_that_drift_from_persisted_metadata(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        ready = _wait_for_terminal(client, request_id)
        with backend._engine.begin() as connection:
            data_files = json.loads(
                connection.execute(
                    text("SELECT data_files_json FROM preview_requests WHERE request_id = :request_id"),
                    {"request_id": request_id},
                ).scalar_one()
            )
            data_files[0]["media_type"] = "application/octet-stream"
            connection.execute(
                text("UPDATE preview_requests SET data_files_json = :data_files_json WHERE request_id = :request_id"),
                {
                    "request_id": request_id,
                    "data_files_json": json.dumps(data_files, sort_keys=True, separators=(",", ":")),
                },
            )

        response = client.get(ready["package"]["download_all_url"])

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored preview artifact is unavailable"}


def test_cli_downloads_exact_bytes_served_by_same_production_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    cli = import_module("core.preview.cli")
    cli_client = SameThreadCliClient(client)
    output_dir = tmp_path / "cli-output"

    monkeypatch.setattr(cli.httpx, "Client", lambda **_kwargs: cli_client)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        exit_code = cli.main(
            [
                "daily-full",
                "--api-url",
                "http://testserver/api/v1",
                "--tenant",
                "production",
                "--start-date",
                "2026-07-01",
                "--end-date",
                "2026-07-02",
                "--output-dir",
                str(output_dir),
            ]
        )

        assert exit_code == 0
        assert cli_client.submitted_request_id is not None
        status = client.get(f"/api/v1/tenants/production/focus-preview/requests/{cli_client.submitted_request_id}")
        assert status.status_code == 200
        package = status.json()["package"]
        artifacts = [package["manifest"], *package["files"]]
        api_bytes = {artifact["name"]: client.get(artifact["download_url"]).content for artifact in artifacts}

    assert {path.name for path in output_dir.iterdir()} == set(api_bytes)
    for name, stored_bytes in api_bytes.items():
        assert (output_dir / name).read_bytes() == stored_bytes


def test_api_observes_running_between_queued_and_ready(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    service = import_module("core.preview.service")
    artifacts = import_module("core.preview.artifacts")
    generator = import_module("core.preview.generator")
    executor = BlockingExecutor()
    package_generator = generator.PreviewPackageGenerator(
        max_csv_file_bytes=None,
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
    )
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "running-artifacts"),
        max_workers=1,
        executor=executor,
        request_id_factory=lambda: "request-running",
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
        package_generator=package_generator,
    )
    entered = Event()
    release = Event()
    original_generate = package_generator.generate

    def blocked_generate(*args: object, **kwargs: object) -> object:
        entered.set()
        assert release.wait(5)
        return original_generate(*args, **kwargs)

    app, client = _client(settings)
    worker: Thread | None = None
    try:
        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
            app.state.preview_runtime.close()
            app.state.preview_runtime = runtime
            app.state.backends["production"] = backend
            with patch.object(package_generator, "generate", side_effect=blocked_generate):
                queued = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
                assert queued.status_code == 202
                assert queued.json()["status"] == "queued"
                worker = Thread(target=executor.run)
                worker.start()
                assert entered.wait(5)

                running = client.get("/api/v1/tenants/production/focus-preview/requests/request-running")
                assert running.status_code == 200
                assert running.json()["status"] == "running"
                assert running.json()["started_at"] == "2026-07-04T00:00:00Z"
                assert running.json()["source_snapshot"] is None
                assert running.json()["package"] is None
                for suffix in ("/manifest", "/files/cost-and-usage.csv", "/archive"):
                    blocked_download = client.get(
                        f"/api/v1/tenants/production/focus-preview/requests/request-running{suffix}"
                    )
                    assert blocked_download.status_code == 409
                    assert blocked_download.json() == {
                        "detail": "Preview request 'request-running' is not ready (status: running)"
                    }

                release.set()
                worker.join(5)
                ready = client.get("/api/v1/tenants/production/focus-preview/requests/request-running")
                assert ready.json()["status"] == "ready"
    finally:
        release.set()
        if worker is not None:
            worker.join(5)
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize("suffix", ["/manifest", "/files/cost-and-usage.csv", "/archive"])
def test_failed_request_downloads_return_exact_409(tmp_path: Path, suffix: str) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(),
        aggregate=_aggregate(),
        allocation=_allocation(),
        state=None,
    )
    with backend.create_unit_of_work() as uow:
        state = uow.pipeline_state.get("confluent_cloud", "tenant-1", date(2026, 7, 1))
        assert state is not None
        state.chargeback_calculated = False
        state.calculation_id = None
        state.calculation_completed_at = None
        uow.pipeline_state.upsert(state)
        uow.commit()
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        terminal = _wait_for_terminal(client, request_id)
        assert terminal["status"] == "failed"
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}{suffix}")

    assert response.status_code == 409
    assert response.json() == {"detail": f"Preview request '{request_id}' failed; inspect diagnostics"}


@pytest.mark.parametrize(
    ("suffix", "method_name"),
    [("/manifest", "read_manifest_bytes"), ("/files/cost-and-usage.csv", "read_file_bytes")],
)
def test_ready_missing_artifact_bytes_return_exact_redacted_500(
    tmp_path: Path,
    suffix: str,
    method_name: str,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        assert _wait_for_terminal(client, request_id)["status"] == "ready"
        with patch.object(app.state.preview_runtime, method_name, side_effect=OSError("filesystem sentinel")):
            response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}{suffix}")

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored preview artifact is unavailable"}
    assert "sentinel" not in response.text


def test_ready_archive_creation_failure_returns_exact_redacted_500(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        assert _wait_for_terminal(client, request_id)["status"] == "ready"
        with patch.object(app.state.preview_runtime, "open_archive", side_effect=OSError("spool sentinel")):
            response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}/archive")

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored preview artifact is unavailable"}
    assert "sentinel" not in response.text


@pytest.mark.parametrize("outcome", ["complete", "failure", "cancel", "never-started"])
def test_archive_endpoint_closes_owned_spool_on_completion_failure_and_cancellation(
    tmp_path: Path,
    outcome: str,
) -> None:
    route = import_module("core.api.routes.focus_preview")
    models = import_module("core.preview.models")

    class RecordingArchive:
        close_calls = 0

        def iter_chunks(self) -> Iterator[bytes]:
            yield b"first"
            if outcome == "failure":
                raise RuntimeError("stream failed")
            yield b"second"

        def close(self) -> None:
            self.close_calls += 1

    archive = RecordingArchive()
    runtime = SimpleNamespace(open_archive=lambda _preview: archive)
    preview = SimpleNamespace(
        request_id="request-ready",
        status=models.PreviewRequestStatus.READY,
        expires_at=None,
    )
    request = SimpleNamespace()
    tenant_config = _settings(tmp_path).tenants["production"]

    async def consume() -> bytes:
        with patch.object(route, "_lookup", return_value=(runtime, preview)):
            response = route.get_archive(
                request,
                "production",
                "request-ready",
                tenant_config,
            )
        if outcome == "never-started":
            assert response.background is not None
            await response.background()
            return b""
        iterator = response.body_iterator.__aiter__()
        if outcome == "cancel":
            first = await anext(iterator)
            await iterator.aclose()
            return first
        try:
            body = b"".join([chunk async for chunk in iterator])
        finally:
            if response.background is not None:
                await response.background()
        return body

    if outcome == "failure":
        with pytest.raises(RuntimeError, match="stream failed"):
            asyncio.run(consume())
    else:
        expected = b"" if outcome == "never-started" else b"first" if outcome == "cancel" else b"firstsecond"
        assert asyncio.run(consume()) == expected
    assert archive.close_calls >= 1


def test_real_api_redacts_corrupt_stored_manifest_and_csv_across_retrieval_routes(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        ready = _wait_for_terminal(client, request_id)
        with backend.create_preview_read_unit_of_work() as uow:
            stored = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
        assert stored is not None and stored.storage_key is not None and stored.package is not None
        package_dir = settings.preview.artifact_root / stored.storage_key
        manifest_path = package_dir / stored.package.manifest.name
        file_path = package_dir / stored.package.files[0].name
        manifest_bytes = manifest_path.read_bytes()
        file_bytes = file_path.read_bytes()
        secret = f"private bytes {package_dir}".encode()
        urls = {
            "manifest": ready["package"]["manifest"]["download_url"],
            "file": ready["package"]["files"][0]["download_url"],
            "archive": ready["package"]["download_all_url"],
        }

        manifest_path.write_bytes(secret)
        for url in urls.values():
            response = client.get(url)
            assert response.status_code == 500
            assert response.json() == {"detail": "Stored preview artifact is unavailable"}
            assert secret not in response.content
            assert str(package_dir) not in response.text

        manifest_path.write_bytes(manifest_bytes)
        file_path.write_bytes(secret)
        manifest_response = client.get(urls["manifest"])
        assert manifest_response.status_code == 200
        assert manifest_response.content == manifest_bytes
        for endpoint in ("file", "archive"):
            response = client.get(urls[endpoint])
            assert response.status_code == 500
            assert response.json() == {"detail": "Stored preview artifact is unavailable"}
            assert secret not in response.content
            assert str(package_dir) not in response.text
        file_path.write_bytes(file_bytes)


@pytest.mark.parametrize(
    ("suffix", "file_name"),
    [("/manifest", "manifest.json"), ("/files/cost-and-usage.csv", "cost-and-usage.csv")],
)
def test_real_finalized_artifact_deletion_returns_redacted_500(
    tmp_path: Path,
    suffix: str,
    file_name: str,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        request_id = submitted.json()["request_id"]
        assert _wait_for_terminal(client, request_id)["status"] == "ready"
        stored = app.state.preview_runtime.get_request(
            backend=backend,
            request_id=request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert stored is not None and stored.storage_key is not None
        artifact_path = settings.preview.artifact_root / stored.storage_key / file_name
        artifact_path.unlink()

        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}{suffix}")

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored preview artifact is unavailable"}
    assert str(artifact_path) not in response.text


@pytest.mark.parametrize(
    ("status", "suffix", "expected_detail"),
    [
        ("queued", "/files/not-enumerated.csv", "Preview request 'request-1' is not ready (status: queued)"),
        ("queued", "/manifest", "Preview request 'request-1' is not ready (status: queued)"),
    ],
)
def test_non_ready_status_precedes_file_membership(
    tmp_path: Path,
    status: str,
    suffix: str,
    expected_detail: str,
) -> None:
    del status
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    service = import_module("core.preview.service")
    artifacts = import_module("core.preview.artifacts")
    from tests.unit.core.preview.test_service import ControlledExecutor

    executor = ControlledExecutor()
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts-controlled"),
        max_workers=1,
        executor=executor,
        request_id_factory=lambda: "request-1",
    )
    app, client = _client(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), client:
        app.state.preview_runtime.close()
        app.state.preview_runtime = runtime
        app.state.backends["production"] = backend
        response = client.post(
            "/api/v1/tenants/production/focus-preview/requests",
            json={
                "grain": "daily",
                "start_date": str(date(2026, 7, 1)),
                "end_date": str(date(2026, 7, 2)),
                "column_profile": "full",
            },
        )
        assert response.status_code == 202
        download = client.get(f"/api/v1/tenants/production/focus-preview/requests/request-1{suffix}")

    assert download.status_code == 409
    assert download.json() == {"detail": expected_detail}

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal
from unittest.mock import patch
from uuid import UUID

import pytest
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import create_engine, text

from core.api.app import create_app
from core.config.models import (
    AppSettings,
    FocusPreviewTenantConfig,
    StorageConfig,
    TenantConfig,
)
from core.preview.storage_availability import (
    PreviewEvidenceAvailability,
    PreviewEvidenceAvailabilityState,
)
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider

POST_PATH = "/api/v1/tenants/{tenant_name}/focus-preview/repairs"
GET_PATH = "/api/v1/tenants/{tenant_name}/focus-preview/repairs/{repair_id}"


def _tenant(
    tmp_path: Path,
    *,
    ecosystem: str = "confluent_cloud",
    enabled: bool = True,
    effective_end: date | None = date(2026, 12, 31),
) -> TenantConfig:
    return TenantConfig(
        ecosystem=ecosystem,
        tenant_id=f"{ecosystem}-tenant",
        lookback_days=200,
        cutoff_days=5,
        retention_days=250,
        storage=StorageConfig(
            connection_string=f"sqlite:///{tmp_path / f'{ecosystem}-{enabled}.db'}",
        ),
        focus_preview=(
            FocusPreviewTenantConfig(
                commercial_profile="direct_payg",
                effective_start_date=date(2026, 1, 1),
                effective_end_date=effective_end,
            )
            if enabled
            else None
        ),
    )


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings(
        tenants={
            "enabled": _tenant(tmp_path),
            "disabled": _tenant(tmp_path, enabled=False),
            "unsupported": _tenant(
                tmp_path,
                ecosystem="unsupported",
                enabled=False,
            ),
        }
    )


def test_repair_request_body_forbids_extra_fields() -> None:
    from core.api.schemas import FocusPreviewRepairRequestBody

    with pytest.raises(ValidationError):
        FocusPreviewRepairRequestBody.model_validate(
            {
                "start_date": "2026-07-01",
                "end_date": "2026-07-02",
                "tenant_id": "scope-expansion",
            }
        )


def test_repair_response_schemas_include_exact_closed_status_values() -> None:
    from core.api.schemas import (
        FocusPreviewRepairDateResponse,
        FocusPreviewRepairResponse,
    )

    schemas = {
        "operation": FocusPreviewRepairResponse.model_json_schema(),
        "date": FocusPreviewRepairDateResponse.model_json_schema(),
    }

    assert schemas["operation"]["properties"]["status"]["enum"] == [
        "queued",
        "running",
        "completed",
        "completed_with_failures",
        "failed",
    ]
    assert schemas["date"]["properties"]["status"]["enum"] == [
        "queued",
        "running",
        "daily_validated",
        "succeeded",
        "failed",
    ]


def test_api_and_both_modes_publish_post_and_owner_scoped_get_while_worker_does_not(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)

    for mode in ("api", "both"):
        app = create_app(settings, mode=mode)
        routes = {(route.path, frozenset(route.methods or ())) for route in app.routes if isinstance(route, APIRoute)}
        assert (POST_PATH, frozenset({"POST"})) in routes
        assert (GET_PATH, frozenset({"GET"})) in routes
        assert POST_PATH in app.openapi()["paths"]
        assert GET_PATH in app.openapi()["paths"]
    worker = create_app(settings, mode="worker")
    worker_paths = {route.path for route in worker.routes if isinstance(route, APIRoute)}
    assert POST_PATH not in worker_paths
    assert GET_PATH not in worker_paths


def test_framework_422_precedes_tenant_runtime_and_backend_checks(tmp_path: Path) -> None:
    app = create_app(_settings(tmp_path), mode="api")

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/tenants/missing/focus-preview/repairs",
            json={"start_date": "not-a-date", "end_date": "2026-07-02"},
        )

    assert response.status_code == 422


def test_unknown_unsupported_and_disabled_tenants_precede_runtime_backend_checks(
    tmp_path: Path,
) -> None:
    app = create_app(_settings(tmp_path), mode="api")
    body = {"start_date": "2026-07-01", "end_date": "2026-07-02"}

    with TestClient(app) as client:
        missing = client.post(
            "/api/v1/tenants/missing/focus-preview/repairs",
            json=body,
        )
        unsupported = client.post(
            "/api/v1/tenants/unsupported/focus-preview/repairs",
            json=body,
        )
        disabled = client.post(
            "/api/v1/tenants/disabled/focus-preview/repairs",
            json=body,
        )

    assert missing.status_code == 404
    assert missing.json() == {"detail": "Tenant 'missing' not found"}
    assert unsupported.status_code == 400
    assert unsupported.json() == {"detail": "FOCUS Mapping Preview currently supports only Confluent Cloud tenants"}
    assert disabled.status_code == 409
    assert disabled.json() == {
        "detail": {
            "code": "preview_commercial_profile_unavailable",
            "message": ("An explicit Direct-billed PAYG profile does not cover the requested interval."),
            "retryable": False,
        }
    }


def test_api_only_valid_post_fails_before_backend_acquisition_when_worker_is_absent(
    tmp_path: Path,
) -> None:
    settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'missing-repair-runtime.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])
    app = create_app(
        settings,
        workflow_runner=runner,  # type: ignore[arg-type]
        mode="api",
    )

    try:
        with TestClient(app) as client:
            runner.clear_lease_observations()
            response = client.post(
                "/api/v1/tenants/enabled/focus-preview/repairs",
                json={"start_date": "2026-07-01", "end_date": "2026-07-02"},
            )

        assert response.status_code == 503
        assert response.json() == {"detail": "FOCUS Mapping Preview repair worker is unavailable"}
        assert runner.acquisitions == []
        assert runner.lease_events == []
    finally:
        backend.dispose()


@pytest.mark.parametrize(
    ("body", "detail"),
    [
        (
            {"start_date": "2026-07-02", "end_date": "2026-07-02"},
            {
                "code": "focus_preview_repair_range_invalid",
                "message": (
                    "FOCUS Mapping Preview repair requires an inclusive start date before the exclusive end date."
                ),
                "retryable": False,
            },
        ),
        (
            {"start_date": "2027-01-01", "end_date": "2027-01-02"},
            {
                "code": "focus_preview_repair_future_range",
                "message": "FOCUS Mapping Preview repair cannot include future UTC dates.",
                "retryable": False,
            },
        ),
        (
            {"start_date": "2025-01-01", "end_date": "2025-01-02"},
            {
                "code": "focus_preview_repair_range_ineligible",
                "message": (
                    "The requested repair range is outside the tenant's complete "
                    "Preview eligibility and retained-data interval."
                ),
                "retryable": False,
            },
        ),
    ],
)
def test_exact_range_error_bodies_precede_runtime_and_backend(
    tmp_path: Path,
    body: dict[str, str],
    detail: dict[str, object],
) -> None:
    app = create_app(_settings(tmp_path), mode="api")

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/tenants/enabled/focus-preview/repairs",
            json=body,
        )

    assert response.status_code == 400
    assert response.json() == {"detail": detail}


class _ProductionBackendLease:
    def __init__(
        self,
        runner: _ProductionRunnerDouble,
        tenant_name: str,
        backend: object,
        *,
        enter_error: BaseException | None,
        exit_error: BaseException | None,
    ) -> None:
        self.runner = runner
        self.tenant_name = tenant_name
        self.backend = backend
        self.enter_error = enter_error
        self.exit_error = exit_error

    def __enter__(self) -> object:
        self.runner.lease_events.append(("enter", self.tenant_name))
        if self.enter_error is not None:
            raise self.enter_error
        return self.backend

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> Literal[False]:
        del exc_type, traceback
        self.runner.lease_events.append(("exit", self.tenant_name))
        self.runner.exit_exceptions.append(exc_value)
        if self.exit_error is not None:
            raise self.exit_error
        return False


class _ProductionRunnerDouble:
    """Complete shape consumed by the application lifespan for this contract."""

    def __init__(self, backend: SQLModelBackend, events: list[str]) -> None:
        self.backend = backend
        self.events = events
        self.busy = False
        self.preview_generation_scheduler = None
        self.acquisitions: list[str] = []
        self.lease_events: list[tuple[str, str]] = []
        self.exit_exceptions: list[BaseException | None] = []
        self.next_backend: object | None = None
        self.next_enter_error: BaseException | None = None
        self.next_exit_error: BaseException | None = None

    def acquire_backend(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> _ProductionBackendLease:
        del tenant_config
        self.acquisitions.append(tenant_name)
        backend = self.backend if self.next_backend is None else self.next_backend
        lease = _ProductionBackendLease(
            self,
            tenant_name,
            backend,
            enter_error=self.next_enter_error,
            exit_error=self.next_exit_error,
        )
        self.next_backend = None
        self.next_enter_error = None
        self.next_exit_error = None
        return lease

    def clear_lease_observations(self) -> None:
        self.acquisitions.clear()
        self.lease_events.clear()
        self.exit_exceptions.clear()

    def run_focus_preview_repair(
        self,
        repair_id: str,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> None:
        del repair_id, tenant_name, tenant_config

    def is_tenant_running(self, tenant_name: str) -> bool:
        del tenant_name
        return self.busy

    def close(self) -> None:
        self.events.append("provider-close")

    def drain(self, timeout: float) -> None:
        del timeout
        self.events.append("runner-drain")


class _UnavailableEvidenceBackend:
    """Full repair-evidence protocol shape with controlled availability."""

    def __init__(self, backend: SQLModelBackend) -> None:
        self.backend = backend

    @property
    def preview_evidence_availability(self) -> PreviewEvidenceAvailability:
        return PreviewEvidenceAvailability(PreviewEvidenceAvailabilityState.UNAVAILABLE)

    def create_preview_evidence_unit_of_work(self) -> Any:
        return self.backend.create_preview_evidence_unit_of_work()

    def create_preview_generation_read_unit_of_work(self) -> Any:
        return self.backend.create_preview_generation_read_unit_of_work()

    def create_preview_evidence_bootstrap(self) -> Any:
        return self.backend.create_preview_evidence_bootstrap()

    def mark_preview_evidence_bootstrap_unavailable(self, error_type: str) -> None:
        self.backend.mark_preview_evidence_bootstrap_unavailable(error_type)


class _UnreadableEvidenceBackend(_UnavailableEvidenceBackend):
    def __init__(self, backend: SQLModelBackend, error: BaseException) -> None:
        super().__init__(backend)
        self.error = error

    @property
    def preview_evidence_availability(self) -> PreviewEvidenceAvailability:
        raise self.error


def _assert_global_500(
    response: Any,
    log_exception: Any,
    expected_error: BaseException,
) -> None:
    assert response.status_code == 500
    body = response.json()
    assert body["detail"] == "Internal server error"
    assert set(body) == {"detail", "error_id"}
    assert str(UUID(body["error_id"])) == body["error_id"]
    assert log_exception.call_args.kwargs["exc_info"] is expected_error


def _valid_repair_body() -> dict[str, str]:
    return {"start_date": "2026-07-01", "end_date": "2026-07-02"}


@pytest.fixture
def production_repair_app(
    tmp_path: Path,
) -> Iterator[tuple[FastAPI, _ProductionRunnerDouble]]:
    settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'repair-contract.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])
    yield (
        create_app(
            settings,
            workflow_runner=runner,  # type: ignore[arg-type]
            mode="both",
        ),
        runner,
    )
    backend.dispose()


def test_both_mode_production_lifespan_constructs_repair_runtime_and_closes_it_before_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.preview import repair

    settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'production-runtime.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    events: list[str] = []
    runner = _ProductionRunnerDouble(backend, events)
    original_close = repair.PreviewRepairRuntime.close

    def recording_close(self: object, *, wait: bool) -> None:
        events.append(f"repair-close-{wait}")
        original_close(self, wait=wait)

    monkeypatch.setattr(repair.PreviewRepairRuntime, "close", recording_close)
    app = create_app(settings, workflow_runner=runner, mode="both")  # type: ignore[arg-type]

    with TestClient(app):
        assert app.state.preview_repair_runtime is not None

    assert events.index("repair-close-True") < events.index("runner-drain")
    backend.dispose()


class _ControlledRepairExecutor:
    def __init__(self, *, fail_submit: bool = False) -> None:
        self.fail_submit = fail_submit
        self.pending: list[Any] = []

    def submit(self, fn: Any) -> object:
        if self.fail_submit:
            raise RuntimeError("controlled scheduling failure")
        self.pending.append(fn)
        return object()

    def shutdown(self, wait: bool = True) -> None:
        del wait


class _RecordingTenantBackendProvider:
    def __init__(self, backend: SQLModelBackend) -> None:
        self.backend = backend
        self.received_exception: BaseException | None = None

    @contextmanager
    def acquire_backend(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> Iterator[SQLModelBackend]:
        del tenant_name, tenant_config
        try:
            yield self.backend
        except BaseException as exc:
            self.received_exception = exc
            raise

    def close(self) -> None:
        return None


def _direct_repair_runtime(
    *,
    backend: SQLModelBackend,
    runner: _ProductionRunnerDouble,
    executor: _ControlledRepairExecutor,
):
    from core.preview.repair import PreviewRepairRuntime

    tenant = _tenant(Path("/tmp"))
    return PreviewRepairRuntime(
        runner=runner,
        backend_provider=FixedTenantBackendProvider({"enabled": backend}),
        max_workers=1,
        configured_owners=(("enabled", tenant),),
        executor=executor,
    )


def _direct_request(runtime: object, runner: object) -> Request:
    app = SimpleNamespace(
        state=SimpleNamespace(
            preview_repair_runtime=runtime,
            workflow_runner=runner,
        )
    )
    return Request({"type": "http", "app": app})


def _raise_on_submit(
    monkeypatch: pytest.MonkeyPatch,
    runtime: object,
    error: BaseException,
) -> None:
    def raise_error(**_kwargs: object) -> object:
        raise error

    monkeypatch.setattr(runtime, "submit", raise_error)


def test_direct_api_busy_precedes_queue_creation_with_exact_body(tmp_path: Path) -> None:
    from core.api.routes.focus_preview import submit_repair
    from core.api.schemas import FocusPreviewRepairRequestBody

    settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'busy.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])
    runner.busy = True
    executor = _ControlledRepairExecutor()
    runtime = _direct_repair_runtime(backend=backend, runner=runner, executor=executor)
    provider = FixedTenantBackendProvider({"enabled": backend})
    try:
        with pytest.raises(HTTPException) as raised:
            submit_repair(
                _direct_request(runtime, runner),
                Response(),
                "enabled",
                FocusPreviewRepairRequestBody(
                    start_date=date(2026, 7, 1),
                    end_date=date(2026, 7, 2),
                ),
                settings,
                provider,
            )
        assert raised.value.status_code == 409
        assert raised.value.detail == {
            "code": "focus_preview_repair_tenant_busy",
            "message": "The tenant pipeline is busy; wait for it to finish and retry the repair.",
            "retryable": True,
        }
        with backend.create_preview_generation_read_unit_of_work() as uow:
            assert (
                uow.repairs.find_active_for_owner(
                    "confluent_cloud",
                    "confluent_cloud-tenant",
                )
                is None
            )
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_direct_api_dedicated_active_error_message_does_not_change_conflict_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.api.routes.focus_preview import submit_repair
    from core.api.schemas import FocusPreviewRepairRequestBody
    from core.preview import repair

    settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'altered-active-message.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])
    runtime = _direct_repair_runtime(
        backend=backend,
        runner=runner,
        executor=_ControlledRepairExecutor(),
    )
    _raise_on_submit(
        monkeypatch,
        runtime,
        repair.PreviewRepairAlreadyActiveError("changed diagnostic wording"),
    )
    provider = _RecordingTenantBackendProvider(backend)
    try:
        with pytest.raises(HTTPException) as raised:
            submit_repair(
                _direct_request(runtime, runner),
                Response(),
                "enabled",
                FocusPreviewRepairRequestBody(
                    start_date=date(2026, 7, 1),
                    end_date=date(2026, 7, 2),
                ),
                settings,
                provider,
            )

        assert raised.value.status_code == 409
        assert raised.value.detail == {
            "code": "focus_preview_repair_in_progress",
            "message": "A FOCUS Mapping Preview repair is already queued or running for this tenant.",
            "retryable": True,
        }
    finally:
        runtime.close(wait=True)
        provider.close()
        backend.dispose()


def test_production_repair_entry_failure_is_exact_storage_503_without_exit(
    production_repair_app: tuple[FastAPI, _ProductionRunnerDouble],
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, runner = production_repair_app
    private_value = "private repair acquisition value"
    sentinel = RuntimeError(private_value)
    with TestClient(app) as client:
        runner.clear_lease_observations()
        runner.next_enter_error = sentinel
        with (
            patch.object(app.state.preview_repair_runtime, "submit") as submit,
            caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"),
        ):
            response = client.post(
                "/api/v1/tenants/enabled/focus-preview/repairs",
                json=_valid_repair_body(),
            )

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview repair storage is unavailable"}
    submit.assert_not_called()
    assert runner.acquisitions == ["enabled"]
    assert runner.lease_events == [("enter", "enabled")]
    assert runner.exit_exceptions == []
    assert ("FOCUS Mapping Preview repair backend failed tenant=enabled error_type=RuntimeError") in caplog.text
    assert private_value not in response.text
    assert private_value not in caplog.text


def test_production_repair_invalid_backend_protocol_is_exact_storage_503_and_releases(
    production_repair_app: tuple[FastAPI, _ProductionRunnerDouble],
) -> None:
    app, runner = production_repair_app
    with TestClient(app) as client:
        runner.clear_lease_observations()
        runner.next_backend = SimpleNamespace(private_value="private invalid protocol")
        with patch.object(app.state.preview_repair_runtime, "submit") as submit:
            response = client.post(
                "/api/v1/tenants/enabled/focus-preview/repairs",
                json=_valid_repair_body(),
            )

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview repair storage is unavailable"}
    submit.assert_not_called()
    assert runner.acquisitions == ["enabled"]
    assert runner.lease_events == [
        ("enter", "enabled"),
        ("exit", "enabled"),
    ]
    assert len(runner.exit_exceptions) == 1
    assert isinstance(runner.exit_exceptions[0], HTTPException)
    assert "private invalid protocol" not in response.text


def test_production_repair_unavailable_evidence_is_exact_storage_503_and_releases(
    production_repair_app: tuple[FastAPI, _ProductionRunnerDouble],
) -> None:
    app, runner = production_repair_app
    with TestClient(app) as client:
        runner.clear_lease_observations()
        runner.next_backend = _UnavailableEvidenceBackend(runner.backend)
        with patch.object(app.state.preview_repair_runtime, "submit") as submit:
            response = client.post(
                "/api/v1/tenants/enabled/focus-preview/repairs",
                json=_valid_repair_body(),
            )

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview repair storage is unavailable"}
    submit.assert_not_called()
    assert runner.acquisitions == ["enabled"]
    assert runner.lease_events == [
        ("enter", "enabled"),
        ("exit", "enabled"),
    ]
    assert len(runner.exit_exceptions) == 1
    assert isinstance(runner.exit_exceptions[0], HTTPException)


def test_production_repair_unreadable_evidence_is_exact_storage_503_and_releases(
    production_repair_app: tuple[FastAPI, _ProductionRunnerDouble],
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, runner = production_repair_app
    private_value = "private repair availability value"
    sentinel = RuntimeError(private_value)
    with TestClient(app) as client:
        runner.clear_lease_observations()
        runner.next_backend = _UnreadableEvidenceBackend(runner.backend, sentinel)
        with (
            patch.object(app.state.preview_repair_runtime, "submit") as submit,
            caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"),
        ):
            response = client.post(
                "/api/v1/tenants/enabled/focus-preview/repairs",
                json=_valid_repair_body(),
            )

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview repair storage is unavailable"}
    submit.assert_not_called()
    assert runner.acquisitions == ["enabled"]
    assert runner.lease_events == [
        ("enter", "enabled"),
        ("exit", "enabled"),
    ]
    assert len(runner.exit_exceptions) == 1
    assert isinstance(runner.exit_exceptions[0], HTTPException)
    assert ("FOCUS Mapping Preview repair backend failed tenant=enabled error_type=RuntimeError") in caplog.text
    assert private_value not in response.text
    assert private_value not in caplog.text


def test_production_repair_intentional_http_exception_keeps_identity_and_releases(
    production_repair_app: tuple[FastAPI, _ProductionRunnerDouble],
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, runner = production_repair_app
    sentinel = HTTPException(418, detail={"code": "intentional_repair_sentinel"})
    with TestClient(app) as client:
        runner.clear_lease_observations()
        with (
            patch.object(
                app.state.preview_repair_runtime,
                "submit",
                side_effect=sentinel,
            ),
            caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"),
        ):
            response = client.post(
                "/api/v1/tenants/enabled/focus-preview/repairs",
                json=_valid_repair_body(),
            )

    assert response.status_code == 418
    assert response.json() == {"detail": {"code": "intentional_repair_sentinel"}}
    assert runner.lease_events == [
        ("enter", "enabled"),
        ("exit", "enabled"),
    ]
    assert runner.exit_exceptions == [sentinel]
    assert "FOCUS Mapping Preview repair backend failed" not in caplog.text


def test_production_repair_unexpected_exception_reaches_global_handler_with_identity(
    production_repair_app: tuple[FastAPI, _ProductionRunnerDouble],
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, runner = production_repair_app
    private_value = "private unexpected repair value"
    sentinel = RuntimeError(private_value)
    with TestClient(app, raise_server_exceptions=False) as client:
        runner.clear_lease_observations()
        with (
            patch.object(
                app.state.preview_repair_runtime,
                "submit",
                side_effect=sentinel,
            ),
            patch("core.api.exception_handler.logger.exception") as log_exception,
            caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"),
        ):
            response = client.post(
                "/api/v1/tenants/enabled/focus-preview/repairs",
                json=_valid_repair_body(),
            )

    _assert_global_500(response, log_exception, sentinel)
    assert runner.lease_events == [
        ("enter", "enabled"),
        ("exit", "enabled"),
    ]
    assert runner.exit_exceptions == [sentinel]
    assert "FOCUS Mapping Preview repair backend failed" not in caplog.text
    assert "repair storage is unavailable" not in caplog.text
    assert private_value not in caplog.text


def test_production_repair_exit_failure_cannot_mask_primary_exception(
    production_repair_app: tuple[FastAPI, _ProductionRunnerDouble],
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, runner = production_repair_app
    primary_private_value = "private primary repair value"
    release_private_value = "private release repair value"
    primary_error = RuntimeError(primary_private_value)
    release_error = OSError(release_private_value)
    with TestClient(app, raise_server_exceptions=False) as client:
        runner.clear_lease_observations()
        runner.next_exit_error = release_error
        with (
            patch.object(
                app.state.preview_repair_runtime,
                "submit",
                side_effect=primary_error,
            ),
            patch("core.api.exception_handler.logger.exception") as log_exception,
            caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"),
        ):
            response = client.post(
                "/api/v1/tenants/enabled/focus-preview/repairs",
                json=_valid_repair_body(),
            )

    _assert_global_500(response, log_exception, primary_error)
    assert runner.lease_events == [
        ("enter", "enabled"),
        ("exit", "enabled"),
    ]
    assert runner.exit_exceptions == [primary_error]
    assert (
        "FOCUS Mapping Preview backend lease release failed "
        "tenant=enabled primary_error_type=RuntimeError "
        "release_error_type=OSError"
    ) in caplog.text
    assert "FOCUS Mapping Preview repair backend failed" not in caplog.text
    assert primary_private_value not in caplog.text
    assert release_private_value not in caplog.text


def test_production_repair_exit_failure_after_success_reaches_global_handler(
    production_repair_app: tuple[FastAPI, _ProductionRunnerDouble],
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, runner = production_repair_app
    private_value = "private successful repair release value"
    release_error = OSError(private_value)
    with TestClient(app, raise_server_exceptions=False) as client:
        runner.clear_lease_observations()
        runner.next_exit_error = release_error
        with (
            patch("core.api.exception_handler.logger.exception") as log_exception,
            caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"),
        ):
            response = client.get("/api/v1/tenants/enabled/focus-preview/repairs/absent")

    _assert_global_500(response, log_exception, release_error)
    assert runner.lease_events == [
        ("enter", "enabled"),
        ("exit", "enabled"),
    ]
    assert runner.exit_exceptions == [None]
    assert "FOCUS Mapping Preview repair backend failed" not in caplog.text
    assert "repair storage is unavailable" not in caplog.text
    assert private_value not in caplog.text


def test_direct_api_scheduling_failure_persists_exact_durable_failure(
    tmp_path: Path,
) -> None:
    from core.api.routes.focus_preview import submit_repair
    from core.api.schemas import FocusPreviewRepairRequestBody

    settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    connection_string = f"sqlite:///{tmp_path / 'schedule-failure.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])
    executor = _ControlledRepairExecutor(fail_submit=True)
    runtime = _direct_repair_runtime(backend=backend, runner=runner, executor=executor)
    provider = FixedTenantBackendProvider({"enabled": backend})
    try:
        with pytest.raises(HTTPException) as raised:
            submit_repair(
                _direct_request(runtime, runner),
                Response(),
                "enabled",
                FocusPreviewRepairRequestBody(
                    start_date=date(2026, 7, 1),
                    end_date=date(2026, 7, 3),
                ),
                settings,
                provider,
            )
        assert raised.value.status_code == 503
        assert raised.value.detail == "FOCUS Mapping Preview repair worker is unavailable"
        with backend.create_preview_generation_read_unit_of_work() as uow:
            persisted = uow.repairs.find_active_for_owner(
                "confluent_cloud",
                "confluent_cloud-tenant",
            )
            assert persisted is None
        engine = create_engine(connection_string)
        try:
            with engine.connect() as connection:
                failed = connection.execute(
                    text(
                        """
                        SELECT status, diagnostic_code
                        FROM ccloud_focus_preview_repairs
                        """
                    )
                ).one()
        finally:
            engine.dispose()
        assert failed == ("failed", "focus_preview_repair_worker_unavailable")
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_direct_api_storage_unavailable_and_foreign_absent_get_are_exact(
    tmp_path: Path,
) -> None:
    from core.api.routes.focus_preview import get_repair, submit_repair
    from core.api.schemas import FocusPreviewRepairRequestBody

    settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'owner-status.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])
    executor = _ControlledRepairExecutor()
    runtime = _direct_repair_runtime(backend=backend, runner=runner, executor=executor)
    try:
        with pytest.raises(HTTPException) as unavailable:
            submit_repair(
                _direct_request(runtime, runner),
                Response(),
                "enabled",
                FocusPreviewRepairRequestBody(
                    start_date=date(2026, 7, 1),
                    end_date=date(2026, 7, 2),
                ),
                settings,
                FixedTenantBackendProvider({"enabled": object()}),  # type: ignore[dict-item]
            )
        assert unavailable.value.status_code == 503
        assert unavailable.value.detail == "FOCUS Mapping Preview repair storage is unavailable"

        queued = runtime.create_queued(
            backend=backend,
            tenant_name="enabled",
            tenant_config=settings.tenants["enabled"],
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            created_at=datetime(2026, 7, 23, tzinfo=UTC),
        )
        empty_backend = SQLModelBackend(
            f"sqlite:///{tmp_path / 'empty-owner-status.db'}",
            CCloudStorageModule(),
            use_migrations=False,
            focus_preview_enabled=True,
        )
        empty_backend.create_tables()
        with pytest.raises(HTTPException) as absent:
            get_repair(
                "enabled",
                queued.repair_id,
                settings,
                FixedTenantBackendProvider({"enabled": empty_backend}),
            )
        absent_detail = absent.value.detail
        empty_backend.dispose()

        foreign_tenant = settings.tenants["enabled"].model_copy(update={"tenant_id": "foreign-tenant"})
        foreign_settings = AppSettings(tenants={"foreign": foreign_tenant})
        foreign_provider = FixedTenantBackendProvider({"foreign": backend})
        with pytest.raises(HTTPException) as foreign:
            get_repair(
                "foreign",
                queued.repair_id,
                foreign_settings,
                foreign_provider,
            )
        assert absent.value.status_code == foreign.value.status_code == 404
        assert absent_detail == foreign.value.detail == (f"FOCUS Mapping Preview repair {queued.repair_id!r} not found")
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_direct_api_get_serializes_daily_validated_as_nonterminal(
    tmp_path: Path,
) -> None:
    from core.api.routes.focus_preview import get_repair

    settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'daily-validated.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])
    runtime = _direct_repair_runtime(
        backend=backend,
        runner=runner,
        executor=_ControlledRepairExecutor(),
    )
    provider = FixedTenantBackendProvider({"enabled": backend})
    try:
        queued = runtime.create_queued(
            backend=backend,
            tenant_name="enabled",
            tenant_config=settings.tenants["enabled"],
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            created_at=datetime(2026, 7, 23, tzinfo=UTC),
        )
        with backend.create_preview_evidence_unit_of_work() as uow:
            uow.repairs.mark_running(
                queued.repair_id,
                started_at=datetime(2026, 7, 23, 0, 0, 1, tzinfo=UTC),
            )
            uow.repairs.mark_date_running(
                queued.repair_id,
                date(2026, 7, 1),
                started_at=datetime(2026, 7, 23, 0, 0, 2, tzinfo=UTC),
            )
            uow.repairs.mark_date_daily_validated(
                queued.repair_id,
                date(2026, 7, 1),
                calculation_id="calculation-1",
                calculation_completed_at=datetime(
                    2026,
                    7,
                    23,
                    0,
                    0,
                    3,
                    tzinfo=UTC,
                ),
                rows_written=0,
            )
            uow.commit()

        response = get_repair(
            "enabled",
            queued.repair_id,
            settings,
            provider,
        )

        assert response.status == "running"
        assert response.dates[0].status == "daily_validated"
        assert response.dates[0].completed_at is None
        assert response.dates[0].calculation_id == "calculation-1"
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_api_only_and_disabled_production_lifespans_do_not_construct_repair_runtime(
    tmp_path: Path,
) -> None:
    enabled_settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    disabled_settings = AppSettings(tenants={"disabled": _tenant(tmp_path, enabled=False)})
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'no-production-runtime.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])

    api_app = create_app(enabled_settings, workflow_runner=runner, mode="api")  # type: ignore[arg-type]
    with TestClient(api_app):
        assert api_app.state.preview_repair_runtime is None
    disabled_app = create_app(disabled_settings, workflow_runner=runner, mode="both")  # type: ignore[arg-type]
    with TestClient(disabled_app):
        assert disabled_app.state.preview_repair_runtime is None

    backend.dispose()


def test_production_post_persists_complete_queue_location_active_guard_and_restart_read(
    tmp_path: Path,
) -> None:
    settings = AppSettings(tenants={"enabled": _tenant(tmp_path)})
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'durable-production-repair.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])
    app = create_app(settings, workflow_runner=runner, mode="both")  # type: ignore[arg-type]

    with TestClient(app) as client:
        runner.clear_lease_observations()
        response = client.post(
            "/api/v1/tenants/enabled/focus-preview/repairs",
            json={"start_date": "2026-07-01", "end_date": "2026-07-04"},
        )
        assert response.status_code == 202
        payload = response.json()
        repair_id = payload["repair_id"]
        assert response.headers["location"] == (f"/api/v1/tenants/enabled/focus-preview/repairs/{repair_id}")
        assert payload["status"] == "queued"
        assert [(item["tracking_date"], item["status"]) for item in payload["dates"]] == [
            ("2026-07-01", "queued"),
            ("2026-07-02", "queued"),
            ("2026-07-03", "queued"),
        ]
        duplicate = client.post(
            "/api/v1/tenants/enabled/focus-preview/repairs",
            json={"start_date": "2026-07-01", "end_date": "2026-07-04"},
        )
        assert duplicate.status_code == 409
        assert duplicate.json() == {
            "detail": {
                "code": "focus_preview_repair_in_progress",
                "message": ("A FOCUS Mapping Preview repair is already queued or running for this tenant."),
                "retryable": True,
            }
        }
        retained = client.get(
            f"/api/v1/tenants/enabled/focus-preview/repairs/{repair_id}",
        )
        assert retained.status_code == 200
        assert retained.json()["repair_id"] == repair_id

    assert runner.acquisitions == ["enabled", "enabled", "enabled"]
    assert runner.lease_events == [
        ("enter", "enabled"),
        ("exit", "enabled"),
        ("enter", "enabled"),
        ("exit", "enabled"),
        ("enter", "enabled"),
        ("exit", "enabled"),
    ]
    assert len(runner.exit_exceptions) == 3
    assert runner.exit_exceptions[0] is None
    assert isinstance(runner.exit_exceptions[1], HTTPException)
    assert runner.exit_exceptions[2] is None

    restarted = create_app(settings, workflow_runner=runner, mode="api")  # type: ignore[arg-type]
    with TestClient(restarted) as client:
        runner.clear_lease_observations()
        retained = client.get(
            f"/api/v1/tenants/enabled/focus-preview/repairs/{repair_id}",
        )
        absent = client.get(
            "/api/v1/tenants/enabled/focus-preview/repairs/absent",
        )

    assert retained.status_code == 200
    assert retained.json()["repair_id"] == repair_id
    assert absent.status_code == 404
    assert absent.json() == {"detail": "FOCUS Mapping Preview repair 'absent' not found"}
    assert runner.acquisitions == ["enabled", "enabled"]
    assert runner.lease_events == [
        ("enter", "enabled"),
        ("exit", "enabled"),
        ("enter", "enabled"),
        ("exit", "enabled"),
    ]
    assert runner.exit_exceptions == [None, None]
    backend.dispose()


def test_production_post_with_omitted_end_persists_admission_created_at(
    tmp_path: Path,
) -> None:
    from core.api.routes import focus_preview

    admission_at = datetime(2026, 7, 23, 23, 59, 59, 987654, tzinfo=UTC)

    class AdmissionDatetime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            del tz
            return admission_at

    settings = AppSettings(
        tenants={
            "enabled": _tenant(
                tmp_path,
                effective_end=None,
            )
        }
    )
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'omitted-end-production-repair.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = _ProductionRunnerDouble(backend, [])
    app = create_app(settings, workflow_runner=runner, mode="both")  # type: ignore[arg-type]

    try:
        with (
            TestClient(app) as client,
            patch.object(
                focus_preview,
                "datetime",
                AdmissionDatetime,
            ),
        ):
            response = client.post(
                "/api/v1/tenants/enabled/focus-preview/repairs",
                json={"start_date": "2026-07-01", "end_date": "2026-07-02"},
            )
            assert response.status_code == 202
            payload = response.json()
            assert payload["created_at"] == "2026-07-23T23:59:59Z"

            retained = client.get(
                f"/api/v1/tenants/enabled/focus-preview/repairs/{payload['repair_id']}",
            )
            assert retained.status_code == 200
            assert retained.json()["created_at"] == "2026-07-23T23:59:59Z"
    finally:
        backend.dispose()

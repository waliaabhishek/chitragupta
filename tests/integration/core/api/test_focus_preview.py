from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from concurrent.futures import Future
from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path
from threading import Event, Thread
from unittest.mock import patch

import anyio.to_thread
import httpx
import pytest

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, StorageConfig, TenantConfig
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.test_service import _aggregate, _allocation, _seed, _source


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


def _wait_for_terminal(client: SameThreadApiClient, request_id: str) -> dict[str, object]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
        body = response.json()
        if body["status"] in {"ready", "failed"}:
            return body
        time.sleep(0.01)
    pytest.fail("preview request did not reach a terminal state")


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


@pytest.mark.parametrize("suffix", ["/request-1", "/request-1/manifest", "/request-1/files/cost-and-usage.csv"])
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


@pytest.mark.parametrize("suffix", ["", "/manifest", "/files/cost-and-usage.csv"])
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
        "column_profile",
        "status",
        "created_at",
        "started_at",
        "completed_at",
        "diagnostic",
        "source_snapshot",
        "package",
    }


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
        generic_export_after = client.post("/api/v1/tenants/production/export", json=export_request)
        assert generic_export_after.status_code == 200
        assert generic_export_after.content == generic_export_before.content

        unlisted = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}/files/unlisted.csv")
        assert unlisted.status_code == 404
        assert unlisted.json() == {"detail": f"Preview file 'unlisted.csv' not found for request '{request_id}'"}


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
    executor = BlockingExecutor()
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "running-artifacts"),
        max_workers=1,
        executor=executor,
        request_id_factory=lambda: "request-running",
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
    )
    entered = Event()
    release = Event()
    original_generate = runtime._generate

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
            with patch.object(runtime, "_generate", side_effect=blocked_generate):
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
                for suffix in ("/manifest", "/files/cost-and-usage.csv"):
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


@pytest.mark.parametrize("suffix", ["/manifest", "/files/cost-and-usage.csv"])
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

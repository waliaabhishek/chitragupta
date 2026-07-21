from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import anyio
import httpx
import pytest
from fastapi.testclient import TestClient

from core.api.app import create_app
from core.config.models import (
    AppSettings,
    FocusPreviewTenantConfig,
    PreviewConfig,
    StorageConfig,
    TenantConfig,
)
from tests.unit.core.preview.test_revision_models import _revision


class _Backend:
    def create_unit_of_work(self) -> Any:
        raise AssertionError("API current reads are read-only")

    def create_read_only_unit_of_work(self) -> Any:
        raise AssertionError("API current reads are read-only")

    def create_preview_read_unit_of_work(self) -> Any:
        raise AssertionError("fake reader owns current lookup")

    def create_preview_write_unit_of_work(self) -> Any:
        raise AssertionError("API current reads are read-only")

    def create_tables(self) -> None:
        return None

    def dispose(self) -> None:
        return None


class _Reader:
    def __init__(self, current: Any | None, history: tuple[Any, ...] | None = None) -> None:
        self.current = current
        self.history = (() if current is None else (current,)) if history is None else history
        self.by_id = {item.revision_id: item for item in self.history}
        self.failures: dict[str, BaseException] = {}
        self.calls: list[str] = []

    def get_current(self, *, backend: Any, ecosystem: str, tenant_id: str, month_start: date) -> Any | None:
        del backend
        self.calls.append(f"current:{ecosystem}:{tenant_id}:{month_start.isoformat()}")
        if failure := self.failures.get("get_current"):
            raise failure
        return self.current

    def get_for_owner(
        self,
        *,
        backend: Any,
        ecosystem: str,
        tenant_id: str,
        revision_id: str,
    ) -> Any | None:
        del backend
        self.calls.append(f"direct:{ecosystem}:{tenant_id}:{revision_id}")
        if failure := self.failures.get("get_for_owner"):
            raise failure
        return self.by_id.get(revision_id)

    def list_for_owner_month(
        self,
        *,
        backend: Any,
        ecosystem: str,
        tenant_id: str,
        month_start: date,
        limit: int,
        cursor_revision_id: str | None,
    ) -> Any:
        del backend
        self.calls.append(f"list:{ecosystem}:{tenant_id}:{month_start.isoformat()}:{limit}:{cursor_revision_id}")
        if failure := self.failures.get("list_for_owner_month"):
            raise failure
        return SimpleNamespace(items=self.history[:limit], next_cursor=None)

    def validation_summary(self, *, revision: Any) -> Any:
        self.calls.append(f"validation:{revision.revision_id}")
        if failure := self.failures.get("validation_summary"):
            raise failure
        return SimpleNamespace(
            status="passed",
            mapping_profile_version="focus-1.4-preview-v5",
            source_records=2,
            rows=2,
            mapping_errors=0,
            artifact_integrity="passed",
        )

    def read_manifest(self, *, revision: Any) -> bytes:
        self.calls.append(f"manifest:{revision.revision_id}")
        if failure := self.failures.get("read_manifest"):
            raise failure
        return f'{{"revision_id":"{revision.revision_id}"}}'.encode()

    def read_file(self, *, revision: Any, file_name: str) -> tuple[Any, bytes]:
        self.calls.append(f"file:{revision.revision_id}:{file_name}")
        if failure := self.failures.get("read_file"):
            raise failure
        metadata = next(
            (item for item in revision.package.files if item.name == file_name),
            None,
        )
        if metadata is None:
            raise FileNotFoundError("unknown revision file")
        return metadata, f"csv-body:{revision.revision_id}".encode()

    def open_archive(self, *, revision: Any) -> Any:
        self.calls.append(f"archive:{revision.revision_id}")
        if failure := self.failures.get("open_archive"):
            raise failure
        return _Archive(body=f"zip:{revision.revision_id}".encode())


class _Archive:
    def __init__(self, *, body: bytes) -> None:
        self.body = body
        self.size_bytes = len(body)
        self.closed = 0

    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        del chunk_size
        yield self.body

    def close(self) -> None:
        self.closed += 1

    def __enter__(self) -> _Archive:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        del exc_type, exc_value, traceback
        self.close()


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings(
        preview=PreviewConfig(artifact_root=tmp_path / "artifacts"),
        tenants={
            "new-label": TenantConfig(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'tenant.db'}"),
                focus_preview=FocusPreviewTenantConfig(
                    commercial_profile="direct_payg",
                    effective_start_date=date(2026, 1, 1),
                    effective_end_date=date(2027, 1, 1),
                ),
            ),
            "unsupported": TenantConfig(ecosystem="other", tenant_id="other"),
        },
    )


@contextmanager
def _client(
    tmp_path: Path,
    *,
    current: Any | None = None,
    history: tuple[Any, ...] | None = None,
) -> Iterator[tuple[TestClient, _Reader]]:
    app = create_app(_settings(tmp_path))
    reader = _Reader(current, history)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        app.state.backends["new-label"] = _Backend()
        app.state.preview_revision_reader = reader
        yield client, reader


def test_current_metadata_uses_live_route_label_and_guarded_urls(tmp_path: Path) -> None:
    revision = _revision(tenant_name_at_publication="old-label")
    with _client(tmp_path, current=revision) as (client, _reader):
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions/current?month=2026-07")

    assert response.status_code == 200
    body = response.json()
    assert body["tenant_name"] == "new-label"
    assert body["revision_id"] == "revision-1"
    assert body["self_url"].endswith("month=2026-07&revision_id=revision-1")
    urls = [
        body["package"]["manifest"]["download_url"],
        body["package"]["download_all_url"],
        *(item["download_url"] for item in body["package"]["files"]),
    ]
    assert all("month=2026-07" in url and "revision_id=revision-1" in url for url in urls)
    assert body["package"]["download_all_name"] == "focus-mapping-preview-2026-07-revision-1.zip"
    assert "tenant-1" not in response.text
    assert "old-label" not in response.text


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (
            "/api/v1/tenants/new-label/focus-preview/revisions/current",
            {"detail": [{"type": "missing", "loc": ["query", "month"], "msg": "Field required", "input": None}]},
        ),
        (
            "/api/v1/tenants/new-label/focus-preview/revisions/current/manifest?month=2026-07",
            {
                "detail": [
                    {
                        "type": "missing",
                        "loc": ["query", "revision_id"],
                        "msg": "Field required",
                        "input": None,
                    }
                ]
            },
        ),
    ],
)
def test_revision_routes_preserve_exact_missing_query_contract(
    tmp_path: Path, path: str, expected: dict[str, object]
) -> None:
    with _client(tmp_path) as (client, _reader):
        response = client.get(path)

    assert response.status_code == 422
    assert response.json() == expected


def test_invalid_month_wins_over_unknown_tenant_before_backend_creation(tmp_path: Path) -> None:
    with (
        _client(tmp_path) as (client, _reader),
        patch("core.api.routes.focus_preview.get_or_create_backend") as backend_factory,
    ):
        response = client.get("/api/v1/tenants/missing/focus-preview/revisions/current?month=2026-7")

    assert response.status_code == 400
    assert response.json() == {"detail": "month must use YYYY-MM"}
    backend_factory.assert_not_called()


def test_unknown_tenant_and_unsupported_ecosystem_are_cheap_failures(tmp_path: Path) -> None:
    with (
        _client(tmp_path) as (client, _reader),
        patch("core.api.routes.focus_preview.get_or_create_backend") as backend_factory,
    ):
        missing = client.get("/api/v1/tenants/missing/focus-preview/revisions/current?month=2026-07")
        unsupported = client.get("/api/v1/tenants/unsupported/focus-preview/revisions/current?month=2026-07")

    assert missing.status_code == 404
    assert missing.json() == {"detail": "Tenant 'missing' not found"}
    assert unsupported.status_code == 400
    assert unsupported.json() == {"detail": "FOCUS Mapping Preview currently supports only Confluent Cloud tenants"}
    backend_factory.assert_not_called()


@pytest.mark.parametrize("reader_state", ["missing", "wrong-type"])
def test_missing_or_wrong_type_reader_is_reported_before_backend_creation(
    tmp_path: Path,
    reader_state: str,
) -> None:
    app = create_app(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        TestClient(app) as client,
        patch("core.api.routes.focus_preview.get_or_create_backend") as backend_factory,
    ):
        if reader_state == "missing":
            del app.state.preview_revision_reader
        else:
            app.state.preview_revision_reader = object()
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions/current?month=2026-07")

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview revision service is unavailable"}
    backend_factory.assert_not_called()


def test_current_not_found_is_owner_masked(tmp_path: Path) -> None:
    with _client(tmp_path, current=None) as (client, _reader):
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions/current?month=2026-07")

    assert response.status_code == 404
    assert response.json() == {"detail": "Current FOCUS Mapping Preview revision not found"}
    assert "tenant-1" not in response.text


def test_history_lists_current_and_superseded_with_replacement_semantics(tmp_path: Path) -> None:
    current = _revision(
        revision_id="revision-current",
        supersedes_revision_id="revision-old",
        tenant_name_at_publication="old-label",
    )
    superseded = _revision(
        revision_id="revision-old",
        superseded_by_revision_id="revision-current",
        is_current=False,
        tenant_name_at_publication="old-label",
    )
    with _client(tmp_path, current=current, history=(current, superseded)) as (client, reader):
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions?month=2026-07&limit=20")

    assert response.status_code == 200
    body = response.json()
    assert body["replacement_semantics"] == "complete_replacement"
    assert body["consumer_action"] == "replace_do_not_aggregate"
    assert body["next_cursor"] is None
    assert [item["revision_id"] for item in body["items"]] == [
        "revision-current",
        "revision-old",
    ]
    assert [item["lifecycle"] for item in body["items"]] == ["current", "superseded"]
    assert body["items"][0]["validation"] == {
        "status": "passed",
        "mapping_profile_version": "focus-1.4-preview-v5",
        "source_records": 2,
        "rows": 2,
        "mapping_errors": 0,
        "artifact_integrity": "passed",
    }
    assert all(item["tenant_name"] == "new-label" for item in body["items"])
    assert "tenant-1" not in response.text
    assert any(call.startswith("list:confluent_cloud:tenant-1:2026-07-01:20") for call in reader.calls)


@pytest.mark.parametrize(
    ("revision_id", "is_current", "superseded_by_revision_id", "expected_lifecycle"),
    [
        ("revision-current", True, None, "current"),
        ("revision-old", False, "revision-current", "superseded"),
    ],
)
def test_direct_current_and_superseded_detail_and_artifacts_preserve_identity(
    tmp_path: Path,
    revision_id: str,
    is_current: bool,
    superseded_by_revision_id: str | None,
    expected_lifecycle: str,
) -> None:
    revision = _revision(
        revision_id=revision_id,
        superseded_by_revision_id=superseded_by_revision_id,
        is_current=is_current,
    )
    base = f"/api/v1/tenants/new-label/focus-preview/revisions/{revision_id}"
    with _client(tmp_path, history=(revision,)) as (client, reader):
        detail = client.get(base)
        manifest = client.get(f"{base}/manifest")
        data_file = client.get(f"{base}/files/cost-and-usage.csv")
        archive = client.get(f"{base}/archive")

    assert detail.status_code == 200
    body = detail.json()
    assert body["revision_id"] == revision_id
    assert body["lifecycle"] == expected_lifecycle
    assert body["consumer_action"] == "replace_do_not_aggregate"
    assert body["self_url"].endswith(f"/revisions/{revision_id}")
    urls = [
        body["package"]["manifest"]["download_url"],
        body["package"]["files"][0]["download_url"],
        body["package"]["download_all_url"],
    ]
    assert all(f"/revisions/{revision_id}/" in url for url in urls)
    assert all("month=" not in url for url in urls)
    assert manifest.content == f'{{"revision_id":"{revision_id}"}}'.encode()
    assert data_file.content == f"csv-body:{revision_id}".encode()
    assert archive.content == f"zip:{revision_id}".encode()
    artifact_calls = [
        call
        for call in reader.calls
        if call
        in {
            f"manifest:{revision_id}",
            f"file:{revision_id}:cost-and-usage.csv",
            f"archive:{revision_id}",
        }
    ]
    assert artifact_calls[0] == f"manifest:{revision_id}"
    assert f"file:{revision_id}:cost-and-usage.csv" in artifact_calls
    assert artifact_calls[-1] == f"archive:{revision_id}"


def test_direct_unknown_revision_has_one_masked_404(tmp_path: Path) -> None:
    with _client(tmp_path, history=()) as (client, _reader):
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions/missing")

    assert response.status_code == 404
    assert response.json() == {"detail": "FOCUS Mapping Preview revision not found"}


def test_direct_unknown_file_is_checked_after_matching_owner_revision(tmp_path: Path) -> None:
    revision = _revision()
    with _client(tmp_path, history=(revision,)) as (client, reader):
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions/revision-1/files/unknown.csv")

    assert response.status_code == 404
    assert response.json() == {"detail": "FOCUS Mapping Preview file not found for revision"}
    assert any(call == "direct:confluent_cloud:tenant-1:revision-1" for call in reader.calls)
    assert "file:revision-1:unknown.csv" in reader.calls


def test_corrupt_manifest_wins_before_direct_unknown_file_404(tmp_path: Path) -> None:
    revisions = import_module("core.preview.revisions")
    revision = _revision()
    with _client(tmp_path, history=(revision,)) as (client, reader):
        reader.failures["read_file"] = revisions.PreviewRevisionArtifactUnavailableError("private corrupt manifest")
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions/revision-1/files/unknown.csv")

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored FOCUS Mapping Preview revision artifact is unavailable"}
    assert "private corrupt manifest" not in response.text


_DIRECT_FAILURE_CASES = [
    ("", "validation_summary", "validation:revision-1"),
    ("/manifest", "read_manifest", "manifest:revision-1"),
    (
        "/files/cost-and-usage.csv",
        "read_file",
        "file:revision-1:cost-and-usage.csv",
    ),
    ("/archive", "open_archive", "archive:revision-1"),
]


@pytest.mark.parametrize(
    ("suffix", "operation", "expected_call"),
    _DIRECT_FAILURE_CASES,
    ids=["detail", "manifest", "file", "archive"],
)
@pytest.mark.parametrize("failure_kind", ["corrupt-reader", "missing-storage"])
def test_direct_immutable_artifact_failures_share_exact_redacted_500(
    tmp_path: Path,
    suffix: str,
    operation: str,
    expected_call: str,
    failure_kind: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    revisions = import_module("core.preview.revisions")
    sensitive = (
        "reader-secret /tmp/direct-reader tenant-1"
        if failure_kind == "corrupt-reader"
        else "storage-secret missing /srv/direct-storage tenant-1"
    )
    failure: BaseException = (
        revisions.PreviewRevisionArtifactUnavailableError(sensitive)
        if failure_kind == "corrupt-reader"
        else OSError(sensitive)
    )
    with _client(tmp_path, history=(_revision(),)) as (client, reader):
        reader.failures[operation] = failure
        response = client.get(f"/api/v1/tenants/new-label/focus-preview/revisions/revision-1{suffix}")

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored FOCUS Mapping Preview revision artifact is unavailable"}
    for secret in (
        "reader-secret",
        "storage-secret",
        "/tmp/direct-reader",
        "/srv/direct-storage",
        "tenant-1",
    ):
        assert secret not in response.text
        assert secret not in caplog.text
    assert expected_call in reader.calls


@pytest.mark.parametrize(
    ("suffix", "operation", "expected_call"),
    _DIRECT_FAILURE_CASES,
    ids=["detail", "manifest", "file", "archive"],
)
@pytest.mark.parametrize("failure_kind", ["corrupt-reader", "missing-storage"])
def test_direct_immutable_artifact_failure_translation_without_testclient_portal(
    tmp_path: Path,
    suffix: str,
    operation: str,
    expected_call: str,
    failure_kind: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revisions = import_module("core.preview.revisions")
    sensitive = (
        "reader-secret /tmp/direct-reader tenant-1"
        if failure_kind == "corrupt-reader"
        else "storage-secret missing /srv/direct-storage tenant-1"
    )
    failure: BaseException = (
        revisions.PreviewRevisionArtifactUnavailableError(sensitive)
        if failure_kind == "corrupt-reader"
        else OSError(sensitive)
    )
    app = create_app(_settings(tmp_path))
    reader = _Reader(None, history=(_revision(),))
    reader.failures[operation] = failure
    app.state.settings = _settings(tmp_path)
    app.state.backends = {"new-label": _Backend()}
    app.state.preview_revision_reader = reader

    async def run_sync_inline(
        function: Any,
        *args: object,
        **_kwargs: object,
    ) -> object:
        return function(*args)

    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)

    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.get(f"/api/v1/tenants/new-label/focus-preview/revisions/revision-1{suffix}")

    response = asyncio.run(request())

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored FOCUS Mapping Preview revision artifact is unavailable"}
    for secret in (
        "reader-secret",
        "storage-secret",
        "/tmp/direct-reader",
        "/srv/direct-storage",
        "tenant-1",
    ):
        assert secret not in response.text
        assert secret not in caplog.text
    assert expected_call in reader.calls


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "",
            {"detail": [{"type": "missing", "loc": ["query", "month"], "msg": "Field required", "input": None}]},
        ),
        (
            "?month=2026-07&limit=0",
            {
                "detail": [
                    {
                        "type": "greater_than_equal",
                        "loc": ["query", "limit"],
                        "msg": "Input should be greater than or equal to 1",
                        "input": "0",
                        "ctx": {"ge": 1},
                    }
                ]
            },
        ),
        (
            "?month=2026-07&limit=101",
            {
                "detail": [
                    {
                        "type": "less_than_equal",
                        "loc": ["query", "limit"],
                        "msg": "Input should be less than or equal to 100",
                        "input": "101",
                        "ctx": {"le": 100},
                    }
                ]
            },
        ),
        (
            "?month=2026-07&cursor=",
            {
                "detail": [
                    {
                        "type": "string_too_short",
                        "loc": ["query", "cursor"],
                        "msg": "String should have at least 1 character",
                        "input": "",
                        "ctx": {"min_length": 1},
                    }
                ]
            },
        ),
    ],
)
def test_history_query_validation_has_exact_installed_stack_bodies(
    tmp_path: Path,
    query: str,
    expected: dict[str, object],
) -> None:
    with _client(tmp_path) as (client, _reader):
        response = client.get(f"/api/v1/tenants/new-label/focus-preview/revisions{query}")

    assert response.status_code == 422
    assert response.json() == expected


@pytest.mark.parametrize(
    "query",
    [
        "?month=2026-07&limit=0",
        "?month=2026-07&limit=101",
        "?month=2026-07&cursor=",
    ],
)
def test_invalid_history_query_short_circuits_wrong_reader_and_backend_creation(
    tmp_path: Path,
    query: str,
) -> None:
    app = create_app(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        TestClient(app) as client,
        patch("core.api.routes.focus_preview.get_or_create_backend") as backend_factory,
    ):
        app.state.preview_revision_reader = object()
        response = client.get(f"/api/v1/tenants/new-label/focus-preview/revisions{query}")

    assert response.status_code == 422
    backend_factory.assert_not_called()


@pytest.mark.parametrize(
    ("operation", "path"),
    [
        (
            "list_for_owner_month",
            "/api/v1/tenants/new-label/focus-preview/revisions?month=2026-07",
        ),
        (
            "get_for_owner",
            "/api/v1/tenants/new-label/focus-preview/revisions/revision-1",
        ),
    ],
)
def test_history_and_direct_repository_exceptions_share_exact_storage_503(
    tmp_path: Path,
    operation: str,
    path: str,
) -> None:
    with _client(tmp_path, history=(_revision(),)) as (client, reader):
        reader.failures[operation] = RuntimeError("private database detail")
        response = client.get(path)

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview revision storage is unavailable"}
    assert "private database detail" not in response.text


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/tenants/new-label/focus-preview/revisions?month=2026-07",
        "/api/v1/tenants/new-label/focus-preview/revisions/revision-1",
    ],
    ids=["history", "direct"],
)
@pytest.mark.parametrize("backend_failure", ["factory", "cached-incompatible"])
def test_history_and_direct_backend_failures_short_circuit_with_exact_storage_503(
    tmp_path: Path,
    path: str,
    backend_failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    route = import_module("core.api.routes.focus_preview")
    settings = _settings(tmp_path)
    app = create_app(settings)
    reader = _Reader(None, history=(_revision(),))
    app.state.settings = settings
    app.state.backends = {}
    app.state.preview_revision_reader = reader
    if backend_failure == "cached-incompatible":
        app.state.backends["new-label"] = object()

    async def run_sync_inline(
        function: Any,
        *args: object,
        **_kwargs: object,
    ) -> object:
        return function(*args)

    async def request(target: str = path) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.get(target)

    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)
    backend_patch = (
        patch(
            "core.api.routes.focus_preview.get_or_create_backend",
            side_effect=RuntimeError("private /srv/backend tenant-1"),
        )
        if backend_failure == "factory"
        else patch(
            "core.api.routes.focus_preview.get_or_create_backend",
            wraps=route.get_or_create_backend,
        )
    )
    with backend_patch as backend_factory:
        if "?month=" in path:
            invalid = asyncio.run(request("/api/v1/tenants/new-label/focus-preview/revisions?month=2026-7"))
            assert invalid.status_code == 400
            assert invalid.json() == {"detail": "month must use YYYY-MM"}
            backend_factory.assert_not_called()
        response = asyncio.run(request())

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview revision storage is unavailable"}
    assert "private" not in response.text
    assert "/srv/backend" not in response.text
    assert "tenant-1" not in response.text
    backend_factory.assert_called_once()
    assert reader.calls == []


def test_corrupt_history_item_fails_entire_page_with_redacted_error(tmp_path: Path) -> None:
    revisions = import_module("core.preview.revisions")
    with _client(tmp_path, history=(_revision(),)) as (client, reader):
        reader.failures["validation_summary"] = revisions.PreviewRevisionArtifactUnavailableError(
            "private /tmp/path tenant-1"
        )
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions?month=2026-07")

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored FOCUS Mapping Preview revision artifact is unavailable"}
    assert "/tmp/path" not in response.text
    assert "tenant-1" not in response.text


def test_invalid_history_cursor_maps_to_exact_400(tmp_path: Path) -> None:
    persistence = import_module("core.preview.persistence")
    with _client(tmp_path) as (client, reader):
        reader.failures["list_for_owner_month"] = persistence.PreviewRevisionCursorError("revision-missing")
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions?month=2026-07&cursor=revision-missing")

    assert response.status_code == 400
    assert response.json() == {"detail": "FOCUS Mapping Preview revision cursor is invalid"}


def test_invalid_history_scope_wins_before_reader_and_backend_creation(tmp_path: Path) -> None:
    app = create_app(_settings(tmp_path))
    with (
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        TestClient(app) as client,
        patch("core.api.routes.focus_preview.get_or_create_backend") as backend_factory,
    ):
        app.state.preview_revision_reader = object()
        response = client.get("/api/v1/tenants/missing/focus-preview/revisions?month=2026-7")

    assert response.status_code == 400
    assert response.json() == {"detail": "month must use YYYY-MM"}
    backend_factory.assert_not_called()


@pytest.mark.parametrize("suffix", ["manifest", "files/unknown.csv", "archive"])
def test_stale_revision_guard_wins_before_artifact_or_file_lookup(tmp_path: Path, suffix: str) -> None:
    with _client(tmp_path, current=_revision()) as (client, reader):
        response = client.get(
            f"/api/v1/tenants/new-label/focus-preview/revisions/current/{suffix}?month=2026-07&revision_id=revision-old"
        )

    assert response.status_code == 409
    assert response.json() == {
        "detail": {
            "code": "focus_preview_current_changed",
            "message": ("The current FOCUS Mapping Preview revision changed; fetch the current revision and retry."),
            "retryable": True,
        }
    }
    assert not any(call.startswith(("manifest:", "file:", "archive:")) for call in reader.calls)


def test_optional_metadata_guard_detects_replacement(tmp_path: Path) -> None:
    with _client(tmp_path, current=_revision()) as (client, _reader):
        response = client.get(
            "/api/v1/tenants/new-label/focus-preview/revisions/current?month=2026-07&revision_id=revision-old"
        )

    assert response.status_code == 409
    assert response.json()["detail"] == {
        "code": "focus_preview_current_changed",
        "message": ("The current FOCUS Mapping Preview revision changed; fetch the current revision and retry."),
        "retryable": True,
    }


def test_unknown_file_on_matching_current_revision_has_exact_masked_response(tmp_path: Path) -> None:
    with _client(tmp_path, current=_revision()) as (client, reader):
        response = client.get(
            "/api/v1/tenants/new-label/focus-preview/revisions/current/files/unknown.csv"
            "?month=2026-07&revision_id=revision-1"
        )

    assert response.status_code == 404
    assert response.json() == {"detail": "FOCUS Mapping Preview file not found for current revision"}
    assert not any(call.startswith("file:") for call in reader.calls)


@pytest.mark.parametrize(
    ("suffix", "operation"),
    [
        ("manifest", "read_manifest"),
        ("files/cost-and-usage.csv", "read_file"),
        ("archive", "open_archive"),
    ],
)
def test_all_corrupt_revision_artifacts_share_one_redacted_500(
    tmp_path: Path, suffix: str, operation: str, caplog: pytest.LogCaptureFixture
) -> None:
    revisions = __import__("core.preview.revisions", fromlist=["PreviewRevisionArtifactUnavailableError"])
    with _client(tmp_path, current=_revision()) as (client, reader):
        reader.failures[operation] = revisions.PreviewRevisionArtifactUnavailableError("private /tmp/path tenant-1")
        response = client.get(
            f"/api/v1/tenants/new-label/focus-preview/revisions/current/{suffix}?month=2026-07&revision_id=revision-1"
        )

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored FOCUS Mapping Preview revision artifact is unavailable"}
    assert "/tmp/path" not in response.text
    assert "tenant-1" not in response.text
    assert "/tmp/path" not in caplog.text
    assert "tenant-1" not in caplog.text
    assert "revision-1" not in caplog.text


def test_backend_creation_failure_has_exact_storage_503(tmp_path: Path) -> None:
    with (
        _client(tmp_path, current=_revision()) as (client, _reader),
        patch("core.api.routes.focus_preview.get_or_create_backend", side_effect=RuntimeError("secret")),
    ):
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions/current?month=2026-07")

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview revision storage is unavailable"}


def test_backend_protocol_narrowing_and_current_read_failure_share_storage_503(tmp_path: Path) -> None:
    app = create_app(_settings(tmp_path))
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        app.state.preview_revision_reader = _Reader(_revision())
        app.state.backends["new-label"] = object()
        wrong_backend = client.get("/api/v1/tenants/new-label/focus-preview/revisions/current?month=2026-07")
        app.state.backends["new-label"] = _Backend()
        app.state.preview_revision_reader.failures["get_current"] = RuntimeError("private database detail")
        failed_read = client.get("/api/v1/tenants/new-label/focus-preview/revisions/current?month=2026-07")

    assert wrong_backend.status_code == 503
    assert failed_read.status_code == 503
    expected = {"detail": "FOCUS Mapping Preview revision storage is unavailable"}
    assert wrong_backend.json() == expected
    assert failed_read.json() == expected
    assert "private database detail" not in failed_read.text

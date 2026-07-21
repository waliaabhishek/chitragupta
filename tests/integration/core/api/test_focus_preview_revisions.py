from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import patch

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
    def __init__(self, current: Any | None) -> None:
        self.current = current
        self.failures: dict[str, BaseException] = {}
        self.calls: list[str] = []

    def get_current(self, *, backend: Any, ecosystem: str, tenant_id: str, month_start: date) -> Any | None:
        del backend
        self.calls.append(f"current:{ecosystem}:{tenant_id}:{month_start.isoformat()}")
        if failure := self.failures.get("get_current"):
            raise failure
        return self.current

    def read_manifest(self, revision: Any) -> bytes:
        self.calls.append(f"manifest:{revision.revision_id}")
        if failure := self.failures.get("read_manifest"):
            raise failure
        return b"{}"

    def read_file(self, revision: Any, file_name: str) -> tuple[Any, bytes]:
        self.calls.append(f"file:{revision.revision_id}:{file_name}")
        if failure := self.failures.get("read_file"):
            raise failure
        metadata = next(item for item in revision.package.files if item.name == file_name)
        return metadata, b"csv-body"

    def open_archive(self, revision: Any) -> Any:
        self.calls.append(f"archive:{revision.revision_id}")
        if failure := self.failures.get("open_archive"):
            raise failure
        return _Archive()


class _Archive:
    size_bytes = 3

    def __init__(self) -> None:
        self.closed = 0

    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        del chunk_size
        yield b"zip"

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
def _client(tmp_path: Path, *, current: Any | None = None) -> Iterator[tuple[TestClient, _Reader]]:
    app = create_app(_settings(tmp_path))
    reader = _Reader(current)
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


def test_task_does_not_expose_general_retained_revision_route(tmp_path: Path) -> None:
    with _client(tmp_path, current=_revision()) as (client, _reader):
        response = client.get("/api/v1/tenants/new-label/focus-preview/revisions/revision-1?month=2026-07")

    assert response.status_code == 404


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

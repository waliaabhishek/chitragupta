from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import httpx
import pytest

from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_cli import RecordingTransport


def _connection() -> list[str]:
    return [
        "--api-url",
        "https://api.example.test/api/v1",
        "--tenant",
        "production",
    ]


def _artifact(name: str, body: bytes, order: int | None, url: str) -> dict[str, object]:
    return {
        "name": name,
        "media_type": "application/json" if order is None else "text/csv",
        "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "order": order,
        "download_url": url,
    }


def _revision_detail(
    *,
    revision_id: str = "revision-2",
    lifecycle: str = "current",
    manifest_revision_id: str | None = None,
    manifest_url: str | None = None,
) -> tuple[dict[str, object], bytes, bytes]:
    data_body = b"BilledCost\n12.34\n"
    file_metadata = {
        "name": "cost-and-usage.csv",
        "media_type": "text/csv",
        "size_bytes": len(data_body),
        "sha256": hashlib.sha256(data_body).hexdigest(),
        "order": 1,
    }
    manifest_body = (
        json.dumps(
            {
                "schema_version": "chitragupta.preview-manifest.v2",
                "revision_id": manifest_revision_id or revision_id,
                "files": [file_metadata],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    base = f"/api/v1/tenants/production/focus-preview/revisions/{revision_id}"
    package = {
        "manifest": _artifact(
            "manifest.json",
            manifest_body,
            None,
            manifest_url or f"{base}/manifest",
        ),
        "files": [
            _artifact(
                "cost-and-usage.csv",
                data_body,
                1,
                f"{base}/files/cost-and-usage.csv",
            )
        ],
        "download_all_name": f"focus-mapping-preview-{revision_id}.zip",
        "download_all_url": f"{base}/archive",
    }
    detail: dict[str, object] = {
        "revision_id": revision_id,
        "tenant_name": "production",
        "month": "2026-07",
        "start_date": "2026-07-01",
        "end_date": "2026-08-01",
        "lifecycle": lifecycle,
        "monthly_status": "provisional",
        "published_at": "2026-07-21T12:00:00Z",
        "supersedes_revision_id": "revision-1" if lifecycle == "current" else None,
        "superseded_by_revision_id": None if lifecycle == "current" else "revision-2",
        "material_sha256": "a" * 64,
        "source_snapshot": {
            "calculation_timestamp": "2026-07-21T11:00:00Z",
            "calculation_coverage": [],
            "source_through": "2026-07-20T23:59:59Z",
            "effective_coverage_start_date": "2026-07-01",
            "effective_coverage_end_date": "2026-07-21",
            "availability_cutoff_end_date": "2026-07-21",
            "monthly_status": "provisional",
        },
        "validation": {
            "status": "passed",
            "mapping_profile_version": "focus-1.4-v1",
            "source_records": 3,
            "rows": 1,
            "mapping_errors": 0,
            "artifact_integrity": "passed",
        },
        "replacement_semantics": "complete_replacement",
        "consumer_action": "replace_do_not_aggregate",
        "detail_url": base,
        "self_url": base,
        "package": package,
    }
    return detail, manifest_body, data_body


def _history_page() -> dict[str, object]:
    current, _manifest, _data = _revision_detail()
    superseded, _manifest, _data = _revision_detail(
        revision_id="revision-1",
        lifecycle="superseded",
    )
    summary_keys = {
        "revision_id",
        "tenant_name",
        "month",
        "start_date",
        "end_date",
        "lifecycle",
        "monthly_status",
        "published_at",
        "supersedes_revision_id",
        "superseded_by_revision_id",
        "material_sha256",
        "source_snapshot",
        "validation",
        "replacement_semantics",
        "consumer_action",
        "detail_url",
    }
    return {
        "items": [
            {key: value for key, value in current.items() if key in summary_keys},
            {key: value for key, value in superseded.items() if key in summary_keys},
        ],
        "next_cursor": "revision-1",
        "replacement_semantics": "complete_replacement",
        "consumer_action": "replace_do_not_aggregate",
    }


def _invoke(
    monkeypatch: pytest.MonkeyPatch,
    transport: RecordingTransport,
    argv: list[str],
) -> int:
    cli = preview_module("cli")
    real_client = httpx.Client
    monkeypatch.setattr(
        cli.httpx,
        "Client",
        lambda **kwargs: real_client(transport=transport, **kwargs),
    )
    return cli.main(argv)


def _no_network() -> RecordingTransport:
    return RecordingTransport(lambda _request, _sequence: pytest.fail("network must not be called"))


def test_revisions_forwards_exact_query_and_headers_and_prints_replacement_warning(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    page = _history_page()
    transport = RecordingTransport(lambda _request, _sequence: httpx.Response(200, json=page))

    code = _invoke(
        monkeypatch,
        transport,
        [
            "revisions",
            *_connection(),
            "--month",
            "2026-07",
            "--limit",
            "2",
            "--cursor",
            "revision-0",
            "--header",
            "Authorization=Bearer secret",
        ],
    )

    assert code == 0
    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert request.url.path == "/api/v1/tenants/production/focus-preview/revisions"
    assert dict(request.url.params) == {"month": "2026-07", "limit": "2", "cursor": "revision-0"}
    assert request.headers["authorization"] == "Bearer secret"
    output = capsys.readouterr().out
    assert output.count("complete replacement; do not aggregate") == 1
    assert output.index("revision-2") < output.index("revision-1")
    assert "current" in output
    assert "superseded" in output


def test_revisions_json_prints_exact_api_page(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    page = _history_page()
    transport = RecordingTransport(lambda _request, _sequence: httpx.Response(200, json=page))

    code = _invoke(
        monkeypatch,
        transport,
        ["revisions", *_connection(), "--month", "2026-07", "--json"],
    )

    assert code == 0
    assert json.loads(capsys.readouterr().out) == page
    assert dict(transport.requests[0].url.params) == {"month": "2026-07", "limit": "20"}


@pytest.mark.parametrize("lifecycle", ["current", "superseded"])
def test_revision_detail_prints_lifecycle_freshness_validation_and_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    lifecycle: str,
) -> None:
    revision_id = "revision-2" if lifecycle == "current" else "revision-1"
    detail, _manifest, _data = _revision_detail(
        revision_id=revision_id,
        lifecycle=lifecycle,
    )
    transport = RecordingTransport(lambda _request, _sequence: httpx.Response(200, json=detail))

    assert _invoke(monkeypatch, transport, ["revision", *_connection(), revision_id]) == 0
    human = capsys.readouterr().out
    assert revision_id in human
    assert lifecycle in human
    assert "2026-07-21T11:00:00Z" in human
    assert "2026-07-20T23:59:59Z" in human
    assert "passed" in human

    json_transport = RecordingTransport(lambda _request, _sequence: httpx.Response(200, json=detail))
    assert (
        _invoke(
            monkeypatch,
            json_transport,
            ["revision", *_connection(), revision_id, "--json"],
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == detail
    assert json_transport.requests[0].url.path.endswith(f"/revisions/{revision_id}")


def test_revision_manifest_local_is_verified_then_atomically_replaces_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detail, manifest, _data = _revision_detail()

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=manifest)
        return httpx.Response(200, json=detail)

    target = tmp_path / "manifest.json"
    target.write_bytes(b"old manifest")
    transport = RecordingTransport(respond)

    code = _invoke(
        monkeypatch,
        transport,
        ["revision", *_connection(), "revision-2", "--manifest", str(target)],
    )

    assert code == 0
    assert target.read_bytes() == manifest
    assert not list(tmp_path.glob(".*.tmp"))
    assert [request.url.path.rsplit("/", 1)[-1] for request in transport.requests] == [
        "revision-2",
        "manifest",
    ]


def test_revision_manifest_stdout_contains_only_fully_verified_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detail, manifest, _data = _revision_detail(revision_id="revision-1", lifecycle="superseded")
    transport = RecordingTransport(
        lambda request, _sequence: (
            httpx.Response(200, content=manifest)
            if request.url.path.endswith("/manifest")
            else httpx.Response(200, json=detail)
        )
    )
    cli = preview_module("cli")
    stdout = type("BinaryStdout", (), {"buffer": io.BytesIO()})()
    monkeypatch.setattr(cli.sys, "stdout", stdout)

    code = _invoke(
        monkeypatch,
        transport,
        ["revision", *_connection(), "revision-1", "--manifest", "-"],
    )

    assert code == 0
    assert stdout.buffer.getvalue() == manifest


@pytest.mark.parametrize(
    "selectors",
    [
        ["--output-dir", "out", "--manifest", "manifest.json"],
        ["--output-dir", "out", "--file", "part.csv", "--output", "part.csv"],
        ["--output-dir", "out", "--archive", "package.zip"],
        ["--manifest", "manifest.json", "--file", "part.csv", "--output", "part.csv"],
        ["--manifest", "manifest.json", "--archive", "package.zip"],
        ["--file", "part.csv", "--output", "part.csv", "--archive", "package.zip"],
    ],
)
def test_revision_rejects_every_pair_of_output_selectors(selectors: list[str]) -> None:
    cli = preview_module("cli")

    with pytest.raises(SystemExit) as exc_info:
        cli.main(["revision", *_connection(), "revision-2", *selectors])

    assert exc_info.value.code == 2


@pytest.mark.parametrize(
    "arguments",
    [
        ["--file", "cost-and-usage.csv"],
        ["--output", "cost-and-usage.csv"],
        ["--json", "--output-dir", "out"],
        ["--json", "--manifest", "manifest.json"],
        ["--json", "--manifest", "-"],
        ["--json", "--file", "cost-and-usage.csv", "--output", "cost.csv"],
        ["--json", "--archive", "package.zip"],
        ["--json", "--archive", "-"],
    ],
)
def test_revision_rejects_invalid_file_output_and_json_combinations(arguments: list[str]) -> None:
    cli = preview_module("cli")

    with pytest.raises(SystemExit) as exc_info:
        cli.main(["revision", *_connection(), "revision-2", *arguments])

    assert exc_info.value.code == 2


@pytest.mark.parametrize("undeclared", ["unknown.csv", "manifest.json"])
def test_revision_file_rejects_undeclared_and_manifest_like_names_before_file_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    undeclared: str,
) -> None:
    detail, _manifest, _data = _revision_detail()
    transport = RecordingTransport(lambda _request, _sequence: httpx.Response(200, json=detail))
    target = tmp_path / "output.csv"

    code = _invoke(
        monkeypatch,
        transport,
        [
            "revision",
            *_connection(),
            "revision-2",
            "--file",
            undeclared,
            "--output",
            str(target),
        ],
    )

    assert code == 1
    assert [request.url.path.rsplit("/", 1)[-1] for request in transport.requests] == ["revision-2"]
    assert not target.exists()


def test_revision_declared_file_verifies_manifest_and_forwards_headers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detail, manifest, data = _revision_detail()

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=manifest)
        if request.url.path.endswith("/cost-and-usage.csv"):
            return httpx.Response(200, content=data)
        return httpx.Response(200, json=detail)

    transport = RecordingTransport(respond)
    target = tmp_path / "cost.csv"

    code = _invoke(
        monkeypatch,
        transport,
        [
            "revision",
            *_connection(),
            "revision-2",
            "--file",
            "cost-and-usage.csv",
            "--output",
            str(target),
            "--header",
            "X-Trace=one",
        ],
    )

    assert code == 0
    assert target.read_bytes() == data
    assert [request.url.path.rsplit("/", 1)[-1] for request in transport.requests] == [
        "revision-2",
        "manifest",
        "cost-and-usage.csv",
    ]
    assert all(request.headers["x-trace"] == "one" for request in transport.requests)


def test_revision_archive_verifies_identity_and_forwards_headers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detail, manifest, data = _revision_detail()
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("manifest.json", manifest)
        archive.writestr("cost-and-usage.csv", data)
    archive_body = archive_buffer.getvalue()

    transport = RecordingTransport(
        lambda request, _sequence: (
            httpx.Response(200, content=archive_body)
            if request.url.path.endswith("/archive")
            else httpx.Response(200, json=detail)
        )
    )
    target = tmp_path / "revision.zip"

    code = _invoke(
        monkeypatch,
        transport,
        [
            "revision",
            *_connection(),
            "revision-2",
            "--archive",
            str(target),
            "--header",
            "X-Trace=one",
        ],
    )

    assert code == 0
    assert target.read_bytes() == archive_body
    assert [request.url.path.rsplit("/", 1)[-1] for request in transport.requests] == [
        "revision-2",
        "archive",
    ]
    assert all(request.headers["x-trace"] == "one" for request in transport.requests)


def test_revision_manifest_forwards_headers_to_detail_and_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detail, manifest, _data = _revision_detail()
    transport = RecordingTransport(
        lambda request, _sequence: (
            httpx.Response(200, content=manifest)
            if request.url.path.endswith("/manifest")
            else httpx.Response(200, json=detail)
        )
    )

    assert (
        _invoke(
            monkeypatch,
            transport,
            [
                "revision",
                *_connection(),
                "revision-2",
                "--manifest",
                str(tmp_path / "manifest.json"),
                "--header",
                "Authorization=Bearer secret",
                "--header",
                "X-Trace=one",
            ],
        )
        == 0
    )
    assert len(transport.requests) == 2
    for request in transport.requests:
        assert request.headers["authorization"] == "Bearer secret"
        assert request.headers["x-trace"] == "one"


@pytest.mark.parametrize("cross_origin_field", ["manifest", "file", "archive"])
def test_revision_rejects_any_cross_origin_artifact_url_before_artifact_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cross_origin_field: str,
) -> None:
    detail, _manifest, _data = _revision_detail(
        manifest_url="https://evil.example/steal" if cross_origin_field == "manifest" else None,
    )
    package = detail["package"]
    assert isinstance(package, dict)
    if cross_origin_field == "file":
        files = package["files"]
        assert isinstance(files, list)
        file_metadata = files[0]
        assert isinstance(file_metadata, dict)
        file_metadata["download_url"] = "https://evil.example/steal"
    elif cross_origin_field == "archive":
        package["download_all_url"] = "https://evil.example/steal"
    transport = RecordingTransport(lambda _request, _sequence: httpx.Response(200, json=detail))

    code = _invoke(
        monkeypatch,
        transport,
        [
            "revision",
            *_connection(),
            "revision-2",
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--header",
            "Authorization=Bearer secret",
        ],
    )

    assert code == 1
    assert len(transport.requests) == 1
    assert transport.requests[0].url.host == "api.example.test"


@pytest.mark.parametrize(
    "corruption",
    ["checksum", "detail-identity", "manifest-identity", "parse"],
)
@pytest.mark.parametrize("destination", ["local", "stdout"])
def test_revision_manifest_integrity_failure_publishes_nothing_and_exits_three(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
    destination: str,
) -> None:
    detail, manifest, _data = _revision_detail(
        manifest_revision_id="other-revision" if corruption == "manifest-identity" else None,
    )
    if corruption == "detail-identity":
        detail["revision_id"] = "other-revision"
    if corruption == "checksum":
        delivered = bytes([manifest[0] ^ 1]) + manifest[1:]
    elif corruption == "parse":
        delivered = b"x" * len(manifest)
        package = detail["package"]
        assert isinstance(package, dict)
        manifest_metadata = package["manifest"]
        assert isinstance(manifest_metadata, dict)
        manifest_metadata["sha256"] = hashlib.sha256(delivered).hexdigest()
    else:
        delivered = manifest

    transport = RecordingTransport(
        lambda request, _sequence: (
            httpx.Response(200, content=delivered)
            if request.url.path.endswith("/manifest")
            else httpx.Response(200, json=detail)
        )
    )
    cli = preview_module("cli")
    target = tmp_path / "manifest.json"
    target.write_bytes(b"existing target")
    if destination == "stdout":
        stdout = type("BinaryStdout", (), {"buffer": io.BytesIO()})()
        monkeypatch.setattr(cli.sys, "stdout", stdout)
        target_arg = "-"
    else:
        stdout = None
        target_arg = str(target)

    code = _invoke(
        monkeypatch,
        transport,
        ["revision", *_connection(), "revision-2", "--manifest", target_arg],
    )

    assert code == 3
    assert target.read_bytes() == b"existing target"
    assert not list(tmp_path.glob(".*.tmp"))
    if stdout is not None:
        assert stdout.buffer.getvalue() == b""


def test_revision_http_failure_exits_one_and_preserves_existing_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "manifest.json"
    target.write_bytes(b"existing target")
    transport = RecordingTransport(lambda _request, _sequence: httpx.Response(404, json={"detail": "not found"}))

    code = _invoke(
        monkeypatch,
        transport,
        ["revision", *_connection(), "missing", "--manifest", str(target)],
    )

    assert code == 1
    assert target.read_bytes() == b"existing target"
    assert not list(tmp_path.glob(".*.tmp"))

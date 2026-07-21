from __future__ import annotations

import hashlib
import io
import json
import warnings
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType

import httpx
import pytest

from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_cli import RecordingTransport


def _artifact(name: str, body: bytes, order: int | None, url: str) -> dict[str, object]:
    return {
        "name": name,
        "media_type": "application/json" if order is None else "text/csv",
        "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "order": order,
        "download_url": url,
    }


def _ready_package() -> tuple[dict[str, object], bytes, dict[str, bytes], bytes]:
    files = {"part-1.csv": b"name,cost\na,1\n", "part-2.csv": b"name,cost\nb,2\n"}
    manifest_value = {
        "schema_version": "chitragupta.preview-manifest.v2",
        "request_id": "request-1",
        "files": [
            {
                "name": name,
                "media_type": "text/csv",
                "size_bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "order": index,
            }
            for index, (name, body) in enumerate(files.items(), 1)
        ],
    }
    manifest = (json.dumps(manifest_value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    base = "/api/v1/tenants/production/focus-preview/requests/request-1"
    package = {
        "manifest": _artifact("manifest.json", manifest, None, f"{base}/manifest"),
        "files": [
            _artifact(name, body, index, f"{base}/files/{name}") for index, (name, body) in enumerate(files.items(), 1)
        ],
        "download_all_name": "focus-mapping-preview-request-1.zip",
        "download_all_url": f"{base}/archive",
    }
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("manifest.json", manifest)
        for name, body in files.items():
            archive.writestr(name, body)
    return package, manifest, files, archive_buffer.getvalue()


def _zip_bytes(entries: list[tuple[str, bytes]]) -> bytes:
    archive_buffer = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_STORED) as archive:
            for name, body in entries:
                archive.writestr(name, body)
    return archive_buffer.getvalue()


def _status(status: str, package: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "request_id": "request-1",
        "tenant_name": "production",
        "grain": "daily",
        "start_date": "2026-07-01",
        "end_date": "2026-07-02",
        "month": None,
        "column_profile": "full",
        "effective_columns": ["BilledCost"],
        "status": status,
        "created_at": "2026-07-03T00:00:00Z",
        "started_at": "2026-07-03T00:00:01Z" if status != "queued" else None,
        "completed_at": "2026-07-03T00:00:02Z" if status in {"ready", "failed", "expired"} else None,
        "expires_at": "2026-07-10T00:00:02Z" if status in {"ready", "expired"} else None,
        "diagnostic": (
            {"code": "failed", "message": "exact failed diagnostic", "retryable": False} if status == "failed" else None
        ),
        "source_snapshot": None,
        "package": package,
    }


def _invoke(monkeypatch: pytest.MonkeyPatch, transport: RecordingTransport, argv: list[str]) -> int:
    cli = preview_module("cli")
    real_client = httpx.Client
    monkeypatch.setattr(cli.httpx, "Client", lambda **kwargs: real_client(transport=transport, **kwargs))
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    return cli.main(argv)


def _connection() -> list[str]:
    return ["--api-url", "https://api.example.test/api/v1", "--tenant", "production"]


class _ChunkedFileStream(httpx.SyncByteStream):
    def __init__(self, body: bytes, *, fail_late: bool) -> None:
        self._body = body
        self._fail_late = fail_late
        self.iterated = False

    def __iter__(self) -> Iterator[bytes]:
        self.iterated = True
        split = max(1, len(self._body) // 2)
        yield self._body[:split]
        if self._fail_late:
            raise OSError("late stream failure")
        yield self._body[split:]


class _StreamingOnlyFileClient:
    def __init__(
        self,
        *,
        status: dict[str, object],
        manifest: bytes,
        file_body: bytes,
        fail_late: bool,
    ) -> None:
        self._status = status
        self._manifest = manifest
        self._file_body = file_body
        self._fail_late = fail_late
        self.file_get_attempted = False
        self.file_stream = _ChunkedFileStream(file_body, fail_late=fail_late)

    def __enter__(self) -> _StreamingOnlyFileClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback

    def get(self, url: str, *, headers: object) -> httpx.Response:
        del headers
        request = httpx.Request("GET", url)
        if url.endswith("/manifest"):
            return httpx.Response(200, content=self._manifest, request=request)
        if "/files/" in url:
            self.file_get_attempted = True
            raise AssertionError("individual file download must not call client.get")
        return httpx.Response(200, json=self._status, request=request)

    @contextmanager
    def stream(self, method: str, url: str, *, headers: object) -> Iterator[httpx.Response]:
        del headers
        assert method == "GET"
        assert "/files/" in url
        request = httpx.Request(method, url)
        response = httpx.Response(200, request=request, stream=self.file_stream)
        try:
            yield response
        finally:
            response.close()


def test_request_no_wait_json_posts_once_and_never_polls_or_downloads(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    queued = _status("queued")
    transport = RecordingTransport(lambda _request, _sequence: httpx.Response(202, json=queued))

    code = _invoke(
        monkeypatch,
        transport,
        [
            "request",
            *_connection(),
            "--start-date",
            "2026-07-01",
            "--end-date",
            "2026-07-02",
            "--no-wait",
            "--json",
        ],
    )

    assert code == 0
    assert json.loads(capsys.readouterr().out) == queued
    assert [request.method for request in transport.requests] == ["POST"]


def test_status_is_one_get_unless_wait_requested(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    transport = RecordingTransport(lambda _request, _sequence: httpx.Response(200, json=_status("running")))
    assert _invoke(monkeypatch, transport, ["status", *_connection(), "request-1", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "running"
    assert len(transport.requests) == 1

    sequence = iter([_status("running"), _status("ready", _ready_package()[0])])
    waiting = RecordingTransport(lambda _request, _sequence: httpx.Response(200, json=next(sequence)))
    assert _invoke(monkeypatch, waiting, ["status", *_connection(), "request-1", "--wait", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready"
    assert len(waiting.requests) == 2


def test_download_directory_verifies_manifest_and_every_file_before_atomic_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, manifest, files, _archive = _ready_package()

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=manifest)
        for name, body in files.items():
            if request.url.path.endswith(name):
                return httpx.Response(200, content=body)
        return httpx.Response(200, json=_status("ready", package))

    output = tmp_path / "output"
    code = _invoke(
        monkeypatch,
        RecordingTransport(respond),
        ["download", *_connection(), "request-1", "--output-dir", str(output)],
    )

    assert code == 0
    assert (output / "manifest.json").read_bytes() == manifest
    assert {name: (output / name).read_bytes() for name in files} == files
    assert not list(tmp_path.rglob("*.tmp"))


def test_download_single_enumerated_file_verifies_manifest_and_file_before_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, manifest, files, _archive = _ready_package()

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=manifest)
        if request.url.path.endswith("/files/part-2.csv"):
            return httpx.Response(200, content=files["part-2.csv"])
        return httpx.Response(200, json=_status("ready", package))

    output = tmp_path / "selected.csv"
    transport = RecordingTransport(respond)
    code = _invoke(
        monkeypatch,
        transport,
        ["download", *_connection(), "request-1", "--file", "part-2.csv", "--output", str(output)],
    )

    assert code == 0
    assert output.read_bytes() == files["part-2.csv"]
    assert [request.url.path.rsplit("/", 1)[-1] for request in transport.requests] == [
        "request-1",
        "manifest",
        "part-2.csv",
    ]


@pytest.mark.parametrize(
    ("failure", "expected_code"),
    [(None, 0), ("late-stream", 1), ("checksum", 3)],
)
def test_individual_file_streams_to_same_parent_temp_and_replaces_only_after_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str | None,
    expected_code: int,
) -> None:
    cli = preview_module("cli")
    package, manifest, files, _archive = _ready_package()
    expected = files["part-2.csv"]
    streamed = bytes([expected[0] ^ 1]) + expected[1:] if failure == "checksum" else expected
    client = _StreamingOnlyFileClient(
        status=_status("ready", package),
        manifest=manifest,
        file_body=streamed,
        fail_late=failure == "late-stream",
    )
    monkeypatch.setattr(cli.httpx, "Client", lambda **_kwargs: client)
    output = tmp_path / "selected.csv"
    original = b"existing target must survive"
    output.write_bytes(original)

    code = cli.main(
        [
            "download",
            *_connection(),
            "request-1",
            "--file",
            "part-2.csv",
            "--output",
            str(output),
        ]
    )

    assert code == expected_code
    assert client.file_get_attempted is False
    assert client.file_stream.iterated is True
    assert output.read_bytes() == (expected if failure is None else original)
    assert [path.name for path in tmp_path.iterdir()] == ["selected.csv"]


def test_download_unknown_file_fails_without_constructing_or_requesting_a_file_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, manifest, _files, _archive = _ready_package()

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=manifest)
        return httpx.Response(200, json=_status("ready", package))

    target = tmp_path / "unknown.csv"
    transport = RecordingTransport(respond)
    code = _invoke(
        monkeypatch,
        transport,
        ["download", *_connection(), "request-1", "--file", "unknown.csv", "--output", str(target)],
    )

    assert code == 3
    assert not target.exists()
    assert [request.url.path.rsplit("/", 1)[-1] for request in transport.requests] == ["request-1", "manifest"]


def test_verified_archive_is_atomically_written_to_a_local_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, _manifest, _files, archive = _ready_package()
    transport = RecordingTransport(
        lambda request, _sequence: (
            httpx.Response(200, content=archive)
            if request.url.path.endswith("/archive")
            else httpx.Response(200, json=_status("ready", package))
        )
    )
    target = tmp_path / "package.zip"

    code = _invoke(
        monkeypatch,
        transport,
        ["download", *_connection(), "request-1", "--archive", str(target)],
    )

    assert code == 0
    assert target.read_bytes() == archive
    assert not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize("corrupt_target", ["manifest", "file"])
def test_checksum_mismatch_exits_three_and_publishes_no_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corrupt_target: str,
) -> None:
    package, manifest, files, _archive = _ready_package()

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=b"corrupt" if corrupt_target == "manifest" else manifest)
        for name, body in files.items():
            if request.url.path.endswith(name):
                return httpx.Response(200, content=b"corrupt" if corrupt_target == "file" else body)
        return httpx.Response(200, json=_status("ready", package))

    target = tmp_path / "single.csv"
    code = _invoke(
        monkeypatch,
        RecordingTransport(respond),
        ["download", *_connection(), "request-1", "--file", "part-1.csv", "--output", str(target)],
    )

    assert code == 3
    assert not target.exists()


def test_verified_archive_stdout_contains_only_archive_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, _manifest, _files, archive = _ready_package()
    transport = RecordingTransport(
        lambda request, _sequence: (
            httpx.Response(200, content=archive)
            if request.url.path.endswith("/archive")
            else httpx.Response(200, json=_status("ready", package))
        )
    )
    cli = preview_module("cli")
    stdout = type("BinaryStdout", (), {"buffer": io.BytesIO()})()
    monkeypatch.setattr(cli.sys, "stdout", stdout)

    code = _invoke(monkeypatch, transport, ["download", *_connection(), "request-1", "--archive", "-"])

    assert code == 0
    assert stdout.buffer.getvalue() == archive


def test_corrupt_archive_stdout_emits_nothing_and_exits_three(monkeypatch: pytest.MonkeyPatch) -> None:
    package, _manifest, _files, _archive = _ready_package()
    transport = RecordingTransport(
        lambda request, _sequence: (
            httpx.Response(200, content=b"not-a-zip")
            if request.url.path.endswith("/archive")
            else httpx.Response(200, json=_status("ready", package))
        )
    )
    cli = preview_module("cli")
    stdout = type("BinaryStdout", (), {"buffer": io.BytesIO()})()
    monkeypatch.setattr(cli.sys, "stdout", stdout)

    assert _invoke(monkeypatch, transport, ["download", *_connection(), "request-1", "--archive", "-"]) == 3
    assert stdout.buffer.getvalue() == b""


@pytest.mark.parametrize(
    "corruption",
    [
        "extra-entry",
        "duplicate-entry",
        "missing-entry",
        "unsafe-path-entry",
        "manifest-status-drift",
        "embedded-size-mismatch",
        "embedded-checksum-mismatch",
    ],
)
def test_syntactically_valid_but_semantically_corrupt_archives_publish_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    package, manifest, files, _archive = _ready_package()
    entries = [("manifest.json", manifest), *files.items()]
    if corruption == "extra-entry":
        entries.append(("extra.csv", b"extra"))
    elif corruption == "duplicate-entry":
        entries.append(("part-2.csv", files["part-2.csv"]))
    elif corruption == "missing-entry":
        entries.pop()
    elif corruption == "unsafe-path-entry":
        entries[-1] = ("../part-2.csv", entries[-1][1])
    elif corruption == "manifest-status-drift":
        manifest_value = json.loads(manifest)
        manifest_value["files"][0]["media_type"] = "application/octet-stream"
        manifest = (json.dumps(manifest_value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        manifest_metadata = package["manifest"]
        assert isinstance(manifest_metadata, dict)
        manifest_metadata["size_bytes"] = len(manifest)
        manifest_metadata["sha256"] = hashlib.sha256(manifest).hexdigest()
        entries[0] = ("manifest.json", manifest)
    elif corruption == "embedded-size-mismatch":
        entries[1] = ("part-1.csv", files["part-1.csv"] + b"x")
    else:
        body = files["part-1.csv"]
        entries[1] = ("part-1.csv", bytes([body[0] ^ 1]) + body[1:])

    archive = _zip_bytes(entries)
    transport = RecordingTransport(
        lambda request, _sequence: (
            httpx.Response(200, content=archive)
            if request.url.path.endswith("/archive")
            else httpx.Response(200, json=_status("ready", package))
        )
    )
    target = tmp_path / "package.zip"

    code = _invoke(
        monkeypatch,
        transport,
        ["download", *_connection(), "request-1", "--archive", str(target)],
    )

    assert code == 3
    assert not target.exists()
    assert not list(tmp_path.rglob("*.tmp"))


def test_directory_download_stages_sequentially_and_never_partially_publishes_after_late_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, manifest, files, _archive = _ready_package()
    output = tmp_path / "output"
    saw_first_file_staged = False

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        nonlocal saw_first_file_staged
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=manifest)
        if request.url.path.endswith("/part-1.csv"):
            return httpx.Response(200, content=files["part-1.csv"])
        if request.url.path.endswith("/part-2.csv"):
            staged = [
                path for path in tmp_path.rglob("*") if path.is_file() and path.read_bytes() == files["part-1.csv"]
            ]
            saw_first_file_staged = bool(staged)
            assert not output.exists()
            return httpx.Response(200, content=b"same-size-bad!!")
        return httpx.Response(200, json=_status("ready", package))

    code = _invoke(
        monkeypatch,
        RecordingTransport(respond),
        ["download", *_connection(), "request-1", "--output-dir", str(output)],
    )

    assert code == 3
    assert saw_first_file_staged
    assert not output.exists()
    assert not [path for path in tmp_path.rglob("*") if path.is_file()]


def test_directory_download_atomically_replaces_preexisting_package_and_removes_stale_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, manifest, files, _archive = _ready_package()
    output = tmp_path / "output"
    output.mkdir()
    (output / "manifest.json").write_bytes(b"old manifest")
    (output / "part-1.csv").write_bytes(b"old first part")
    (output / "stale.csv").write_bytes(b"must disappear")

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=manifest)
        for name, body in files.items():
            if request.url.path.endswith(name):
                return httpx.Response(200, content=body)
        return httpx.Response(200, json=_status("ready", package))

    code = _invoke(
        monkeypatch,
        RecordingTransport(respond),
        ["download", *_connection(), "request-1", "--output-dir", str(output)],
    )

    assert code == 0
    assert {path.name: path.read_bytes() for path in output.iterdir()} == {
        "manifest.json": manifest,
        **files,
    }
    assert {path.name for path in tmp_path.iterdir()} == {"output"}


def test_preexisting_directory_is_restored_whole_when_atomic_package_replacement_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, manifest, files, _archive = _ready_package()
    output = tmp_path / "output"
    output.mkdir()
    original = {
        "manifest.json": b"old manifest",
        "part-1.csv": b"old first part",
        "stale.csv": b"preserve on rollback",
    }
    for name, body in original.items():
        (output / name).write_bytes(body)

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=manifest)
        for name, body in files.items():
            if request.url.path.endswith(name):
                return httpx.Response(200, content=body)
        return httpx.Response(200, json=_status("ready", package))

    cli = preview_module("cli")
    real_replace = cli.os.replace
    failed = False

    def fail_new_package_publish(source: object, target: object) -> None:
        nonlocal failed
        target_path = Path(target)
        if target_path == output and not failed:
            failed = True
            assert not output.exists()
            raise OSError("atomic replacement failed")
        real_replace(source, target)

    monkeypatch.setattr(cli.os, "replace", fail_new_package_publish)
    code = _invoke(
        monkeypatch,
        RecordingTransport(respond),
        ["download", *_connection(), "request-1", "--output-dir", str(output)],
    )

    assert code == 1
    assert failed
    assert {path.name: path.read_bytes() for path in output.iterdir()} == original
    assert {path.name for path in tmp_path.iterdir()} == {"output"}


@pytest.mark.parametrize("terminal", ["failed", "expired"])
def test_terminal_failed_or_expired_download_is_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
) -> None:
    transport = RecordingTransport(lambda _request, _sequence: httpx.Response(200, json=_status(terminal)))

    assert (
        _invoke(
            monkeypatch,
            transport,
            ["download", *_connection(), "request-1", "--archive", "-"],
        )
        == 1
    )


def test_no_wait_rejects_output_and_json_rejects_archive_stdout() -> None:
    cli = preview_module("cli")
    with pytest.raises(SystemExit) as no_wait:
        cli.main(
            [
                "request",
                *_connection(),
                "--start-date",
                "2026-07-01",
                "--end-date",
                "2026-07-02",
                "--no-wait",
                "--archive",
                "out.zip",
            ]
        )
    assert no_wait.value.code == 2
    with pytest.raises(SystemExit) as stdout:
        cli.main(["download", *_connection(), "request-1", "--archive", "-", "--json"])
    assert stdout.value.code == 2


@pytest.mark.parametrize(
    "output_arguments",
    [
        ["--output-dir", "https://files.example.test/output"],
        ["--file", "part-1.csv", "--output", "file:///tmp/output.csv"],
        ["--archive", "https://files.example.test/package.zip"],
        ["--archive", "//remote-host/package.zip"],
    ],
)
def test_download_rejects_url_and_nonlocal_output_paths_before_network(output_arguments: list[str]) -> None:
    cli = preview_module("cli")

    with pytest.raises(SystemExit) as exc_info:
        cli.main(["download", *_connection(), "request-1", *output_arguments])

    assert exc_info.value.code == 2

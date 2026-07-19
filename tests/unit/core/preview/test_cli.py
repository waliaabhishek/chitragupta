from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.unit.core.preview.conftest import preview_module


def _status(
    request_id: str,
    status: str,
    *,
    diagnostic: dict[str, object] | None = None,
    package: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "request_id": request_id,
        "tenant_name": "production",
        "grain": "daily",
        "start_date": "2026-07-01",
        "end_date": "2026-07-02",
        "column_profile": "full",
        "status": status,
        "created_at": "2026-07-03T00:00:00Z",
        "started_at": None,
        "completed_at": None,
        "diagnostic": diagnostic,
        "source_snapshot": None,
        "package": package,
    }


class RecordingTransport(httpx.BaseTransport):
    """Full synchronous httpx transport fake used by the remote CLI."""

    def __init__(
        self,
        responder: Any,
    ) -> None:
        self.requests: list[httpx.Request] = []
        self._responder = responder
        self.closed = False

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._responder(request, len(self.requests))

    def close(self) -> None:
        self.closed = True


def _invoke(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    transport: RecordingTransport,
    *extra: str,
) -> int:
    cli = preview_module("cli")
    real_client = httpx.Client

    def client_factory(**kwargs: object) -> httpx.Client:
        return real_client(transport=transport, **kwargs)

    monkeypatch.setattr(cli.httpx, "Client", client_factory)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    return cli.main(
        [
            "daily-full",
            "--api-url",
            "https://api.example.test/api/v1",
            "--tenant",
            "production",
            "--start-date",
            "2026-07-01",
            "--end-date",
            "2026-07-02",
            "--output-dir",
            str(tmp_path / "output"),
            *extra,
        ]
    )


def test_cli_forwards_duplicate_headers_on_post_every_poll_manifest_and_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "super-secret-token"  # pragma: allowlist secret
    package = {
        "manifest": {
            "name": "manifest.json",
            "media_type": "application/json",
            "size_bytes": 3,
            "sha256": "a" * 64,
            "download_url": "/api/v1/tenants/production/focus-preview/requests/request-1/manifest",
        },
        "files": [
            {
                "name": "cost-and-usage.csv",
                "media_type": "text/csv",
                "size_bytes": 4,
                "sha256": "b" * 64,
                "order": 1,
                "download_url": "/api/v1/tenants/production/focus-preview/requests/request-1/files/cost-and-usage.csv",
            }
        ],
    }

    def respond(request: httpx.Request, sequence: int) -> httpx.Response:
        if request.method == "POST":
            assert json.loads(request.content) == {
                "grain": "daily",
                "start_date": "2026-07-01",
                "end_date": "2026-07-02",
                "column_profile": "full",
            }
            return httpx.Response(202, json=_status("request-1", "queued"))
        if request.url.path.endswith("/manifest"):
            return httpx.Response(200, content=b"{}\n", headers={"content-type": "application/json"})
        if request.url.path.endswith("cost-and-usage.csv"):
            return httpx.Response(200, content=b"a,b\n", headers={"content-type": "text/csv"})
        status = "running" if sequence == 2 else "ready"
        return httpx.Response(200, json=_status("request-1", status, package=package if status == "ready" else None))

    transport = RecordingTransport(respond)
    exit_code = _invoke(
        monkeypatch,
        tmp_path,
        transport,
        "--header",
        f"X-Secret={secret}",
        "--header",
        "X-Duplicate=first",
        "--header",
        "X-Duplicate=second",
    )

    assert exit_code == 0
    assert [request.method for request in transport.requests] == ["POST", "GET", "GET", "GET", "GET"]
    for request in transport.requests:
        assert request.headers.get_list("x-secret") == [secret]
        assert request.headers.get_list("x-duplicate") == ["first", "second"]
    assert (tmp_path / "output" / "manifest.json").read_bytes() == b"{}\n"
    assert (tmp_path / "output" / "cost-and-usage.csv").read_bytes() == b"a,b\n"
    captured = capsys.readouterr()
    assert secret not in captured.out
    assert secret not in captured.err
    assert secret not in caplog.text
    assert transport.closed is True


@pytest.mark.parametrize(
    ("code", "message", "retryable"),
    [
        (
            "calculation_metadata_unavailable",
            "One or more requested dates lack preview calculation metadata.",
            False,
        ),
        (
            "calculation_unavailable",
            "No successful persisted calculation is available for the requested dates; run the pipeline and retry.",
            True,
        ),
        (
            "calculation_coverage_incomplete",
            "No successful persisted calculation covers every requested date; run the pipeline and retry.",
            True,
        ),
    ],
)
def test_cli_prints_persisted_failure_without_resubmit_or_derived_remediation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    code: str,
    message: str,
    retryable: bool,
) -> None:
    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json=_status("request-1", "queued"))
        return httpx.Response(
            200,
            json=_status(
                "request-1",
                "failed",
                diagnostic={"code": code, "message": message, "retryable": retryable},
            ),
        )

    transport = RecordingTransport(respond)
    exit_code = _invoke(monkeypatch, tmp_path, transport)

    assert exit_code == 1
    assert capsys.readouterr().err.strip() == f"Preview failed [{code}]: {message}"
    assert [request.method for request in transport.requests].count("POST") == 1
    if code == "calculation_metadata_unavailable":
        assert "run the pipeline" not in message.casefold()
        assert "repair" not in message.casefold()


def test_cli_rejects_cross_origin_download_before_attaching_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    package = {
        "manifest": {
            "name": "manifest.json",
            "media_type": "application/json",
            "size_bytes": 3,
            "sha256": "a" * 64,
            "download_url": "https://evil.example/steal",
        },
        "files": [],
    }

    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json=_status("request-1", "queued"))
        return httpx.Response(200, json=_status("request-1", "ready", package=package))

    transport = RecordingTransport(respond)
    exit_code = _invoke(monkeypatch, tmp_path, transport, "--header", "Authorization=Bearer secret")

    assert exit_code != 0
    assert len(transport.requests) == 2
    assert all(request.url.host == "api.example.test" for request in transport.requests)
    assert "secret" not in capsys.readouterr().err


def test_cli_http_error_redacts_raw_url_userinfo_and_query_sentinels() -> None:
    cli = preview_module("cli")
    url = (
        "https://sentinel-user:sentinel-password@"  # pragma: allowlist secret
        "api.example.test/api/v1/status?token=sentinel-query"
    )
    request = httpx.Request(
        "GET",
        url,
    )
    response = httpx.Response(500, request=request)

    with pytest.raises(RuntimeError) as error:
        cli._raise_for_status(response)

    assert str(error.value) == "GET https://api.example.test/api/v1/status returned HTTP 500"
    assert "sentinel-user" not in str(error.value)
    assert "sentinel-password" not in str(error.value)
    assert "sentinel-query" not in str(error.value)


@pytest.mark.parametrize("header", ["missing-equals", "=empty-name"])
def test_cli_rejects_invalid_headers_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    header: str,
) -> None:
    transport = RecordingTransport(lambda _request, _sequence: pytest.fail("network must not be called"))

    assert _invoke(monkeypatch, tmp_path, transport, "--header", header) != 0
    assert transport.requests == []


def test_cli_is_remote_only_and_contains_no_server_mapping_or_storage_imports() -> None:
    cli = preview_module("cli")
    source = inspect.getsource(cli)

    assert "core.preview.mapping" not in source
    assert "core.preview.persistence" not in source
    assert "core.preview.artifacts" not in source
    assert "sqlite" not in source.casefold()

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_cli import RecordingTransport


def _ready(profile: str, columns: list[str]) -> dict[str, object]:
    return {
        "request_id": "request-1",
        "tenant_name": "production",
        "grain": "monthly",
        "start_date": "2026-07-01",
        "end_date": "2026-08-01",
        "month": "2026-07",
        "column_profile": profile,
        "effective_columns": columns,
        "status": "ready",
        "created_at": "2026-07-15T00:00:00Z",
        "started_at": "2026-07-15T00:00:01Z",
        "completed_at": "2026-07-15T00:00:02Z",
        "diagnostic": None,
        "source_snapshot": None,
        "package": {
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
                    "download_url": (
                        "/api/v1/tenants/production/focus-preview/requests/request-1/files/cost-and-usage.csv"
                    ),
                }
            ],
        },
    }


def _invoke(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    transport: RecordingTransport,
    arguments: list[str],
) -> int:
    cli = preview_module("cli")
    real_client = httpx.Client

    def client_factory(**kwargs: object) -> httpx.Client:
        return real_client(transport=transport, **kwargs)

    monkeypatch.setattr(cli.httpx, "Client", client_factory)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    return cli.main(
        [
            "request",
            "--api-url",
            "https://api.example.test/api/v1",
            "--tenant",
            "production",
            *arguments,
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )


@pytest.mark.parametrize(
    ("arguments", "expected_body", "effective"),
    [
        (
            ["--month", "2026-07"],
            {"grain": "monthly", "month": "2026-07", "column_profile": "full"},
            ["BilledCost"],
        ),
        (
            [
                "--start-date",
                "2026-07-01",
                "--end-date",
                "2026-07-02",
                "--column-profile",
                "summary",
            ],
            {
                "grain": "daily",
                "start_date": "2026-07-01",
                "end_date": "2026-07-02",
                "column_profile": "summary",
            },
            ["BilledCost", "Tags"],
        ),
        (
            [
                "--month",
                "2026-07",
                "--column-profile",
                "custom",
                "--column",
                "Tags",
                "--column",
                "BilledCost",
            ],
            {
                "grain": "monthly",
                "month": "2026-07",
                "column_profile": "custom",
                "columns": ["Tags", "BilledCost"],
            },
            ["Tags", "BilledCost"],
        ),
    ],
)
def test_request_command_serializes_grain_profile_and_custom_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    expected_body: dict[str, object],
    effective: list[str],
) -> None:
    def respond(request: httpx.Request, _sequence: int) -> httpx.Response:
        if request.method == "POST":
            assert json.loads(request.content) == expected_body
            return httpx.Response(202, json=_ready(str(expected_body["column_profile"]), effective))
        if request.url.path.endswith("manifest"):
            return httpx.Response(200, content=b"{}\n")
        return httpx.Response(200, content=b"a,b\n")

    transport = RecordingTransport(respond)

    assert _invoke(monkeypatch, tmp_path, transport, arguments) == 0
    assert [request.method for request in transport.requests] == ["POST", "GET", "GET"]


@pytest.mark.parametrize(
    "arguments",
    [
        ["--month", "2026-07", "--start-date", "2026-07-01", "--end-date", "2026-07-02"],
        ["--start-date", "2026-07-01"],
        ["--end-date", "2026-07-02"],
        ["--month", "2026-07", "--column", "BilledCost"],
        ["--month", "2026-07", "--column-profile", "summary", "--column", "BilledCost"],
    ],
)
def test_request_command_rejects_invalid_argument_combinations_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
) -> None:
    assert "request" in preview_module("cli")._parser().format_help()
    transport = RecordingTransport(lambda _request, _sequence: pytest.fail("network must not be called"))

    with pytest.raises(SystemExit):
        _invoke(monkeypatch, tmp_path, transport, arguments)
    assert transport.requests == []

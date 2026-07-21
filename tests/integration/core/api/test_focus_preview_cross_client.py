from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

import anyio.to_thread
import pytest

from core.api.app import create_app
from core.preview import cli
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.test_focus_preview import (
    SameThreadApiClient,
    SameThreadCliClient,
    _aggregate,
    _allocation,
    _body,
    _seed,
    _settings,
    _source,
    _wait_for_terminal,
)


@pytest.fixture(autouse=True)
def _inline_startup_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run_inline(function: Callable[..., object], *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Callable[..., object], *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", run_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)


def test_one_real_stored_package_has_identical_api_cli_frontend_and_persisted_hashes(
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
    app = create_app(settings)
    output_dir = tmp_path / "cli-output"
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), SameThreadApiClient(app) as client:
        app.state.backends["production"] = backend
        submitted = client.post("/api/v1/tenants/production/focus-preview/requests", json=_body())
        status = _wait_for_terminal(client, submitted.json()["request_id"])
        assert status["status"] == "ready"
        package = status["package"]
        artifacts = [package["manifest"], *package["files"]]
        api_bodies = {artifact["name"]: client.get(artifact["download_url"]).content for artifact in artifacts}
        archive = client.get(package["download_all_url"])
        assert archive.status_code == 200

        cli_client = SameThreadCliClient(client)
        monkeypatch.setattr(cli.httpx, "Client", lambda **_kwargs: cli_client)
        monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
        assert (
            cli.main(
                [
                    "download",
                    "--api-url",
                    "http://testserver/api/v1",
                    "--tenant",
                    "production",
                    status["request_id"],
                    "--output-dir",
                    str(output_dir),
                ]
            )
            == 0
        )

        with backend.create_preview_read_unit_of_work() as uow:
            persisted = uow.requests.get_for_owner(status["request_id"], "confluent_cloud", "tenant-1")
        assert persisted is not None and persisted.package is not None
        persisted_metadata = {item.name: item.sha256 for item in (persisted.package.manifest, *persisted.package.files)}
        expected = {artifact["name"]: artifact["sha256"] for artifact in artifacts}
        assert persisted_metadata == expected
        assert {name: hashlib.sha256(body).hexdigest() for name, body in api_bodies.items()} == expected
        assert {name: hashlib.sha256((output_dir / name).read_bytes()).hexdigest() for name in api_bodies} == expected

        response_bodies = {
            artifact["download_url"]: base64.b64encode(api_bodies[artifact["name"]]).decode() for artifact in artifacts
        }
        response_bodies[package["download_all_url"]] = base64.b64encode(archive.content).decode()
        encoded_fixture = base64.b64encode(
            json.dumps({"status": status, "bodies": response_bodies}, sort_keys=True).encode()
        ).decode()

    environment = {
        **os.environ,
        "VITE_FOCUS_PREVIEW_CROSS_CLIENT_FIXTURE": encoded_fixture,
    }
    completed = subprocess.run(
        [
            "npm",
            "--prefix",
            "frontend",
            "test",
            "--",
            "--coverage.enabled=false",
            "src/api/focusPreview.crossClient.test.ts",
        ],
        cwd=Path(__file__).parents[4],
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    backend.dispose()

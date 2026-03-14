from __future__ import annotations

from unittest.mock import patch

from core.config.models import ApiConfig, AppSettings
from main import run_api


def _make_settings(host: str = "127.0.0.1", port: int = 8080) -> AppSettings:
    return AppSettings(
        api=ApiConfig(host=host, port=port),
        tenants={},
    )


# ---------------------------------------------------------------------------
# uvicorn.run kwargs
# ---------------------------------------------------------------------------


class TestRunApiUvicornConfig:
    """run_api must pass specific concurrency-limiting kwargs to uvicorn.run."""

    def test_run_api_passes_workers_1(self) -> None:
        """run_api must call uvicorn.run(workers=1) — required for shared in-process runner."""
        settings = _make_settings()
        with patch("uvicorn.run") as mock_run:
            run_api(settings)

        assert mock_run.called
        assert mock_run.call_args.kwargs.get("workers") == 1

    def test_run_api_passes_limit_concurrency_100(self) -> None:
        """run_api must call uvicorn.run(limit_concurrency=100) for backpressure."""
        settings = _make_settings()
        with patch("uvicorn.run") as mock_run:
            run_api(settings)

        assert mock_run.call_args.kwargs.get("limit_concurrency") == 100

    def test_run_api_passes_timeout_keep_alive_10(self) -> None:
        """run_api must call uvicorn.run(timeout_keep_alive=10) to close idle sockets."""
        settings = _make_settings()
        with patch("uvicorn.run") as mock_run:
            run_api(settings)

        assert mock_run.call_args.kwargs.get("timeout_keep_alive") == 10

    def test_run_api_passes_host_and_port_from_settings(self) -> None:
        """Existing host/port forwarding must be preserved alongside new kwargs."""
        settings = _make_settings(host="0.0.0.0", port=9090)
        with patch("uvicorn.run") as mock_run:
            run_api(settings)

        assert mock_run.call_args.kwargs.get("host") == "0.0.0.0"
        assert mock_run.call_args.kwargs.get("port") == 9090

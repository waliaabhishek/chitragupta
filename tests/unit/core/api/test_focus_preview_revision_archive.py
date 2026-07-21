from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from importlib import import_module
from typing import Any

import pytest

from tests.unit.core.preview.test_revision_models import _revision


class _Archive:
    size_bytes = 3

    def __init__(self, *, stream_error: BaseException | None = None, close_error: BaseException | None = None) -> None:
        self.stream_error = stream_error
        self.close_error = close_error
        self.close_calls = 0

    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        del chunk_size
        yield b"one"
        if self.stream_error is not None:
            raise self.stream_error

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error

    def __enter__(self) -> _Archive:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        del exc_type, exc_value, traceback
        self.close()


def _route() -> Any:
    return import_module("core.api.routes.focus_preview")


def test_revision_archive_close_wrapper_never_propagates_or_logs_sensitive_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    route = _route()
    archive = _Archive(close_error=OSError("secret /tmp/private tenant-1"))

    with caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"):
        route._close_revision_archive_safely(archive, preserving=None)

    assert archive.close_calls == 1
    assert "OSError" in caplog.text
    assert "secret" not in caplog.text
    assert "/tmp/private" not in caplog.text
    assert "tenant-1" not in caplog.text
    assert "Traceback" not in caplog.text


def test_revision_archive_close_wrapper_preserves_active_stream_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    route = _route()
    stream_error = RuntimeError("stream sentinel")
    archive = _Archive(close_error=OSError("close sentinel"))

    with caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"):
        route._close_revision_archive_safely(archive, preserving=stream_error)

    assert "RuntimeError" in caplog.text
    assert "OSError" in caplog.text
    assert "stream sentinel" not in caplog.text
    assert "close sentinel" not in caplog.text


def _response(monkeypatch: pytest.MonkeyPatch, archive: _Archive) -> Any:
    route = _route()
    reader = type("Reader", (), {"open_archive": lambda self, revision: archive})()
    monkeypatch.setattr(route, "_revision_reader", lambda request: reader)
    monkeypatch.setattr(route, "_current_revision", lambda *args: _revision())
    return route.get_current_revision_archive(
        request=object(),
        tenant_name="new-label",
        revision_id="revision-1",
        scope=object(),
    )


def test_archive_stream_normal_consumption_and_background_fallback_both_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = _Archive()
    response = _response(monkeypatch, archive)

    async def consume() -> None:
        assert [chunk async for chunk in response.body_iterator] == [b"one"]
        assert archive.close_calls == 1
        await response.background()

    asyncio.run(consume())
    assert archive.close_calls == 2


def test_archive_stream_failure_preserves_original_when_close_also_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream_error = RuntimeError("stream sentinel")
    archive = _Archive(stream_error=stream_error, close_error=OSError("close sentinel"))
    response = _response(monkeypatch, archive)

    async def consume() -> None:
        iterator = response.body_iterator.__aiter__()
        assert await anext(iterator) == b"one"
        with pytest.raises(RuntimeError, match="stream sentinel") as raised:
            await anext(iterator)
        assert raised.value is stream_error

    asyncio.run(consume())
    assert archive.close_calls == 1


def test_archive_stream_close_or_cancellation_path_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = _Archive()
    response = _response(monkeypatch, archive)

    async def cancel() -> None:
        iterator = response.body_iterator.__aiter__()
        assert await anext(iterator) == b"one"
        await iterator.aclose()

    asyncio.run(cancel())
    assert archive.close_calls == 1


def test_archive_background_cleanup_runs_when_body_is_never_consumed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = _Archive()
    response = _response(monkeypatch, archive)

    asyncio.run(response.background())

    assert archive.close_calls == 1

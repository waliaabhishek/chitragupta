from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, cast

import pytest

from core.api.app import RequestContextMiddleware, RequestTimeoutMiddleware
from core.api.streaming import streaming_csv_response

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

_HEADERS = {"Content-Disposition": "attachment; filename=gate.csv"}


async def _wait_thread_event(event: threading.Event, timeout: float = 3) -> None:
    assert await asyncio.wait_for(asyncio.to_thread(event.wait, timeout), timeout=timeout + 1)


def _production_stack(
    producer_factory: Callable[[], Iterator[str]],
    *,
    timeout_seconds: int = 1,
    receive_observer: Callable[[Message], None] | None = None,
) -> ASGIApp:
    async def endpoint(scope: Scope, receive: Receive, send: Send) -> None:
        async def observe_receive() -> Message:
            message = await receive()
            if receive_observer is not None:
                receive_observer(message)
            return message

        await streaming_csv_response(producer_factory, headers=_HEADERS)(scope, observe_receive, send)

    return cast(
        "ASGIApp",
        RequestContextMiddleware(RequestTimeoutMiddleware(endpoint, timeout_seconds=timeout_seconds)),
    )


def _direct_app(producer_factory: Callable[[], Iterator[str]]) -> ASGIApp:
    async def endpoint(scope: Scope, receive: Receive, send: Send) -> None:
        await streaming_csv_response(producer_factory, headers=_HEADERS)(scope, receive, send)

    return endpoint


async def _invoke(
    app: ASGIApp,
    spec_version: str,
    send: Send,
) -> tuple[asyncio.Queue[Message], asyncio.Task[None]]:
    receive_messages: asyncio.Queue[Message] = asyncio.Queue()
    receive_messages.put_nowait({"type": "http.request", "body": b"", "more_body": False})

    async def receive() -> Message:
        return await receive_messages.get()

    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec_version},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/csv",
        "raw_path": b"/csv",
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "client": ("127.0.0.1", 1234),
        "server": ("127.0.0.1", 8080),
        "state": {},
    }

    async def run_app() -> None:
        await app(scope, receive, send)

    return receive_messages, asyncio.create_task(run_app())


async def _wait_for_status(messages: asyncio.Queue[Message], status_code: int) -> Message:
    while True:
        message = await asyncio.wait_for(messages.get(), timeout=3)
        if message["type"] == "http.response.start" and message["status"] == status_code:
            return message


@pytest.mark.parametrize("spec_version", ["2.0", "2.4"])
def test_preheader_timeout_cancels_worker_before_header_handoff(spec_version: str) -> None:
    async def run() -> None:
        setup_entered = threading.Event()
        release_setup = threading.Event()
        closed = threading.Event()
        advanced: list[str] = []
        worker_thread: list[int] = []
        output_messages: asyncio.Queue[Message] = asyncio.Queue()
        messages: list[Message] = []

        def producer() -> Iterator[str]:
            worker_thread.append(threading.get_ident())
            try:
                setup_entered.set()
                assert release_setup.wait(3)
                yield "id\n"
                advanced.append("row-1")
                yield "row-1\n"
            finally:
                assert threading.get_ident() == worker_thread[0]
                closed.set()

        async def send(message: Message) -> None:
            messages.append(message)
            await output_messages.put(message)

        _, task = await _invoke(_production_stack(producer), spec_version, send)
        await _wait_thread_event(setup_entered)
        start = await _wait_for_status(output_messages, 504)
        assert start["status"] == 504
        assert await asyncio.wait_for(output_messages.get(), timeout=1) == {
            "type": "http.response.body",
            "body": b'{"detail":"Request exceeded 1s timeout"}',
            "more_body": True,
        }
        assert await asyncio.wait_for(output_messages.get(), timeout=1) == {
            "type": "http.response.body",
            "body": b"",
            "more_body": False,
        }

        # The outer response's terminal body means its disconnect relay has run before release.
        release_setup.set()
        await _wait_thread_event(closed)
        await asyncio.wait_for(task, timeout=3)
        assert len(advanced) <= 1
        assert not any(
            message.get("type") == "http.response.start" and message.get("status") == 200 for message in messages
        )
        assert not any(message.get("body") in (b"id\n", b"row-1\n") for message in messages)

    asyncio.run(run())


@pytest.mark.parametrize("spec_version", ["2.0", "2.4"])
def test_disconnect_before_header_closes_worker_without_response_start(spec_version: str) -> None:
    async def run() -> None:
        setup_entered = threading.Event()
        release_setup = threading.Event()
        closed = threading.Event()
        disconnect_seen = threading.Event()
        worker_thread: list[int] = []
        messages: list[Message] = []

        def producer() -> Iterator[str]:
            worker_thread.append(threading.get_ident())
            try:
                setup_entered.set()
                assert release_setup.wait(3)
                yield "id\n"
                yield "never\n"
            finally:
                assert threading.get_ident() == worker_thread[0]
                closed.set()

        async def send(message: Message) -> None:
            messages.append(message)

        def observe_receive(message: Message) -> None:
            if message["type"] == "http.disconnect":
                disconnect_seen.set()

        receive_queue, task = await _invoke(
            _production_stack(producer, receive_observer=observe_receive), spec_version, send
        )
        await _wait_thread_event(setup_entered)
        receive_queue.put_nowait({"type": "http.disconnect"})
        await _wait_thread_event(disconnect_seen)
        release_setup.set()
        await _wait_thread_event(closed)
        try:
            await asyncio.wait_for(task, timeout=3)
        except RuntimeError as exc:
            assert str(exc) == "No response returned."
        assert not any(message["type"] == "http.response.start" for message in messages)

    asyncio.run(run())


@pytest.mark.parametrize("spec_version", ["2.0", "2.4"])
def test_disconnect_during_blocked_query_discards_one_inflight_row(spec_version: str) -> None:
    async def run() -> None:
        query_entered = threading.Event()
        release_query = threading.Event()
        closed = threading.Event()
        disconnect_seen = threading.Event()
        header_sent = asyncio.Event()
        advanced: list[str] = []
        worker_thread: list[int] = []
        messages: list[Message] = []

        def producer() -> Iterator[str]:
            worker_thread.append(threading.get_ident())
            try:
                advanced.append("header")
                yield "id\n"
                query_entered.set()
                assert release_query.wait(3)
                advanced.append("row-1")
                yield "row-1\n"
                advanced.append("row-2")
                yield "row-2\n"
            finally:
                assert threading.get_ident() == worker_thread[0]
                closed.set()

        async def send(message: Message) -> None:
            messages.append(message)
            if message.get("body") == b"id\n":
                header_sent.set()

        def observe_receive(message: Message) -> None:
            if message["type"] == "http.disconnect":
                disconnect_seen.set()

        receive_queue, task = await _invoke(
            _production_stack(producer, receive_observer=observe_receive), spec_version, send
        )
        await asyncio.wait_for(header_sent.wait(), timeout=3)
        await _wait_thread_event(query_entered)
        receive_queue.put_nowait({"type": "http.disconnect"})
        await _wait_thread_event(disconnect_seen)
        release_query.set()
        await _wait_thread_event(closed)
        try:
            await asyncio.wait_for(task, timeout=3)
        except RuntimeError as exc:
            assert str(exc) == "No response returned."
        assert advanced == ["header", "row-1"]
        assert [
            message["body"] for message in messages if message["type"] == "http.response.body" and message.get("body")
        ] == [b"id\n"]

    asyncio.run(run())


@pytest.mark.parametrize("spec_version", ["2.0", "2.4"])
def test_disconnect_during_blocked_transport_send_closes_worker_and_releases_send(spec_version: str) -> None:
    async def run() -> None:
        row_send_started = asyncio.Event()
        release_send = asyncio.Event()
        send_stopped = threading.Event()
        disconnect_seen = threading.Event()
        closed = threading.Event()
        advanced: list[str] = []
        worker_thread: list[int] = []
        messages: list[Message] = []

        def producer() -> Iterator[str]:
            worker_thread.append(threading.get_ident())
            try:
                for name, chunk in (
                    ("header", "id\n"),
                    ("row-1", "row-1\n"),
                    ("row-2", "row-2\n"),
                    ("row-3", "row-3\n"),
                    ("row-4", "row-4\n"),
                ):
                    advanced.append(name)
                    yield chunk
            finally:
                assert threading.get_ident() == worker_thread[0]
                closed.set()

        async def send(message: Message) -> None:
            messages.append(message)
            if message.get("body") == b"row-1\n":
                row_send_started.set()
                try:
                    await release_send.wait()
                finally:
                    send_stopped.set()

        def observe_receive(message: Message) -> None:
            if message["type"] == "http.disconnect":
                disconnect_seen.set()

        receive_queue, task = await _invoke(
            _production_stack(producer, receive_observer=observe_receive), spec_version, send
        )
        await asyncio.wait_for(row_send_started.wait(), timeout=3)
        receive_queue.put_nowait({"type": "http.disconnect"})
        await _wait_thread_event(disconnect_seen)
        try:
            await _wait_thread_event(closed)
        finally:
            # Also release the simulated transport if cancellation did not unwind its await.
            release_send.set()
        await _wait_thread_event(send_stopped)
        try:
            await asyncio.wait_for(task, timeout=3)
        except RuntimeError as exc:
            assert str(exc) == "No response returned."
        assert advanced[:2] == ["header", "row-1"]
        assert set(advanced) <= {"header", "row-1", "row-2", "row-3"}
        assert not any(message.get("body") == b"row-4\n" for message in messages)

    asyncio.run(run())


@pytest.mark.parametrize("spec_version", ["2.0", "2.4"])
def test_production_stack_backpressure_is_bounded_and_normal_drain_is_exact(spec_version: str) -> None:
    async def run() -> None:
        row_send_started = asyncio.Event()
        release_send = asyncio.Event()
        row4_advanced = threading.Event()
        advanced: list[str] = []
        messages: list[Message] = []

        def producer() -> Iterator[str]:
            for name, chunk in (
                ("header", "id,value\n"),
                ("row-1", '1,"a,b"\n'),
                ("row-2", "2,c\n"),
                ("row-3", "3,d\n"),
                ("row-4", "4,e\n"),
                ("row-5", "5,f\n"),
            ):
                advanced.append(name)
                if name == "row-4":
                    row4_advanced.set()
                yield chunk

        async def send(message: Message) -> None:
            messages.append(message)
            if message.get("body") == b'1,"a,b"\n':
                row_send_started.set()
                await release_send.wait()

        _, task = await _invoke(_production_stack(producer), spec_version, send)
        await asyncio.wait_for(row_send_started.wait(), timeout=3)
        assert not await asyncio.to_thread(row4_advanced.wait, 0.25)
        assert advanced[:2] == ["header", "row-1"]
        release_send.set()
        await asyncio.wait_for(task, timeout=3)

        assert messages[0] == {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-disposition", b"attachment; filename=gate.csv"),
                (b"content-type", b"text/csv; charset=utf-8"),
            ],
        }
        assert [message["body"] for message in messages if message["type"] == "http.response.body"] == [
            b"id,value\n",
            b'1,"a,b"\n',
            b"2,c\n",
            b"3,d\n",
            b"4,e\n",
            b"5,f\n",
            b"",
        ]
        assert messages[-1] == {"type": "http.response.body", "body": b"", "more_body": False}
        assert advanced == ["header", "row-1", "row-2", "row-3", "row-4", "row-5"]

    asyncio.run(run())


@pytest.mark.parametrize("spec_version", ["2.0", "2.4"])
def test_direct_helper_does_not_advance_while_its_send_is_blocked(spec_version: str) -> None:
    async def run() -> None:
        row_send_started = asyncio.Event()
        release_send = asyncio.Event()
        advanced: list[str] = []
        messages: list[Message] = []

        def producer() -> Iterator[str]:
            for name, chunk in (("header", "id\n"), ("row-1", "row-1\n"), ("row-2", "row-2\n")):
                advanced.append(name)
                yield chunk

        async def send(message: Message) -> None:
            messages.append(message)
            if message.get("body") == b"row-1\n":
                row_send_started.set()
                await release_send.wait()

        _, task = await _invoke(_direct_app(producer), spec_version, send)
        await asyncio.wait_for(row_send_started.wait(), timeout=3)
        assert advanced == ["header", "row-1"]
        release_send.set()
        await asyncio.wait_for(task, timeout=3)
        assert [message.get("body") for message in messages if message["type"] == "http.response.body"] == [
            b"id\n",
            b"row-1\n",
            b"row-2\n",
            b"",
        ]

    asyncio.run(run())


@pytest.mark.parametrize("spec_version", ["2.0", "2.4"])
def test_raw_task_cancellation_bounds_inflight_query_and_retrieves_worker_outcome(spec_version: str) -> None:
    async def run() -> None:
        query_entered = threading.Event()
        release_query = threading.Event()
        closed = threading.Event()
        header_sent = asyncio.Event()
        advanced: list[str] = []
        worker_thread: list[int] = []
        messages: list[Message] = []
        loop_errors: list[dict[str, object]] = []
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))

        def producer() -> Iterator[str]:
            worker_thread.append(threading.get_ident())
            try:
                advanced.append("header")
                yield "id\n"
                query_entered.set()
                assert release_query.wait(3)
                advanced.append("row-1")
                yield "row-1\n"
                advanced.append("row-2")
                yield "row-2\n"
            finally:
                assert threading.get_ident() == worker_thread[0]
                closed.set()

        async def send(message: Message) -> None:
            messages.append(message)
            if message.get("body") == b"id\n":
                header_sent.set()

        _, task = await _invoke(_production_stack(producer), spec_version, send)
        await asyncio.wait_for(header_sent.wait(), timeout=3)
        await _wait_thread_event(query_entered)
        task.cancel()
        await asyncio.sleep(0)
        release_query.set()
        try:
            await asyncio.wait_for(task, timeout=3)
        except asyncio.CancelledError:
            pass
        except RuntimeError as exc:
            assert str(exc) == "No response returned."
        await _wait_thread_event(closed)
        await asyncio.sleep(0)
        assert advanced[:1] == ["header"]
        assert set(advanced) <= {"header", "row-1"}
        assert {message.get("body") for message in messages if message["type"] == "http.response.body"} <= {
            b"id\n",
            b"row-1\n",
        }
        assert not any("never retrieved" in str(context.get("message", "")).lower() for context in loop_errors)

    asyncio.run(run())


def test_send_failure_is_not_hidden_by_disconnect_guard() -> None:
    async def run() -> None:
        closed = threading.Event()
        worker_thread: list[int] = []
        messages: list[Message] = []

        def producer() -> Iterator[str]:
            worker_thread.append(threading.get_ident())
            try:
                yield "id\n"
                yield "row\n"
            finally:
                assert threading.get_ident() == worker_thread[0]
                closed.set()

        async def send(message: Message) -> None:
            messages.append(message)
            if message.get("body") == b"row\n":
                raise RuntimeError("synthetic transport failure")

        _, task = await _invoke(_direct_app(producer), "2.4", send)
        with pytest.raises(BaseExceptionGroup) as exc_info:
            await asyncio.wait_for(task, timeout=3)
        assert any(
            isinstance(error, RuntimeError) and str(error) == "synthetic transport failure"
            for error in exc_info.value.exceptions
        )
        await _wait_thread_event(closed)
        assert any(message.get("body") == b"row\n" for message in messages)

    asyncio.run(run())

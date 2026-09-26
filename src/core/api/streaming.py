from __future__ import annotations

from collections.abc import Callable, Coroutine, Iterator
from concurrent.futures import CancelledError as FutureCancelledError
from typing import TYPE_CHECKING, Any

import anyio
from anyio import from_thread, to_thread
from starlette.responses import StreamingResponse

if TYPE_CHECKING:
    from anyio.abc import TaskStatus
    from starlette.types import Message, Receive, Scope, Send


class _WorkerOwnedCSVResponse(StreamingResponse):
    def __init__(self, producer_factory: Callable[[], Iterator[str]], headers: dict[str, str]) -> None:
        super().__init__(iter(()), media_type="text/csv", headers=headers)
        self._producer_factory = producer_factory

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        worker_scope = anyio.CancelScope()
        cancelled_exc = anyio.get_cancelled_exc_class()

        async def send_while_active(message: Message) -> None:
            if not worker_scope.cancel_called:
                await send(message)

        async def watch_disconnect(*, task_status: TaskStatus[None]) -> None:
            task_status.started()
            while (await receive())["type"] != "http.disconnect":
                pass
            worker_scope.cancel()

        worker_completed = False
        with worker_scope:
            try:
                async with anyio.create_task_group() as task_group:
                    # This listener is the primary cancellation path for a pre-header timeout.
                    await task_group.start(watch_disconnect)
                    await to_thread.run_sync(
                        self._stream_in_worker,
                        send_while_active,
                        cancelled_exc,
                        abandon_on_cancel=False,
                    )
                    worker_completed = True
                    task_group.cancel_scope.cancel()
            finally:
                # Back up disconnect/raw-task cancellation without cancelling a normal response.
                if not worker_completed and not worker_scope.cancel_called:
                    worker_scope.cancel()

    def _stream_in_worker(
        self, send: Callable[[Message], Coroutine[Any, Any, None]], cancelled_exc: type[BaseException]
    ) -> None:
        producer = iter(self._producer_factory())
        try:
            from_thread.check_cancelled()
            try:
                first_chunk = next(producer)
            except StopIteration:
                raise RuntimeError("CSV producer must yield a header") from None

            # Storage setup and header production must complete before the response starts.
            from_thread.check_cancelled()
            from_thread.run(
                send,
                {"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers},
            )
            from_thread.run(
                send,
                {"type": "http.response.body", "body": first_chunk.encode(self.charset or "utf-8"), "more_body": True},
            )

            while True:
                from_thread.check_cancelled()
                try:
                    chunk = next(producer)
                except StopIteration:
                    break
                from_thread.check_cancelled()
                from_thread.run(
                    send,
                    {"type": "http.response.body", "body": chunk.encode(self.charset or "utf-8"), "more_body": True},
                )

            from_thread.check_cancelled()
            from_thread.run(send, {"type": "http.response.body", "body": b"", "more_body": False})
        except cancelled_exc:
            return
        except FutureCancelledError:
            try:
                from_thread.check_cancelled()
            except cancelled_exc:
                return
            raise
        finally:
            close = getattr(producer, "close", None)
            if close is not None:
                close()


def streaming_csv_response(
    producer_factory: Callable[[], Iterator[str]],
    *,
    headers: dict[str, str],
) -> StreamingResponse:
    return _WorkerOwnedCSVResponse(producer_factory, headers=headers)

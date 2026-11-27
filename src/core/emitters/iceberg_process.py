"""Own the lifetime and bounded handoff for a destination-only Iceberg process."""

from __future__ import annotations

import os
import pickle
import socket
import subprocess
import sys
from contextlib import contextmanager
from struct import pack, unpack
from tempfile import TemporaryFile
from time import monotonic
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from datetime import date
    from typing import BinaryIO

    from core.config.models import IcebergCatalogConfig

_BATCH_FILE_BYTES = 32 * 1024 * 1024
_ADDRESS_SPACE_BYTES = 2 * 1024 * 1024 * 1024
_CONTROL_BYTES = 64 * 1024
_POLL_SECONDS = 0.05
_REAP_SECONDS = 1.0
_SET_PARENT_DEATH_SIGNAL = 1  # Linux PR_SET_PDEATHSIG.


class IcebergDestinationError(RuntimeError):
    """A destination failure with a code safe to record."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class IcebergConflict(Exception):  # noqa: N818
    """The catalog rejected the loaded base."""


class IcebergUnconfirmed(Exception):  # noqa: N818
    """The catalog may have published the intended day."""


class _Channel:
    """Exchange small messages over the inherited local socket."""

    def __init__(self, peer: socket.socket, check: Callable[[], None] = lambda: None) -> None:
        self._peer = peer
        self._check = check

    def send(self, message: object) -> None:
        encoded = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
        if len(encoded) > _CONTROL_BYTES:
            raise IcebergDestinationError("destination_control_size")
        pending = memoryview(pack("!I", len(encoded)) + encoded)
        while pending:
            self._check()
            try:
                pending = pending[self._peer.send(pending) :]
            except TimeoutError:
                continue

    def _read(self, size: int) -> bytes:
        data = bytearray()
        while len(data) < size:
            self._check()
            try:
                part = self._peer.recv(size - len(data))
            except TimeoutError:
                continue
            if not part:
                raise EOFError
            data.extend(part)
        return bytes(data)

    def receive(self) -> object:
        (size,) = unpack("!I", self._read(4))
        if not 0 < size <= _CONTROL_BYTES:
            raise IcebergDestinationError("destination_control_size")
        # Only the two ends of an inherited socket can supply messages.
        return pickle.loads(self._read(size))  # noqa: S301


def _limit_destination(parent: int) -> None:
    import ctypes
    import resource
    import signal

    # Linux PR_SET_PDEATHSIG, followed by a check for a parent already lost
    # before this process could request the signal.
    if ctypes.CDLL(None).prctl(_SET_PARENT_DEATH_SIGNAL, signal.SIGKILL, 0, 0, 0) != 0:
        raise IcebergDestinationError("destination_resource_limit_unavailable")
    if os.getppid() != parent:
        raise EOFError

    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        ceiling = min(limit for limit in (_ADDRESS_SPACE_BYTES, soft, hard) if limit != resource.RLIM_INFINITY)
        resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
    except OSError, ValueError:
        raise IcebergDestinationError("destination_resource_limit_unavailable") from None


def _read_batches(channel: _Channel, batch_file: BinaryIO) -> Iterator[list[dict[str, object]]]:
    while True:
        command = channel.receive()
        if command == "finish":
            return
        if command != "batch":
            raise IcebergDestinationError("destination_process")
        batch_file.seek(0)
        # The parent writes this bounded file and only this child reads it.
        yield cast("list[dict[str, object]]", pickle.load(batch_file))  # noqa: S301
        channel.send(("staged", None))


def _destination(parent: int, channel: _Channel, batch_file: BinaryIO) -> None:
    phase = "catalog_connection"
    committing = False
    try:
        _limit_destination(parent)
        try:
            # Keep native worker stacks within the destination address space.
            os.environ["PYICEBERG_MAX_WORKERS"] = "4"
            import pyarrow as pa  # type: ignore[import-untyped]
            from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException

            from core.emitters.iceberg_transport import IcebergCatalog, load_table, publish
        except ImportError:
            raise IcebergDestinationError("iceberg_dependency_unavailable") from None
        pa.set_cpu_count(2)
        pa.set_io_thread_count(4)
        settings, identifier = cast("tuple[IcebergCatalogConfig, str]", channel.receive())
        catalog = IcebergCatalog(settings)
        with catalog:
            phase = "table_load"
            table = load_table(catalog, identifier)
            channel.send(("loaded", str(table.metadata.table_uuid)))
            command = channel.receive()
            if command is None:
                return
            phase = "table_write"
            scope, row_count, max_bytes = cast("tuple[tuple[str, str, date], int, int]", command)

            def permit_commit() -> None:
                channel.send(("commit_ready", None))
                if channel.receive() != "commit":
                    raise IcebergDestinationError("destination_process")

            try:
                snapshot = publish(
                    table,
                    _read_batches(channel, batch_file),
                    scope,
                    row_count,
                    max_bytes,
                    permit_commit=permit_commit,
                )
            except CommitFailedException:
                channel.send(("conflict", None))
                return
            except CommitStateUnknownException:
                channel.send(("unconfirmed", None))
                return
            except Exception:
                if catalog.commit_started:
                    channel.send(("unconfirmed", None))
                    return
                raise
            committing = True  # A close error after a commit leaves its outcome uncertain.
        channel.send(("published", snapshot))
    except BrokenPipeError, EOFError:
        return
    except MemoryError:
        channel.send(("unconfirmed", None) if committing else ("error", "destination_memory"))
    except IcebergDestinationError as exc:
        channel.send(("unconfirmed", None) if committing else ("error", exc.code))
    except Exception:
        channel.send(("unconfirmed", None) if committing else ("error", phase))


class IcebergDestination:
    """A checked base and its one-use destination process."""

    def __init__(
        self, peer: socket.socket, batch_file: BinaryIO, deadline: float, shutdown_check: Callable[[], bool]
    ) -> None:
        self._batch_file = batch_file
        self._deadline = deadline
        self._shutdown_check = shutdown_check
        self._commit_permitted = False
        self._channel = _Channel(peer, self._check)
        self.uuid = ""

    def _raise_stopped(self, code: str) -> None:
        if self._commit_permitted:
            raise IcebergUnconfirmed
        raise IcebergDestinationError(code)

    def _check(self) -> None:
        if self._shutdown_check():
            self._raise_stopped("export_interrupted")
        if monotonic() >= self._deadline:
            self._raise_stopped("export_deadline")

    def _send(self, value: object) -> None:
        try:
            self._channel.send(value)
        except EOFError, OSError:
            self._raise_stopped("destination_process")

    def _receive(self, expected: str) -> object:
        try:
            event, data = cast("tuple[str, object]", self._channel.receive())
        except EOFError, OSError:
            self._raise_stopped("destination_process")
        match event:
            case "error":
                raise IcebergDestinationError(cast("str", data))
            case "conflict":
                raise IcebergConflict
            case "unconfirmed":
                raise IcebergUnconfirmed
            case _:
                if event != expected:
                    self._raise_stopped("destination_process")
        return data

    def load(self, settings: IcebergCatalogConfig, identifier: str) -> None:
        self._send((settings, identifier))
        self.uuid = cast("str", self._receive("loaded"))

    def overwrite_batches(
        self,
        batches: Iterable[list[dict[str, object]]],
        *,
        ecosystem: str,
        tenant_id: str,
        chargeback_date: date,
        row_count: int,
        max_bytes: int,
    ) -> int:
        self._send(((ecosystem, tenant_id, chargeback_date), row_count, max_bytes))
        for batch in batches:
            self._check()
            payload = pickle.dumps(batch, protocol=pickle.HIGHEST_PROTOCOL)
            if len(payload) > _BATCH_FILE_BYTES:
                raise IcebergDestinationError("source_byte_limit")
            self._batch_file.seek(0)
            self._batch_file.truncate()
            self._batch_file.write(payload)
            self._batch_file.flush()
            del payload
            self._send("batch")
            self._receive("staged")
        self._send("finish")
        self._receive("commit_ready")
        # Lost confirmation is ambiguous only after the child can call REST.
        self._check()
        self._commit_permitted = True
        try:
            self._send("commit")
            return cast("int", self._receive("published"))
        except IcebergConflict, IcebergUnconfirmed:
            raise
        except Exception:
            raise IcebergUnconfirmed from None


@contextmanager
def open_iceberg_table(
    settings: IcebergCatalogConfig,
    identifier: str,
    *,
    deadline: float,
    shutdown_check: Callable[[], bool],
) -> Iterator[IcebergDestination]:
    if sys.platform != "linux":
        raise IcebergDestinationError("destination_resource_limit_unavailable")
    caller, worker = socket.socketpair()
    with caller, worker, TemporaryFile(dir=os.environ.get("SQLITE_TMPDIR")) as batch_file:
        caller.settimeout(_POLL_SECONDS)
        fd, batch_fd = worker.fileno(), batch_file.fileno()
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from core.emitters.iceberg_process import run_destination; run_destination()",
                str(os.getpid()),
                str(fd),
                str(batch_fd),
            ],
            pass_fds=(fd, batch_fd),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        worker.close()
        try:
            destination = IcebergDestination(caller, batch_file, deadline - _REAP_SECONDS, shutdown_check)
            destination.load(settings, identifier)
            yield destination
        finally:
            if process.poll() is None:
                process.kill()
            try:
                process.wait(timeout=_REAP_SECONDS)
            except subprocess.TimeoutExpired:
                raise IcebergDestinationError("destination_reap") from None


def run_destination() -> None:
    """Run the child entry point with inherited descriptors."""
    parent_pid, socket_fd, file_fd = map(int, sys.argv[1:])
    with socket.socket(fileno=socket_fd) as peer, os.fdopen(file_fd, "rb", buffering=0) as batch_stream:
        _destination(parent_pid, _Channel(peer), batch_stream)

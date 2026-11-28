"""Apply one monotonic deadline to a dedicated export SQL connection."""

from __future__ import annotations

from contextlib import closing
from ipaddress import ip_address
from os import environ
from os.path import abspath
from pathlib import Path
from selectors import DefaultSelector
from sqlite3 import SQLITE_INTERRUPT
from time import monotonic
from typing import TYPE_CHECKING, cast

from sqlalchemy import event, make_url
from sqlalchemy.pool import NullPool
from sqlmodel import create_engine

from core.storage.iceberg import IcebergStorageDeadlineError, IcebergStorageUnsupportedError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from sqlite3 import Connection as SQLiteConnection
    from sqlite3 import Cursor as SQLiteCursor

    from psycopg.abc import ConnParam, PQGen
    from sqlalchemy import Connection, Engine
    from sqlalchemy.engine import URL, ExceptionContext
    from sqlalchemy.pool import ConnectionPoolEntry


def export_sql_remaining(deadline: float) -> float:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise IcebergStorageDeadlineError("Iceberg SQL deadline expired")
    return remaining


def create_export_sql_engine(connection_string: str, deadline: float, *, read_only: bool) -> Engine:
    export_sql_remaining(deadline)
    url = make_url(connection_string)
    if url.drivername in {"sqlite", "sqlite+pysqlite"}:
        engine = create_engine(_existing_sqlite_url(url, read_only=read_only), poolclass=NullPool)
        _configure_sqlite(engine, deadline, read_only=read_only)
        return engine
    if url.drivername == "postgresql+psycopg":
        engine = create_engine(url, poolclass=NullPool, creator=_postgresql_creator(url, deadline))
        event.listen(engine, "handle_error", _postgresql_deadline_error, retval=True)
        return engine
    raise IcebergStorageUnsupportedError("Iceberg SQL deadlines support SQLite and PostgreSQL with psycopg 3")


def _existing_sqlite_url(url: URL, *, read_only: bool) -> URL:
    database = url.database
    if not database or database == ":memory:":
        raise IcebergStorageUnsupportedError("Iceberg SQLite storage requires a database file")
    _, parameters = url.get_dialect()().create_connect_args(url)
    if parameters.get("uri") and database.startswith("file:"):
        mode = url.query.get("mode")
        if database == "file::memory:" or mode == "memory":
            raise IcebergStorageUnsupportedError("Iceberg SQLite storage requires a database file")
        if mode == "ro" and not read_only:
            raise IcebergStorageUnsupportedError("Iceberg SQLite checkpoint writes require a writable database file")
    else:
        # Preserve the driver options. Unknown query arguments were never interpreted as SQLite URI flags.
        driver_options = {key: url.query[key] for key in parameters if key in url.query}
        url = url.set(database=Path(abspath(database)).as_uri(), query=driver_options)
    return url.update_query_dict({"uri": "true", "mode": "ro" if read_only else "rw"})


def _configure_sqlite(engine: Engine, deadline: float, *, read_only: bool) -> None:
    def busy_timeout() -> int:
        return min(5000, int(1000 * export_sql_remaining(deadline)))

    def set_busy_timeout(connection: SQLiteConnection) -> None:
        connection.execute(f"PRAGMA busy_timeout={busy_timeout()}").close()

    @event.listens_for(engine, "do_connect")
    def limit_initial_wait(_dialect: object, _record: object, _args: object, parameters: dict[str, object]) -> None:
        parameters["timeout"] = busy_timeout() / 1000

    @event.listens_for(engine, "connect")
    def configure(connection: SQLiteConnection, record: ConnectionPoolEntry) -> None:
        def abort_at_deadline() -> int:
            if monotonic() < deadline:
                return 0
            record.info["iceberg_deadline_interrupted"] = True
            return 1

        connection.set_progress_handler(abort_at_deadline, 1000)
        if read_only:
            connection.execute("PRAGMA query_only=1").close()

    @event.listens_for(engine, "handle_error", retval=True)
    def translate_interrupt(context: ExceptionContext) -> IcebergStorageDeadlineError | None:
        connection = context.connection
        if (
            connection is not None
            and getattr(context.original_exception, "sqlite_errorcode", None) == SQLITE_INTERRUPT
            and connection.info.pop("iceberg_deadline_interrupted", False)
        ):
            return IcebergStorageDeadlineError("Iceberg SQL deadline expired")
        return None

    @event.listens_for(engine, "before_cursor_execute")
    def limit_statement(_connection: Connection, cursor: SQLiteCursor, *_args: object) -> None:
        set_busy_timeout(cursor.connection)

    @event.listens_for(engine, "commit")
    def limit_commit(connection: Connection) -> None:
        set_busy_timeout(cast("SQLiteConnection", connection.connection.dbapi_connection))


def _postgresql_creator(url: URL, deadline: float) -> Callable[[], object]:
    from psycopg import Connection as PostgreSQLConnection
    from psycopg.conninfo import make_conninfo
    from psycopg.errors import ConnectionTimeout
    from psycopg.generators import connect
    from psycopg.rows import TupleRow

    args, parameters = url.get_dialect()().create_connect_args(url)
    _validate_postgresql_address(parameters)

    class DeadlineConnection(PostgreSQLConnection[TupleRow]):
        def wait[T](self, gen: PQGen[T], interval: float = 0.1) -> T:
            del interval
            try:
                export_sql_remaining(deadline)
                events = next(gen)
                while True:
                    events = gen.send(_socket_ready(self.pgconn.socket, events, deadline))
            except StopIteration as result:
                return cast("T", result.value)
            except IcebergStorageDeadlineError as exc:
                self.close()
                raise ConnectionTimeout("Iceberg SQL deadline expired") from exc
            finally:
                gen.close()

    def open_connection() -> DeadlineConnection:
        export_sql_remaining(deadline)
        conninfo = make_conninfo(*args, **parameters)
        with closing(connect(conninfo)) as generator:
            try:
                socket, events = next(generator)
                while True:
                    socket, events = generator.send(_socket_ready(socket, events, deadline))
            except StopIteration as result:
                connection = DeadlineConnection(result.value)
        try:
            # Keep a stricter timeout supplied by the server, role, or normal connection options.
            timeout_ms = max(1, int(1000 * export_sql_remaining(deadline)))
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT set_config('statement_timeout', "
                    "least(%s, coalesce(nullif(setting::integer, 0), %s))::text, false) "
                    "FROM pg_settings WHERE name = 'statement_timeout'",
                    (timeout_ms, timeout_ms),
                )
            connection.autocommit = False
        except BaseException:
            connection.close()
            raise
        return connection

    return open_connection


def _postgresql_deadline_error(context: ExceptionContext) -> IcebergStorageDeadlineError | None:
    if isinstance(context.original_exception.__cause__, IcebergStorageDeadlineError):
        # Keep the driver's disconnect handling. The new exception cannot reuse the cause in its own chain.
        return IcebergStorageDeadlineError("Iceberg SQL deadline expired")
    return None


def _validate_postgresql_address(parameters: Mapping[str, ConnParam]) -> None:
    service = parameters.get("service") or environ.get("PGSERVICE")
    if service and not parameters.get("hostaddr"):
        raise IcebergStorageUnsupportedError("Iceberg PostgreSQL service connections require an explicit hostaddr")
    host = str(parameters.get("host") or environ.get("PGHOST", ""))
    hostaddr = str(parameters.get("hostaddr") or environ.get("PGHOSTADDR", ""))
    if hostaddr:
        addresses = hostaddr.split(",")
    elif host:
        addresses = host.split(",")
    else:
        return  # libpq uses the local Unix socket by default.
    for address in addresses:
        if not hostaddr and address.startswith(("/", "@")):
            continue
        try:
            ip_address(address)
        except ValueError:
            raise IcebergStorageUnsupportedError(
                "Iceberg PostgreSQL storage requires a numeric host, Unix socket, or numeric hostaddr"
            ) from None


def _socket_ready(socket: int, events: int, deadline: float) -> int:
    with DefaultSelector() as selector:
        selector.register(socket, events)
        ready = selector.select(export_sql_remaining(deadline))
    if not ready:
        raise IcebergStorageDeadlineError("Iceberg SQL deadline expired")
    return ready[0][1]

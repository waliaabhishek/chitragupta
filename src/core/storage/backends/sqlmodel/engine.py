from __future__ import annotations

import logging
import threading
from urllib.parse import urlparse

from sqlalchemy import Engine, event
from sqlmodel import create_engine

logger = logging.getLogger(__name__)
_engines: dict[str, Engine] = {}
_engine_lock = threading.Lock()

_RO_KEY_PREFIX = "readonly:"

_BASE_SQLITE_PRAGMAS = [
    "PRAGMA journal_mode=WAL;",
    "PRAGMA cache_size=-65535;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA temp_store=MEMORY;",
    "PRAGMA busy_timeout=5000;",
]


def _create_cached_engine(
    cache_key: str,
    connection_string: str,
    extra_pragmas: list[str] | None = None,
    **engine_kwargs: object,
) -> Engine:
    """Create and cache an engine under cache_key with double-checked locking.

    SQLite engines receive _BASE_SQLITE_PRAGMAS plus any extra_pragmas on
    every new connection. Non-SQLite engines are created without pragmas.
    """
    if cache_key not in _engines:
        with _engine_lock:
            if cache_key not in _engines:
                parsed = urlparse(connection_string)
                safe_url = f"{parsed.scheme}://{parsed.hostname or '?'}"
                logger.info("Creating new engine for: %s...", safe_url)
                new_engine = create_engine(connection_string, pool_pre_ping=True, **engine_kwargs)

                if connection_string.startswith("sqlite"):
                    pragmas = list(_BASE_SQLITE_PRAGMAS)
                    if extra_pragmas:
                        pragmas.extend(extra_pragmas)

                    @event.listens_for(new_engine, "connect")
                    def _set_sqlite_pragma(dbapi_connection: object, _connection_record: object) -> None:
                        cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
                        for pragma in pragmas:
                            cursor.execute(pragma)
                        cursor.close()

                _engines[cache_key] = new_engine
    return _engines[cache_key]


def get_or_create_engine(connection_string: str, **engine_kwargs: object) -> Engine:
    """Get or create a cached engine for the given connection string.

    Uses double-checked locking to ensure one engine per connection string.
    SQLite engines get WAL mode and performance pragmas.
    """
    return _create_cached_engine(connection_string, connection_string, **engine_kwargs)  # type: ignore[arg-type]  # engine kwargs forwarded from caller; extra_pragmas always passed explicitly


def get_or_create_read_only_engine(connection_string: str, **engine_kwargs: object) -> Engine:
    """Get or create a cached read-only engine for the given connection string.

    SQLite connections get PRAGMA query_only=1 in addition to WAL/cache pragmas,
    preventing lock escalation beyond SHARED. WAL readers and the pipeline
    writer proceed concurrently with zero contention.
    """
    return _create_cached_engine(
        _RO_KEY_PREFIX + connection_string,
        connection_string,
        extra_pragmas=["PRAGMA query_only=1;"],
        **engine_kwargs,
    )


def dispose_all_engines() -> None:
    """Dispose all cached engines. Call during shutdown."""
    with _engine_lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()

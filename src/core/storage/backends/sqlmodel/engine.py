from __future__ import annotations

import logging
import threading

from sqlalchemy import Engine, event
from sqlmodel import create_engine

logger = logging.getLogger(__name__)
_engines: dict[str, Engine] = {}
_engine_lock = threading.Lock()


def get_or_create_engine(connection_string: str, **engine_kwargs: object) -> Engine:
    """Get or create a cached engine for the given connection string.

    Uses double-checked locking to ensure one engine per connection string.
    SQLite engines get WAL mode and performance pragmas.
    """
    if connection_string not in _engines:
        with _engine_lock:
            if connection_string not in _engines:
                logger.info("Creating new engine for: %s...", connection_string[:30])
                new_engine = create_engine(connection_string, pool_pre_ping=True, **engine_kwargs)

                if connection_string.startswith("sqlite"):

                    @event.listens_for(new_engine, "connect")
                    def _set_sqlite_pragma(dbapi_connection: object, _connection_record: object) -> None:
                        cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
                        cursor.execute("PRAGMA journal_mode=WAL;")
                        cursor.execute("PRAGMA cache_size=-65535;")
                        cursor.execute("PRAGMA synchronous=NORMAL;")
                        cursor.execute("PRAGMA temp_store=MEMORY;")
                        cursor.execute("PRAGMA busy_timeout=5000;")
                        cursor.close()

                _engines[connection_string] = new_engine
    return _engines[connection_string]


def dispose_all_engines() -> None:
    """Dispose all cached engines. Call during shutdown."""
    with _engine_lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()

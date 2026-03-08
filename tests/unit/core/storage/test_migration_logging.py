from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend


@pytest.fixture()
def backend(tmp_path) -> SQLModelBackend:
    db_path = tmp_path / "test.db"
    conn = f"sqlite:///{db_path}"
    return SQLModelBackend(conn, CoreStorageModule(), use_migrations=True)


class TestMigrationLoggingPreservation:
    def test_root_level_preserved(self, backend: SQLModelBackend) -> None:
        """Root logger level is unchanged after _run_migrations() returns."""
        root = logging.root
        # Force a distinct level so clobbering to WARNING is detectable
        root.setLevel(logging.DEBUG)
        original_level = root.level  # DEBUG = 10

        def clobber_level(cfg, rev):
            root.setLevel(logging.WARNING)

        try:
            with patch("alembic.command.upgrade", side_effect=clobber_level):
                backend._run_migrations()

            assert root.level == original_level, (
                f"Root logger level was not restored: got {root.level!r}, expected {original_level!r}"
            )
        finally:
            root.setLevel(logging.WARNING)  # restore to pytest default

    def test_root_handlers_preserved(self, backend: SQLModelBackend) -> None:
        """Root logger handlers list is restored after _run_migrations() returns."""
        root = logging.root
        original_handlers = root.handlers[:]

        sentinel_handler = logging.NullHandler()
        original_handlers_with_sentinel = [sentinel_handler]
        root.handlers[:] = original_handlers_with_sentinel

        def clobber_handlers(cfg, rev):
            root.handlers[:] = []

        try:
            with patch("alembic.command.upgrade", side_effect=clobber_handlers):
                backend._run_migrations()

            assert root.handlers == original_handlers_with_sentinel, (
                f"Root logger handlers were not restored: got {root.handlers!r}"
            )
        finally:
            root.handlers[:] = original_handlers

    def test_restore_on_exception(self, backend: SQLModelBackend) -> None:
        """Root logger state is restored even when command.upgrade raises."""
        root = logging.root
        original_level = root.level
        original_handlers = root.handlers[:]

        def clobber_and_raise(cfg, rev):
            root.setLevel(logging.CRITICAL)
            root.handlers[:] = []
            raise RuntimeError("alembic exploded")

        try:
            with (
                patch("alembic.command.upgrade", side_effect=clobber_and_raise),
                pytest.raises(RuntimeError, match="alembic exploded"),
            ):
                backend._run_migrations()

            assert root.level == original_level, (
                f"Root level not restored after exception: got {root.level!r}, expected {original_level!r}"
            )
            assert root.handlers == original_handlers, (
                f"Root handlers not restored after exception: got {root.handlers!r}"
            )
        finally:
            # Ensure assertion failures don't leak mutated root logger state
            root.setLevel(original_level)
            root.handlers[:] = original_handlers

    def test_command_upgrade_called_with_correct_config(self, backend: SQLModelBackend) -> None:
        """command.upgrade is called once with a Config whose sqlalchemy.url matches the backend.

        Logging preservation is intentionally tested separately in test_root_level_preserved
        and test_root_handlers_preserved to keep each test focused on a single concern.
        """
        captured: list = []

        def capture_upgrade(cfg, rev) -> None:
            captured.append((cfg, rev))

        with patch("alembic.command.upgrade", side_effect=capture_upgrade):
            backend._run_migrations()

        assert len(captured) == 1
        cfg, rev = captured[0]
        assert rev == "head"
        assert cfg.get_main_option("sqlalchemy.url") == backend._connection_string

    def test_create_tables_preserves_root_logger(self, backend: SQLModelBackend) -> None:
        """create_tables() preserves root logger state when use_migrations=True."""
        root = logging.root
        root.setLevel(logging.DEBUG)
        original_level = root.level
        original_handlers = root.handlers[:]

        def clobber_level(cfg, rev):
            root.setLevel(logging.WARNING)

        try:
            with patch("alembic.command.upgrade", side_effect=clobber_level):
                backend.create_tables()

            assert root.level == original_level, (
                f"Root logger level was not restored by create_tables(): "
                f"got {root.level!r}, expected {original_level!r}"
            )
        finally:
            root.setLevel(logging.WARNING)
            root.handlers[:] = original_handlers

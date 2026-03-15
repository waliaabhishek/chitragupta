from __future__ import annotations

import pytest

# get_or_create_read_only_engine does not exist yet — ImportError causes red state for all tests
from core.storage.backends.sqlmodel.engine import (
    _engine_lock,
    _engines,
    dispose_all_engines,
    get_or_create_engine,
    get_or_create_read_only_engine,
)


@pytest.fixture(autouse=True)
def clean_engine_cache() -> object:
    """Clean engine cache before and after each test."""
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()
    yield
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()


class TestReadOnlyEngineCache:
    def test_read_only_engine_cached_separately_from_write_engine(self) -> None:
        """get_or_create_read_only_engine returns a different object than get_or_create_engine."""
        cs = "sqlite:///:memory:"
        write_engine = get_or_create_engine(cs)
        ro_engine = get_or_create_read_only_engine(cs)
        assert ro_engine is not write_engine

    def test_read_only_engine_returns_same_object_on_second_call(self) -> None:
        """get_or_create_read_only_engine is idempotent — same engine object on repeated calls."""
        cs = "sqlite:///:memory:"
        ro_engine_1 = get_or_create_read_only_engine(cs)
        ro_engine_2 = get_or_create_read_only_engine(cs)
        assert ro_engine_1 is ro_engine_2

    def test_read_only_engine_stored_with_readonly_prefix(self) -> None:
        """Read-only engine is cached under 'readonly:<connection_string>' key."""
        cs = "sqlite:///:memory:"
        get_or_create_read_only_engine(cs)
        assert f"readonly:{cs}" in _engines

    def test_write_engine_not_created_when_only_read_only_requested(self) -> None:
        """Calling get_or_create_read_only_engine does not populate the plain connection string key."""
        cs = "sqlite:///:memory:"
        get_or_create_read_only_engine(cs)
        assert cs not in _engines

    def test_dispose_all_engines_clears_both_write_and_readonly_entries(self, tmp_path: object) -> None:
        """dispose_all_engines() clears entries for both the write and read-only engines."""
        db_path = tmp_path / "test.db"  # type: ignore[operator]
        cs = f"sqlite:///{db_path}"
        get_or_create_engine(cs)
        get_or_create_read_only_engine(cs)

        assert cs in _engines
        assert f"readonly:{cs}" in _engines

        dispose_all_engines()

        assert cs not in _engines
        assert f"readonly:{cs}" not in _engines

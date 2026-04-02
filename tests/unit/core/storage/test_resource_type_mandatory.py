from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.resource import CoreResource, ResourceStatus


@pytest.fixture
def session() -> Generator[Session]:
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose(close=True)


def _make_resource(**overrides: Any) -> CoreResource:
    defaults: dict[str, Any] = dict(
        ecosystem="eco",
        tenant_id="t1",
        resource_id="r1",
        resource_type="kafka_cluster",
        status=ResourceStatus.ACTIVE,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        metadata={},
    )
    defaults.update(overrides)
    return CoreResource(**defaults)


# ---------------------------------------------------------------------------
# Test 1: find_active_at without resource_type → TypeError
# ---------------------------------------------------------------------------


class TestFindActiveAtResourceTypeMandatory:
    """resource_type must be a required keyword argument on find_active_at."""

    def test_find_active_at_without_resource_type_raises_type_error(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        repo = SQLModelResourceRepository(session)
        with pytest.raises(TypeError):
            repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC))  # type: ignore[call-arg]

    def test_find_by_period_without_resource_type_raises_type_error(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        repo = SQLModelResourceRepository(session)
        with pytest.raises(TypeError):
            repo.find_by_period(  # type: ignore[call-arg]
                "eco",
                "t1",
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 2, 1, tzinfo=UTC),
            )

    def test_find_paginated_without_resource_type_raises_type_error(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        repo = SQLModelResourceRepository(session)
        with pytest.raises(TypeError):
            repo.find_paginated("eco", "t1", limit=10, offset=0)  # type: ignore[call-arg]

    def test_find_by_parent_without_resource_type_raises_type_error(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        repo = SQLModelResourceRepository(session)
        with pytest.raises(TypeError):
            repo.find_by_parent("eco", "t1", "parent-1")  # type: ignore[call-arg]

    def test_find_active_at_with_resource_type_does_not_raise(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        repo = SQLModelResourceRepository(session)
        # Should not raise
        results, total = repo.find_active_at(
            "eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), resource_type="kafka_cluster"
        )
        assert results == []

    def test_find_active_at_with_list_resource_type_does_not_raise(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        repo = SQLModelResourceRepository(session)
        results, total = repo.find_active_at(
            "eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), resource_type=["kafka_cluster", "topic"]
        )
        assert results == []


# ---------------------------------------------------------------------------
# Test 2: _apply_resource_type_filter helper
# ---------------------------------------------------------------------------


class TestApplyResourceTypeFilter:
    """_apply_resource_type_filter must exist and append correct WHERE clauses."""

    def _import_fn(self) -> Any:
        from core.storage.backends.sqlmodel.repositories import _apply_resource_type_filter

        return _apply_resource_type_filter

    def test_function_is_importable(self) -> None:
        fn = self._import_fn()
        assert callable(fn)

    def test_str_appends_one_clause(self) -> None:
        fn = self._import_fn()
        where: list[Any] = []
        fn(where, "kafka_cluster")
        assert len(where) == 1

    def test_str_clause_is_not_in_operator(self) -> None:
        """str resource_type → equality clause, not IN clause."""
        fn = self._import_fn()
        where: list[Any] = []
        fn(where, "kafka_cluster")
        sql = str(where[0].compile(compile_kwargs={"literal_binds": True}))
        # Equality has no IN keyword
        assert " IN " not in sql.upper()

    def test_str_clause_contains_the_value(self) -> None:
        fn = self._import_fn()
        where: list[Any] = []
        fn(where, "kafka_cluster")
        sql = str(where[0].compile(compile_kwargs={"literal_binds": True}))
        assert "kafka_cluster" in sql

    def test_list_appends_one_clause(self) -> None:
        fn = self._import_fn()
        where: list[Any] = []
        fn(where, ["a", "b"])
        assert len(where) == 1

    def test_list_clause_uses_in_operator(self) -> None:
        """Non-empty Sequence → IN clause."""
        fn = self._import_fn()
        where: list[Any] = []
        fn(where, ["a", "b"])
        sql = str(where[0].compile(compile_kwargs={"literal_binds": True}))
        assert " IN " in sql.upper()

    def test_empty_list_appends_one_clause(self) -> None:
        fn = self._import_fn()
        where: list[Any] = []
        fn(where, [])
        assert len(where) == 1

    def test_empty_list_clause_is_literal_false(self) -> None:
        """Empty sequence → literal(False) — guaranteed zero rows."""
        fn = self._import_fn()
        where: list[Any] = []
        fn(where, [])
        sql = str(where[0].compile(compile_kwargs={"literal_binds": True}))
        # literal(False) compiles to "0" or "false"
        assert sql.strip().lower() in ("0", "false")

    def test_empty_list_clause_has_no_in_or_equals(self) -> None:
        fn = self._import_fn()
        where: list[Any] = []
        fn(where, [])
        sql = str(where[0].compile(compile_kwargs={"literal_binds": True}))
        assert " IN " not in sql.upper()
        assert "resource_type" not in sql.lower()

    def test_appends_to_existing_where_list(self) -> None:
        """Helper must append, not replace, existing where clauses."""
        fn = self._import_fn()
        sentinel = object()
        where: list[Any] = [sentinel]  # type: ignore[list-item]
        fn(where, "kafka_cluster")
        assert len(where) == 2
        assert where[0] is sentinel

    def test_str_filters_correctly_via_real_query(self, session: Session) -> None:
        """Equality filter actually works against SQLite."""
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="kc-1", resource_type="kafka_cluster"))
        repo.upsert(_make_resource(resource_id="topic-1", resource_type="topic"))
        session.commit()

        results, _ = repo.find_active_at("eco", "t1", datetime(2026, 6, 1, tzinfo=UTC), resource_type="kafka_cluster")
        assert len(results) == 1
        assert results[0].resource_id == "kc-1"

    def test_list_filters_correctly_via_real_query(self, session: Session) -> None:
        """IN filter returns multiple matching types."""
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="kc-1", resource_type="kafka_cluster"))
        repo.upsert(_make_resource(resource_id="topic-1", resource_type="topic"))
        repo.upsert(_make_resource(resource_id="env-1", resource_type="environment"))
        session.commit()

        results, _ = repo.find_active_at(
            "eco",
            "t1",
            datetime(2026, 6, 1, tzinfo=UTC),
            resource_type=["kafka_cluster", "environment"],
        )
        assert len(results) == 2
        assert {r.resource_type for r in results} == {"kafka_cluster", "environment"}

    def test_empty_list_returns_zero_rows_via_real_query(self, session: Session) -> None:
        """Empty sequence → literal(False) → zero rows (no accidental full-table scan)."""
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="kc-1", resource_type="kafka_cluster"))
        session.commit()

        results, _ = repo.find_active_at("eco", "t1", datetime(2026, 6, 1, tzinfo=UTC), resource_type=[])
        assert results == []

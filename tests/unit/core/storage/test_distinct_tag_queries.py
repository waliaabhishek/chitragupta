from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest
from sqlmodel import Session, SQLModel, create_engine

if TYPE_CHECKING:
    from sqlalchemy import Engine

from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository


@pytest.fixture
def engine() -> Generator[Engine]:
    eng = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(eng)
    yield eng
    eng.dispose(close=True)


@pytest.fixture
def session(engine: Engine) -> Generator[Session]:
    with Session(engine) as s:
        yield s


class TestGetDistinctKeys:
    def test_get_distinct_keys_returns_sorted_deduplicated_keys(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "e1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "e2", "team", "a", "admin")
        repo.add_tag("t1", "resource", "e3", "env", "staging", "admin")
        session.commit()

        keys = repo.get_distinct_keys("t1")

        assert keys == ["env", "team"]

    def test_get_distinct_keys_entity_type_filter_resource(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "identity", "u1", "team", "platform", "admin")
        session.commit()

        keys = repo.get_distinct_keys("t1", entity_type="resource")

        assert keys == ["env"]

    def test_get_distinct_keys_entity_type_filter_identity(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "identity", "u1", "team", "platform", "admin")
        session.commit()

        keys = repo.get_distinct_keys("t1", entity_type="identity")

        assert keys == ["team"]

    def test_get_distinct_keys_empty_returns_empty_list(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)

        keys = repo.get_distinct_keys("t1")

        assert keys == []

    def test_get_distinct_keys_tenant_isolation(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t2", "resource", "r1", "team", "platform", "admin")
        session.commit()

        t1_keys = repo.get_distinct_keys("t1")
        t2_keys = repo.get_distinct_keys("t2")

        assert t1_keys == ["env"]
        assert t2_keys == ["team"]

    def test_get_distinct_keys_alphabetical_order(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "e1", "zone", "a", "admin")
        repo.add_tag("t1", "resource", "e2", "app", "b", "admin")
        repo.add_tag("t1", "resource", "e3", "env", "c", "admin")
        session.commit()

        keys = repo.get_distinct_keys("t1")

        assert keys == ["app", "env", "zone"]


class TestGetDistinctValues:
    def test_get_distinct_values_returns_sorted_deduplicated_values(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r2", "env", "staging", "admin")
        repo.add_tag("t1", "resource", "r3", "env", "prod", "admin")  # duplicate value
        session.commit()

        values = repo.get_distinct_values("t1", "env")

        assert values == ["prod", "staging"]

    def test_get_distinct_values_entity_type_filter(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "identity", "u1", "env", "staging", "admin")
        session.commit()

        resource_values = repo.get_distinct_values("t1", "env", entity_type="resource")
        identity_values = repo.get_distinct_values("t1", "env", entity_type="identity")

        assert resource_values == ["prod"]
        assert identity_values == ["staging"]

    def test_get_distinct_values_q_prefix_filter(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r2", "env", "production", "admin")
        repo.add_tag("t1", "resource", "r3", "env", "staging", "admin")
        session.commit()

        values = repo.get_distinct_values("t1", "env", q="pro")

        assert values == ["prod", "production"]

    def test_get_distinct_values_q_prefix_case_insensitive(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "Production", "admin")
        repo.add_tag("t1", "resource", "r2", "env", "staging", "admin")
        session.commit()

        values = repo.get_distinct_values("t1", "env", q="pro")

        assert values == ["Production"]

    def test_get_distinct_values_unknown_key_returns_empty_list(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        values = repo.get_distinct_values("t1", "nonexistent")

        assert values == []

    def test_get_distinct_values_tenant_isolation(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t2", "resource", "r1", "env", "staging", "admin")
        session.commit()

        t1_values = repo.get_distinct_values("t1", "env")
        t2_values = repo.get_distinct_values("t2", "env")

        assert t1_values == ["prod"]
        assert t2_values == ["staging"]

    def test_get_distinct_values_alphabetical_order(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "staging", "admin")
        repo.add_tag("t1", "resource", "r2", "env", "dev", "admin")
        repo.add_tag("t1", "resource", "r3", "env", "prod", "admin")
        session.commit()

        values = repo.get_distinct_values("t1", "env")

        assert values == ["dev", "prod", "staging"]

    def test_get_distinct_values_no_cross_key_leakage(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r1", "region", "us-east-1", "admin")
        session.commit()

        env_values = repo.get_distinct_values("t1", "env")
        region_values = repo.get_distinct_values("t1", "region")

        assert env_values == ["prod"]
        assert region_values == ["us-east-1"]

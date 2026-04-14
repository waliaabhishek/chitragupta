from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.storage.backends.sqlmodel.base_tables import IdentityTable, ResourceTable
from core.storage.backends.sqlmodel.repositories import (
    SQLModelEntityTagRepository,
    SQLModelGraphRepository,
)

ECOSYSTEM = "confluent_cloud"
TENANT_ID = "org-test"

_CREATED = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def engine() -> Generator[Any]:
    eng = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(eng)
    yield eng
    eng.dispose(close=True)


@pytest.fixture
def session(engine: Any) -> Generator[Session]:
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session: Session) -> SQLModelGraphRepository:
    tags_repo = SQLModelEntityTagRepository(session)
    return SQLModelGraphRepository(session, tags_repo)


def _resource(
    resource_id: str,
    resource_type: str = "kafka_cluster",
    parent_id: str | None = None,
    display_name: str | None = None,
    status: str = "active",
) -> ResourceTable:
    return ResourceTable(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        resource_type=resource_type,
        display_name=display_name or resource_id,
        parent_id=parent_id,
        status=status,
        cloud=None,
        region=None,
        created_at=_CREATED,
        deleted_at=None,
    )


def _identity(identity_id: str, display_name: str | None = None) -> IdentityTable:
    return IdentityTable(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        identity_id=identity_id,
        identity_type="service_account",
        display_name=display_name or identity_id,
        created_at=_CREATED,
        deleted_at=None,
    )


class TestSearchEntitiesParentDisplayName:
    def test_search_entities_includes_parent_display_name(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Resource results include resolved parent_display_name from the parent resource row."""
        session.add(_resource("env-abc", resource_type="environment", parent_id=None, display_name="ACME Env"))
        session.add(
            _resource("lkc-kafka", resource_type="kafka_cluster", parent_id="env-abc", display_name="Kafka Prod")
        )
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "lkc-kafka")

        assert len(results) == 1
        r = results[0]
        assert r.id == "lkc-kafka"
        assert r.parent_display_name == "ACME Env"

    def test_search_entities_identity_parent_display_name_is_none(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Identity results always have parent_display_name=None — identities have no parent."""
        session.add(_identity("sa-search", display_name="search-service-account"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "sa-search")

        identity_results = [r for r in results if r.id == "sa-search"]
        assert len(identity_results) == 1
        assert identity_results[0].parent_display_name is None

    def test_search_entities_missing_parent_returns_none(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Resource whose parent doesn't exist in resources table gets parent_display_name=None."""
        session.add(
            _resource(
                "lkc-orphan",
                resource_type="kafka_cluster",
                parent_id="env-missing",
                display_name="Orphan Cluster",
            )
        )
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "lkc-orphan")

        assert len(results) == 1
        assert results[0].parent_display_name is None

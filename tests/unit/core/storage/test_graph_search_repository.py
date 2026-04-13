from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.graph import GraphSearchResultData
from core.storage.backends.sqlmodel.base_tables import IdentityTable, ResourceTable
from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository, SQLModelGraphRepository

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
    parent_id: str | None = "env-abc",
    display_name: str | None = None,
    status: str = "active",
    deleted_at: datetime | None = None,
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
        deleted_at=deleted_at,
    )


def _identity(
    identity_id: str,
    identity_type: str = "service_account",
    display_name: str | None = None,
    deleted_at: datetime | None = None,
) -> IdentityTable:
    return IdentityTable(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        identity_id=identity_id,
        identity_type=identity_type,
        display_name=display_name or identity_id,
        created_at=_CREATED,
        deleted_at=deleted_at,
    )


class TestGraphSearchRepositoryReturnsResults:
    def test_search_returns_matching_resources(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V1: partial match on resource_id returns GraphSearchResultData list."""
        session.add(_resource("lkc-kafka-prod", "kafka_cluster", display_name="Kafka Prod"))
        session.add(_resource("lkc-other", "kafka_cluster", display_name="Other"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "kafka")

        ids = [r.id for r in results]
        assert "lkc-kafka-prod" in ids

    def test_search_returns_graphsearchresultdata_instances(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V3: returned items are GraphSearchResultData with correct fields."""
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc", display_name="ABC Cluster"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "lkc-abc")

        assert len(results) == 1
        r = results[0]
        assert isinstance(r, GraphSearchResultData)
        assert r.id == "lkc-abc"
        assert r.resource_type == "kafka_cluster"
        assert r.display_name == "ABC Cluster"
        assert r.parent_id == "env-abc"
        assert r.status == "active"


class TestGraphSearchRepositoryILIKE:
    def test_search_ilike_case_insensitive_matches_display_name(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V2: query 'KafKa' (mixed case) matches display_name='kafka-prod'."""
        session.add(_resource("lkc-001", "kafka_cluster", display_name="kafka-prod"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "KafKa")

        assert any(r.id == "lkc-001" for r in results)

    def test_search_ilike_matches_on_resource_id(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """ILIKE applied to resource_id as well as display_name."""
        session.add(_resource("my-KAFKA-cluster", "kafka_cluster", display_name="unrelated"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "kafka")

        assert any(r.id == "my-KAFKA-cluster" for r in results)

    def test_search_includes_identities(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V3: search also queries IdentityTable; identity results included."""
        session.add(_identity("sa-kafka-001", display_name="kafka service account"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "kafka")

        assert any(r.id == "sa-kafka-001" for r in results)


class TestGraphSearchRepositoryIdentityParentId:
    def test_search_identity_has_parent_id_none(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V7: identity result always has parent_id=None (IdentityTable has no parent_id column)."""
        session.add(_identity("sa-001"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "sa-001")

        identity_results = [r for r in results if r.id == "sa-001"]
        assert len(identity_results) == 1
        assert identity_results[0].parent_id is None

    def test_search_deleted_identity_has_status_deleted(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Deleted identity (deleted_at is not None) → status='deleted'."""
        session.add(_identity("sa-gone", deleted_at=datetime(2026, 3, 1, tzinfo=UTC)))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "sa-gone")

        assert len(results) == 1
        assert results[0].status == "deleted"

    def test_search_active_identity_has_status_active(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Active identity (deleted_at is None) → status='active'."""
        session.add(_identity("sa-alive"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "sa-alive")

        assert len(results) == 1
        assert results[0].status == "active"


class TestGraphSearchRepositoryRelevanceOrder:
    def test_search_relevance_exact_before_prefix_before_substring(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V4: exact match (score=0) < prefix (score=1) < substring (score=2)."""
        session.add(_resource("kafka", "kafka_cluster", display_name="exact match"))
        session.add(_resource("kafka-prod", "kafka_cluster", display_name="prefix match"))
        session.add(_resource("my-kafka-cluster", "kafka_cluster", display_name="substring match"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "kafka")

        ids = [r.id for r in results]
        assert ids.index("kafka") < ids.index("kafka-prod")
        assert ids.index("kafka-prod") < ids.index("my-kafka-cluster")


class TestGraphSearchRepositoryLimit:
    def test_search_limits_results_to_20(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V5: with 30 matching resources, response contains exactly 20 results."""
        for i in range(30):
            session.add(_resource(f"lkc-kafka-{i:02d}", "kafka_cluster"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "kafka")

        assert len(results) == 20


class TestGraphSearchRepositoryEmpty:
    def test_search_no_match_returns_empty_list(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V6: no matching entities → returns [], never raises."""
        session.add(_resource("lkc-abc", "kafka_cluster"))
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "xyznonexistent")

        assert results == []


class TestGraphSearchRepositoryTenantIsolation:
    def test_search_does_not_return_other_tenant_resources(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V19: resources belonging to a different tenant are not returned."""
        # Add resource for a different tenant
        other_tenant_resource = ResourceTable(
            ecosystem=ECOSYSTEM,
            tenant_id="other-tenant",
            resource_id="lkc-kafka-other",
            resource_type="kafka_cluster",
            display_name="kafka-other",
            parent_id=None,
            status="active",
            cloud=None,
            region=None,
            created_at=_CREATED,
            deleted_at=None,
        )
        session.add(other_tenant_resource)
        session.commit()

        results = repo.search_entities(ECOSYSTEM, TENANT_ID, "kafka")

        assert all(r.id != "lkc-kafka-other" for r in results)

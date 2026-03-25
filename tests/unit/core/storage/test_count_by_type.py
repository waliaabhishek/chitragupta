from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.counts import TypeStatusCounts
from core.models.identity import CoreIdentity, Identity
from core.models.resource import CoreResource, Resource, ResourceStatus
from core.storage.backends.sqlmodel.repositories import (
    SQLModelIdentityRepository,
    SQLModelResourceRepository,
)


@pytest.fixture
def session() -> Generator[Session]:
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose(close=True)


class TestResourceCountByType:
    def _make_resource(self, **overrides: Any) -> Resource:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            resource_id="r1",
            resource_type="kafka_cluster",
            status=ResourceStatus.ACTIVE,
            created_at=datetime(2026, 1, 10, tzinfo=UTC),
            metadata={},
        )
        defaults.update(overrides)
        return CoreResource(**defaults)

    def test_count_by_type_single_type(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        for i in range(3):
            repo.upsert(self._make_resource(resource_id=f"r{i}", resource_type="kafka_cluster"))
        session.commit()

        result = repo.count_by_type("eco", "t1")

        assert result == {"kafka_cluster": TypeStatusCounts(total=3, active=3, deleted=0)}

    def test_count_by_type_empty(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)

        result = repo.count_by_type("eco", "t1")

        assert result == {}

    def test_count_by_type_multiple_types(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="env-1", resource_type="environment"))
        repo.upsert(self._make_resource(resource_id="conn-1", resource_type="connector"))
        repo.upsert(self._make_resource(resource_id="conn-2", resource_type="connector"))
        repo.upsert(self._make_resource(resource_id="kafka-1", resource_type="kafka_cluster"))
        repo.upsert(self._make_resource(resource_id="kafka-2", resource_type="kafka_cluster"))
        repo.upsert(self._make_resource(resource_id="kafka-3", resource_type="kafka_cluster"))
        session.commit()

        result = repo.count_by_type("eco", "t1")

        assert result == {
            "environment": TypeStatusCounts(total=1, active=1, deleted=0),
            "connector": TypeStatusCounts(total=2, active=2, deleted=0),
            "kafka_cluster": TypeStatusCounts(total=3, active=3, deleted=0),
        }

    def test_count_by_type_tenant_isolation(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        # Tenant A: 2 resources
        repo.upsert(self._make_resource(tenant_id="t1", resource_id="r1", resource_type="kafka_cluster"))
        repo.upsert(self._make_resource(tenant_id="t1", resource_id="r2", resource_type="kafka_cluster"))
        # Tenant B: 5 resources
        for i in range(5):
            repo.upsert(self._make_resource(tenant_id="t2", resource_id=f"r{i}", resource_type="kafka_cluster"))
        session.commit()

        result_a = repo.count_by_type("eco", "t1")
        result_b = repo.count_by_type("eco", "t2")

        assert result_a == {"kafka_cluster": TypeStatusCounts(total=2, active=2, deleted=0)}
        assert result_b == {"kafka_cluster": TypeStatusCounts(total=5, active=5, deleted=0)}

    def test_count_by_type_mixed_status(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        for i in range(3):
            repo.upsert(
                self._make_resource(
                    resource_id=f"active-{i}", resource_type="kafka_cluster", status=ResourceStatus.ACTIVE
                )
            )
        for i in range(2):
            repo.upsert(
                self._make_resource(
                    resource_id=f"deleted-{i}", resource_type="kafka_cluster", status=ResourceStatus.DELETED
                )
            )
        session.commit()

        result = repo.count_by_type("eco", "t1")

        assert result == {"kafka_cluster": TypeStatusCounts(total=5, active=3, deleted=2)}


class TestIdentityCountByType:
    def _make_identity(self, **overrides: Any) -> Identity:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            identity_id="id1",
            identity_type="service_account",
            created_at=datetime(2026, 1, 10, tzinfo=UTC),
            metadata={},
        )
        defaults.update(overrides)
        return CoreIdentity(**defaults)

    def test_count_by_type_single_type(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        for i in range(2):
            repo.upsert(self._make_identity(identity_id=f"sa-{i}", identity_type="service_account"))
        session.commit()

        result = repo.count_by_type("eco", "t1")

        assert result == {"service_account": TypeStatusCounts(total=2, active=2, deleted=0)}

    def test_count_by_type_empty(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)

        result = repo.count_by_type("eco", "t1")

        assert result == {}

    def test_count_by_type_multiple_types(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity(identity_id="sa-1", identity_type="service_account"))
        repo.upsert(self._make_identity(identity_id="sa-2", identity_type="service_account"))
        repo.upsert(self._make_identity(identity_id="user-1", identity_type="user"))
        session.commit()

        result = repo.count_by_type("eco", "t1")

        assert result == {
            "service_account": TypeStatusCounts(total=2, active=2, deleted=0),
            "user": TypeStatusCounts(total=1, active=1, deleted=0),
        }

    def test_count_by_type_tenant_isolation(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity(tenant_id="t1", identity_id="sa-1", identity_type="service_account"))
        repo.upsert(self._make_identity(tenant_id="t2", identity_id="sa-1", identity_type="service_account"))
        repo.upsert(self._make_identity(tenant_id="t2", identity_id="sa-2", identity_type="service_account"))
        session.commit()

        result_a = repo.count_by_type("eco", "t1")
        result_b = repo.count_by_type("eco", "t2")

        assert result_a == {"service_account": TypeStatusCounts(total=1, active=1, deleted=0)}
        assert result_b == {"service_account": TypeStatusCounts(total=2, active=2, deleted=0)}

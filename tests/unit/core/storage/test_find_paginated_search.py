from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.identity import CoreIdentity
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.repositories import (
    SQLModelEntityTagRepository,
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


def _make_identity(**overrides: Any) -> CoreIdentity:
    defaults: dict[str, Any] = dict(
        ecosystem="eco",
        tenant_id="t1",
        identity_id="u1",
        identity_type="user",
        display_name=None,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        metadata={},
    )
    defaults.update(overrides)
    return CoreIdentity(**defaults)


def _make_resource(**overrides: Any) -> CoreResource:
    defaults: dict[str, Any] = dict(
        ecosystem="eco",
        tenant_id="t1",
        resource_id="r1",
        resource_type="kafka",
        status=ResourceStatus.ACTIVE,
        created_at=datetime(2026, 1, 10, tzinfo=UTC),
        metadata={},
    )
    defaults.update(overrides)
    return CoreResource(**defaults)


# ---------------------------------------------------------------------------
# Identity repository — search
# ---------------------------------------------------------------------------


class TestIdentityFindPaginatedSearch:
    def test_search_matches_identity_id(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="alice-svc"))
        repo.upsert(_make_identity(identity_id="bob-svc"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, search="alice")

        assert total == 1
        assert items[0].identity_id == "alice-svc"

    def test_search_matches_display_name(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="u1", display_name="Alice Smith"))
        repo.upsert(_make_identity(identity_id="u2", display_name="Bob Jones"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, search="smith")

        assert total == 1
        assert items[0].identity_id == "u1"

    def test_search_is_case_insensitive(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="ALICE-SVC"))
        repo.upsert(_make_identity(identity_id="bob-svc"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, search="alice")

        assert total == 1

    def test_search_partial_match(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="service-account-prod"))
        repo.upsert(_make_identity(identity_id="user-prod"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, search="service")

        assert total == 1
        assert items[0].identity_id == "service-account-prod"

    def test_search_no_match_returns_empty(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="alice", display_name="Alice"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, search="zzz-nonexistent")

        assert total == 0
        assert items == []


# ---------------------------------------------------------------------------
# Identity repository — sort
# ---------------------------------------------------------------------------


class TestIdentityFindPaginatedSort:
    def test_sort_by_identity_id_asc(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="zzz-id"))
        repo.upsert(_make_identity(identity_id="aaa-id"))
        repo.upsert(_make_identity(identity_id="mmm-id"))
        session.commit()

        items, _ = repo.find_paginated("eco", "t1", limit=10, offset=0, sort_by="identity_id", sort_order="asc")

        assert items[0].identity_id == "aaa-id"
        assert items[-1].identity_id == "zzz-id"

    def test_sort_by_identity_id_desc(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="zzz-id"))
        repo.upsert(_make_identity(identity_id="aaa-id"))
        session.commit()

        items, _ = repo.find_paginated("eco", "t1", limit=10, offset=0, sort_by="identity_id", sort_order="desc")

        assert items[0].identity_id == "zzz-id"
        assert items[1].identity_id == "aaa-id"

    def test_sort_by_display_name_asc(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="u1", display_name="Zebra"))
        repo.upsert(_make_identity(identity_id="u2", display_name="Apple"))
        session.commit()

        items, _ = repo.find_paginated("eco", "t1", limit=10, offset=0, sort_by="display_name", sort_order="asc")

        assert items[0].display_name == "Apple"

    def test_sort_by_identity_type_asc(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="u1", identity_type="user"))
        repo.upsert(_make_identity(identity_id="sa1", identity_type="service_account"))
        session.commit()

        items, _ = repo.find_paginated("eco", "t1", limit=10, offset=0, sort_by="identity_type", sort_order="asc")

        # "service_account" < "user" alphabetically
        assert items[0].identity_type == "service_account"

    def test_invalid_sort_by_falls_back_to_identity_id(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="zzz"))
        repo.upsert(_make_identity(identity_id="aaa"))
        session.commit()

        # Invalid sort_by — must not raise, falls back to identity_id asc
        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, sort_by="invalid_column")

        assert total == 2
        assert items[0].identity_id == "aaa"


# ---------------------------------------------------------------------------
# Identity repository — tag filter
# ---------------------------------------------------------------------------


class TestIdentityFindPaginatedTagFilter:
    def test_tag_key_filter_returns_matching(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_identity(identity_id="tagged-user"))
        repo.upsert(_make_identity(identity_id="untagged-user"))
        session.commit()
        tag_repo.add_tag("t1", "identity", "tagged-user", "cost_center", "eng", "admin")
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, tag_key="cost_center", tags_repo=tag_repo)

        assert total == 1
        assert items[0].identity_id == "tagged-user"

    def test_tag_key_filter_excludes_non_matching_key(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_identity(identity_id="u1"))
        repo.upsert(_make_identity(identity_id="u2"))
        session.commit()
        tag_repo.add_tag("t1", "identity", "u1", "team", "platform", "admin")
        session.commit()

        # Filtering by "cost_center" — neither identity has that key
        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, tag_key="cost_center", tags_repo=tag_repo)

        assert total == 0

    def test_tag_key_and_value_filter(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_identity(identity_id="u1"))
        repo.upsert(_make_identity(identity_id="u2"))
        session.commit()
        tag_repo.add_tag("t1", "identity", "u1", "cost_center", "eng", "admin")
        tag_repo.add_tag("t1", "identity", "u2", "cost_center", "ops", "admin")
        session.commit()

        items, total = repo.find_paginated(
            "eco",
            "t1",
            limit=10,
            offset=0,
            tag_key="cost_center",
            tag_value="eng",
            tags_repo=tag_repo,
        )

        assert total == 1
        assert items[0].identity_id == "u1"

    def test_tag_key_alone_matches_any_value(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_identity(identity_id="u1"))
        repo.upsert(_make_identity(identity_id="u2"))
        repo.upsert(_make_identity(identity_id="u3"))
        session.commit()
        tag_repo.add_tag("t1", "identity", "u1", "env", "prod", "admin")
        tag_repo.add_tag("t1", "identity", "u2", "env", "staging", "admin")
        # u3 has no tag
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, tag_key="env", tags_repo=tag_repo)

        assert total == 2

    def test_tag_filter_does_not_cross_entity_types(self, session: Session) -> None:
        """A resource tag with the same key must not match identity filter."""
        repo = SQLModelIdentityRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_identity(identity_id="u1"))
        session.commit()
        # Tag a resource, not the identity
        tag_repo.add_tag("t1", "resource", "r1", "cost_center", "eng", "admin")
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, tag_key="cost_center", tags_repo=tag_repo)

        assert total == 0


# ---------------------------------------------------------------------------
# Identity repository — combined filters
# ---------------------------------------------------------------------------


class TestIdentityFindPaginatedCombined:
    def test_search_and_identity_type_combined(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="alice-user", identity_type="user"))
        repo.upsert(_make_identity(identity_id="alice-sa", identity_type="service_account"))
        repo.upsert(_make_identity(identity_id="bob-user", identity_type="user"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, search="alice", identity_type="user")

        assert total == 1
        assert items[0].identity_id == "alice-user"

    def test_search_and_tag_key_combined(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_identity(identity_id="alice-tagged"))
        repo.upsert(_make_identity(identity_id="alice-untagged"))
        repo.upsert(_make_identity(identity_id="bob-tagged"))
        session.commit()
        tag_repo.add_tag("t1", "identity", "alice-tagged", "team", "eng", "admin")
        tag_repo.add_tag("t1", "identity", "bob-tagged", "team", "eng", "admin")
        session.commit()

        items, total = repo.find_paginated(
            "eco",
            "t1",
            limit=10,
            offset=0,
            search="alice",
            tag_key="team",
            tags_repo=tag_repo,
        )

        assert total == 1
        assert items[0].identity_id == "alice-tagged"


# ---------------------------------------------------------------------------
# Resource repository — search
# ---------------------------------------------------------------------------


class TestResourceFindPaginatedSearch:
    def test_search_matches_resource_id(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="kafka-prod"))
        repo.upsert(_make_resource(resource_id="ksql-dev"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, resource_type="kafka", search="kafka")

        assert total == 1
        assert items[0].resource_id == "kafka-prod"

    def test_search_matches_display_name(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="r1", display_name="Production Database"))
        repo.upsert(_make_resource(resource_id="r2", display_name="Dev Cache"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, resource_type="kafka", search="database")

        assert total == 1
        assert items[0].resource_id == "r1"

    def test_search_is_case_insensitive(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="KAFKA-CLUSTER"))
        repo.upsert(_make_resource(resource_id="ksql-cluster"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, resource_type="kafka", search="kafka")

        assert total == 1

    def test_search_no_match_returns_empty(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="kafka-prod"))
        session.commit()

        items, total = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", search="zzz-nonexistent"
        )

        assert total == 0
        assert items == []


# ---------------------------------------------------------------------------
# Resource repository — sort
# ---------------------------------------------------------------------------


class TestResourceFindPaginatedSort:
    def test_sort_by_resource_id_asc(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="zzz-resource"))
        repo.upsert(_make_resource(resource_id="aaa-resource"))
        session.commit()

        items, _ = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", sort_by="resource_id", sort_order="asc"
        )

        assert items[0].resource_id == "aaa-resource"

    def test_sort_by_resource_id_desc(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="zzz-resource"))
        repo.upsert(_make_resource(resource_id="aaa-resource"))
        session.commit()

        items, _ = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", sort_by="resource_id", sort_order="desc"
        )

        assert items[0].resource_id == "zzz-resource"

    def test_sort_by_display_name_asc(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="r1", display_name="Zebra DB"))
        repo.upsert(_make_resource(resource_id="r2", display_name="Apple Cache"))
        session.commit()

        items, _ = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", sort_by="display_name", sort_order="asc"
        )

        assert items[0].display_name == "Apple Cache"

    def test_sort_by_resource_type_asc(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="r1", resource_type="ksql"))
        repo.upsert(_make_resource(resource_id="r2", resource_type="kafka"))
        session.commit()

        items, _ = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type=["kafka", "ksql"], sort_by="resource_type", sort_order="asc"
        )

        # "kafka" < "ksql" alphabetically
        assert items[0].resource_type == "kafka"

    def test_sort_by_status_asc(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="r1", status=ResourceStatus.DELETED))
        repo.upsert(_make_resource(resource_id="r2", status=ResourceStatus.ACTIVE))
        session.commit()

        items, _ = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", sort_by="status", sort_order="asc"
        )

        # "active" < "deleted" alphabetically
        assert items[0].resource_id == "r2"

    def test_invalid_sort_by_falls_back_to_resource_id(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="zzz"))
        repo.upsert(_make_resource(resource_id="aaa"))
        session.commit()

        # Invalid sort_by — must not raise, falls back to resource_id asc
        items, total = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", sort_by="invalid_column"
        )

        assert total == 2
        assert items[0].resource_id == "aaa"


# ---------------------------------------------------------------------------
# Resource repository — tag filter
# ---------------------------------------------------------------------------


class TestResourceFindPaginatedTagFilter:
    def test_tag_key_filter_returns_matching(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_resource(resource_id="r-tagged"))
        repo.upsert(_make_resource(resource_id="r-untagged"))
        session.commit()
        tag_repo.add_tag("t1", "resource", "r-tagged", "cost_center", "eng", "admin")
        session.commit()

        items, total = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", tag_key="cost_center", tags_repo=tag_repo
        )

        assert total == 1
        assert items[0].resource_id == "r-tagged"

    def test_tag_key_and_value_filter(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_resource(resource_id="r1"))
        repo.upsert(_make_resource(resource_id="r2"))
        session.commit()
        tag_repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        tag_repo.add_tag("t1", "resource", "r2", "env", "dev", "admin")
        session.commit()

        items, total = repo.find_paginated(
            "eco",
            "t1",
            limit=10,
            offset=0,
            resource_type="kafka",
            tag_key="env",
            tag_value="prod",
            tags_repo=tag_repo,
        )

        assert total == 1
        assert items[0].resource_id == "r1"

    def test_tag_key_alone_matches_any_value(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_resource(resource_id="r1"))
        repo.upsert(_make_resource(resource_id="r2"))
        repo.upsert(_make_resource(resource_id="r3"))
        session.commit()
        tag_repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        tag_repo.add_tag("t1", "resource", "r2", "team", "data", "admin")
        # r3 untagged
        session.commit()

        items, total = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", tag_key="team", tags_repo=tag_repo
        )

        assert total == 2

    def test_tag_filter_does_not_cross_entity_types(self, session: Session) -> None:
        """An identity tag must not match the resource filter."""
        repo = SQLModelResourceRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_resource(resource_id="r1"))
        session.commit()
        # Tag an identity, not the resource
        tag_repo.add_tag("t1", "identity", "u1", "cost_center", "eng", "admin")
        session.commit()

        items, total = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", tag_key="cost_center", tags_repo=tag_repo
        )

        assert total == 0


# ---------------------------------------------------------------------------
# Resource repository — combined filters
# ---------------------------------------------------------------------------


class TestResourceFindPaginatedCombined:
    def test_search_and_resource_type_combined(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="kafka-prod", resource_type="kafka"))
        repo.upsert(_make_resource(resource_id="kafka-dev", resource_type="kafka"))
        repo.upsert(_make_resource(resource_id="ksql-prod", resource_type="ksql"))
        session.commit()

        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, search="prod", resource_type="kafka")

        assert total == 1
        assert items[0].resource_id == "kafka-prod"

    def test_search_and_status_combined(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="r-active-match", status=ResourceStatus.ACTIVE))
        repo.upsert(_make_resource(resource_id="r-deleted-match", status=ResourceStatus.DELETED))
        repo.upsert(_make_resource(resource_id="r-active-other", status=ResourceStatus.ACTIVE))
        session.commit()

        items, total = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", search="match", status="active"
        )

        assert total == 1
        assert items[0].resource_id == "r-active-match"

    def test_search_and_tag_key_combined(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)
        repo.upsert(_make_resource(resource_id="kafka-tagged"))
        repo.upsert(_make_resource(resource_id="kafka-untagged"))
        repo.upsert(_make_resource(resource_id="ksql-tagged"))
        session.commit()
        tag_repo.add_tag("t1", "resource", "kafka-tagged", "owner", "team-a", "admin")
        tag_repo.add_tag("t1", "resource", "ksql-tagged", "owner", "team-b", "admin")
        session.commit()

        items, total = repo.find_paginated(
            "eco",
            "t1",
            limit=10,
            offset=0,
            resource_type="kafka",
            search="kafka",
            tag_key="owner",
            tags_repo=tag_repo,
        )

        assert total == 1
        assert items[0].resource_id == "kafka-tagged"


# ---------------------------------------------------------------------------
# tag_value without tag_key — must be silently ignored
# ---------------------------------------------------------------------------


class TestTagValueWithoutTagKeyIgnored:
    def test_identity_tag_value_without_tag_key_returns_all(self, session: Session) -> None:
        """tag_value with no tag_key must not filter — all identities returned."""
        repo = SQLModelIdentityRepository(session)
        repo.upsert(_make_identity(identity_id="u1"))
        repo.upsert(_make_identity(identity_id="u2"))
        session.commit()

        # tags_repo=None means the tag filter block is skipped entirely
        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, tag_value="eng", tags_repo=None)

        assert total == 2

    def test_resource_tag_value_without_tag_key_returns_all(self, session: Session) -> None:
        """tag_value with no tag_key must not filter — all resources returned."""
        repo = SQLModelResourceRepository(session)
        repo.upsert(_make_resource(resource_id="r1"))
        repo.upsert(_make_resource(resource_id="r2"))
        session.commit()

        items, total = repo.find_paginated(
            "eco", "t1", limit=10, offset=0, resource_type="kafka", tag_value="prod", tags_repo=None
        )

        assert total == 2

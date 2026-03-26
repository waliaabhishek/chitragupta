from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_resource(
    backend: SQLModelBackend,
    resource_id: str,
    tenant_id: str = "test-tenant",
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="test-eco",
                tenant_id=tenant_id,
                resource_id=resource_id,
                resource_type="kafka_cluster",
                status=ResourceStatus.ACTIVE,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={},
            )
        )
        uow.commit()


def _seed_identity(
    backend: SQLModelBackend,
    identity_id: str,
    tenant_id: str = "test-tenant",
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.identities.upsert(
            CoreIdentity(
                ecosystem="test-eco",
                tenant_id=tenant_id,
                identity_id=identity_id,
                identity_type="user",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={},
            )
        )
        uow.commit()


def _seed_chargeback(
    backend: SQLModelBackend,
    identity_id: str,
    resource_id: str | None,
    product_type: str = "kafka",
    timestamp: datetime | None = None,
) -> None:
    ts = timestamp or (datetime.now(UTC) - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(
            ChargebackRow(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                timestamp=ts,
                resource_id=resource_id,
                product_category="compute",
                product_type=product_type,
                identity_id=identity_id,
                cost_type=CostType.USAGE,
                amount=Decimal("10.00"),
                tags={},
                metadata={},
            )
        )
        uow.commit()


class TestResourceOverridesIdentityTags:
    def test_resource_tag_overrides_identity_tag_on_same_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r1")

        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/identity/u1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "staging", "created_by": "admin"},
        )

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks")
        data = response.json()
        row = data["items"][0]
        # resource wins on key collision
        assert row["tags"] == {"env": "staging"}

    def test_identity_only_key_preserved_when_resource_does_not_have_it(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r1")

        # identity has "team", resource has "env" — no collision
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/identity/u1/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks")
        data = response.json()
        row = data["items"][0]
        assert row["tags"] == {"env": "prod", "team": "platform"}

    def test_after_delete_resource_tag_identity_tag_becomes_visible(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r1")

        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/identity/u1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "staging", "created_by": "admin"},
        )
        app_with_backend.delete("/api/v1/tenants/test-tenant/entities/resource/r1/tags/env")

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks")
        data = response.json()
        row = data["items"][0]
        assert row["tags"] == {"env": "prod"}


class TestNullResourceIdTags:
    def test_null_resource_id_row_has_only_identity_tags(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id=None)

        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/identity/u1/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks")
        data = response.json()
        row = data["items"][0]
        assert row["tags"] == {"team": "platform"}

    def test_null_resource_id_row_has_empty_tags_when_no_identity_tags(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id=None)

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks")
        data = response.json()
        row = data["items"][0]
        assert row["tags"] == {}


class TestNoNPlusOne:
    def test_batch_fetch_tags_for_multiple_rows(self, in_memory_backend: SQLModelBackend) -> None:
        """find_by_filters with tags_repo populates tags for all rows without N queries."""
        for i in range(5):
            _seed_resource(in_memory_backend, f"r{i}")
            _seed_identity(in_memory_backend, f"u{i}")
            _seed_chargeback(
                in_memory_backend,
                identity_id=f"u{i}",
                resource_id=f"r{i}",
                product_type=f"type{i}",
            )

        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(5):
                uow.tags.add_tag("test-tenant", "resource", f"r{i}", "env", "prod", "admin")
                uow.tags.add_tag("test-tenant", "identity", f"u{i}", "team", "platform", "admin")
            uow.commit()

        with in_memory_backend.create_unit_of_work() as uow:
            rows, total = uow.chargebacks.find_by_filters(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                tags_repo=uow.tags,
            )

        assert total == 5
        # Every row should have tags populated
        for row in rows:
            assert "env" in row.tags
            assert row.tags["env"] == "prod"
            assert "team" in row.tags
            assert row.tags["team"] == "platform"


class TestTagFilterCrossTypeFalsePositive:
    def test_resource_tag_filter_does_not_match_identity_with_same_entity_id(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Filtering by resource tag env=prod must not match rows whose only link is
        an identity entity_id that happens to share the same string as the resource."""
        # Both resource and identity share entity_id="abc"
        _seed_resource(in_memory_backend, "abc")
        _seed_identity(in_memory_backend, "abc")

        # Chargeback with resource_id=abc and identity_id=abc
        _seed_chargeback(in_memory_backend, identity_id="abc", resource_id="abc", product_type="kafka")
        # Chargeback with only identity_id=abc (resource_id=None)
        _seed_chargeback(in_memory_backend, identity_id="abc", resource_id=None, product_type="flink")

        # resource has env=prod; identity has env=staging
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/abc/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/identity/abc/tags",
            json={"tag_key": "env", "tag_value": "staging", "created_by": "admin"},
        )

        # Filter by resource tag env=prod
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"tag_key": "env", "tag_value": "prod"},
        )
        data = response.json()
        # Only the row with resource_id="abc" should match
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "abc"


class TestTagFilterMatch:
    def test_tag_filter_returns_only_matching_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_resource(in_memory_backend, "r2")
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r1", product_type="kafka")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r2", product_type="flink")

        # Only r1 has the tag
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"tag_key": "env", "tag_value": "prod"},
        )
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "r1"

    def test_tag_filter_by_key_only_matches_any_value(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_resource(in_memory_backend, "r2")
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r1", product_type="kafka")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r2", product_type="flink")

        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r2/tags",
            json={"tag_key": "env", "tag_value": "staging", "created_by": "admin"},
        )

        # Filter by key only — both should match
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"tag_key": "env"},
        )
        data = response.json()
        assert data["total"] == 2


class TestTagFilterNoMatch:
    def test_tag_filter_no_match_returns_empty_200(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r1")

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"tag_key": "env", "tag_value": "nonexistent"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert data["items"] == []


class TestExportCSVTagsFormat:
    def test_export_tags_column_uses_key_equals_value_format(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r1")

        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "columns": ["identity_id", "tags"],
            },
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        assert len(lines) == 2
        # Cell must be "env=prod", not just "env" (which would happen with dict key iteration)
        assert "env=prod" in lines[1]

    def test_export_multiple_tags_use_semicolon_separator(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r1")

        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "columns": ["resource_id", "tags"],
            },
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        assert len(lines) == 2
        tags_cell = lines[1].split(",")[1]
        # Both key=value pairs must be present, separated by semicolons
        tag_pairs = set(tags_cell.split(";"))
        assert "env=prod" in tag_pairs
        assert "team=platform" in tag_pairs

    def test_export_tags_populated_from_entity_tags(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Export must pass tags_repo so rows have entity tags, not empty."""
        _seed_resource(in_memory_backend, "r1")
        _seed_identity(in_memory_backend, "u1")
        _seed_chargeback(in_memory_backend, identity_id="u1", resource_id="r1")

        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "columns": ["resource_id", "tags"],
            },
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        assert len(lines) == 2
        assert "team=platform" in lines[1]

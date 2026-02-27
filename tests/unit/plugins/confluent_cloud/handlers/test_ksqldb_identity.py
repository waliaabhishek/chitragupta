"""Tests for ksqlDB identity resolution helper."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from core.models import Identity, Resource


class TestResolveKsqldbIdentity:
    """Tests for resolve_ksqldb_identity function."""

    def test_resource_found_owner_found_returns_owner(self, mock_uow: MagicMock) -> None:
        """Resource with owner_id resolves to the owner identity."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-owner-123"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-123",
            identity_type="service_account",
            display_name="My SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
        mock_uow.identities.find_by_period.return_value = ([sa_owner], 1)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-owner-123" in result.resource_active.ids()
        owner = result.resource_active.get("sa-owner-123")
        assert owner is not None
        assert owner.display_name == "My SA"

    def test_resource_not_found_returns_deleted_sentinel(self, mock_uow: MagicMock) -> None:
        """Missing ksqlDB resource creates ksqldb_deleted sentinel."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            KSQLDB_DELETED_SENTINEL,
            resolve_ksqldb_identity,
        )

        mock_uow.resources.find_by_period.return_value = ([], 0)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert KSQLDB_DELETED_SENTINEL in result.resource_active.ids()
        sentinel = result.resource_active.get(KSQLDB_DELETED_SENTINEL)
        assert sentinel is not None
        assert sentinel.identity_type == "ksqldb_credentials"
        assert sentinel.display_name == "ksqlDB Deleted When Calculation Started"

    def test_resource_found_owner_id_missing_returns_unknown_sentinel(self, mock_uow: MagicMock) -> None:
        """Resource without owner_id creates ksqldb_owner_unknown sentinel."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={},  # No owner_id
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "ksqldb_owner_unknown" in result.resource_active.ids()
        sentinel = result.resource_active.get("ksqldb_owner_unknown")
        assert sentinel is not None
        assert sentinel.identity_type == "ksqldb_credentials"
        assert sentinel.display_name == "ksqlDB Owner Unknown"

    def test_resource_found_owner_not_in_identities_creates_sentinel_from_id(self, mock_uow: MagicMock) -> None:
        """Owner not in identities DB creates sentinel from owner_id."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-unknown-999"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)  # Owner not in DB

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-unknown-999" in result.resource_active.ids()
        sentinel = result.resource_active.get("sa-unknown-999")
        assert sentinel is not None
        assert sentinel.identity_type == "service_account"
        assert sentinel.display_name == "Unknown service_account"

    def test_sentinel_type_service_account(self, mock_uow: MagicMock) -> None:
        """Owner ID with sa- prefix creates service_account sentinel."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-12345"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        sentinel = result.resource_active.get("sa-12345")
        assert sentinel is not None
        assert sentinel.identity_type == "service_account"

    def test_sentinel_type_user(self, mock_uow: MagicMock) -> None:
        """Owner ID with u- prefix creates user sentinel."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "u-user-456"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        sentinel = result.resource_active.get("u-user-456")
        assert sentinel is not None
        assert sentinel.identity_type == "user"

    def test_sentinel_type_identity_pool(self, mock_uow: MagicMock) -> None:
        """Owner ID with pool- prefix creates identity_pool sentinel."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "pool-abc-789"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        sentinel = result.resource_active.get("pool-abc-789")
        assert sentinel is not None
        assert sentinel.identity_type == "identity_pool"

    def test_sentinel_type_unknown(self, mock_uow: MagicMock) -> None:
        """Owner ID with unrecognized prefix creates unknown sentinel."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "xyz-mystery-123"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        sentinel = result.resource_active.get("xyz-mystery-123")
        assert sentinel is not None
        assert sentinel.identity_type == "unknown"

    def test_tenant_period_and_metrics_derived_are_empty(self, mock_uow: MagicMock) -> None:
        """tenant_period and metrics_derived are returned empty."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-owner-123"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-123",
            identity_type="service_account",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
        mock_uow.identities.find_by_period.return_value = ([sa_owner], 1)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.tenant_period) == 0
        assert len(result.metrics_derived) == 0

    def test_filters_to_correct_ksqldb_resource(self, mock_uow: MagicMock) -> None:
        """Only the matching ksqlDB resource_id is used."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_match = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-correct"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        ksqldb_other = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-OTHER",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-wrong"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_correct = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-correct",
            identity_type="service_account",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = (
            [ksqldb_match, ksqldb_other],
            2,
        )
        mock_uow.identities.find_by_period.return_value = ([sa_correct], 1)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-correct" in result.resource_active.ids()
        assert "sa-wrong" not in result.resource_active.ids()

    def test_user_owner_found_returns_user_identity(self, mock_uow: MagicMock) -> None:
        """Resource with user owner_id resolves to the user identity."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "u-user-789"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        user_owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="u-user-789",
            identity_type="user",
            display_name="Human User",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
        mock_uow.identities.find_by_period.return_value = ([user_owner], 1)

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "u-user-789" in result.resource_active.ids()
        owner = result.resource_active.get("u-user-789")
        assert owner is not None
        assert owner.display_name == "Human User"
        assert owner.identity_type == "user"

"""Tests for ksqlDB identity resolution helper."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from core.models import CoreIdentity, CoreResource

# ---------------------------------------------------------------------------
# TASK-028 — Direct lookup tests appended after existing tests (see below)
# ---------------------------------------------------------------------------


class TestResolveKsqldbIdentity:
    """Tests for resolve_ksqldb_identity function."""

    def test_resource_found_owner_found_returns_owner(self, mock_uow: MagicMock) -> None:
        """Resource with owner_id resolves to the owner identity."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-owner-123"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-123",
            identity_type="service_account",
            display_name="My SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

        mock_uow.identities.get.return_value = sa_owner

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

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={},  # No owner_id
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

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

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-unknown-999"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

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

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-12345"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

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

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "u-user-456"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

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

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "pool-abc-789"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

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

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "xyz-mystery-123"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

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

        CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-owner-123"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-123",
            identity_type="service_account",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

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

        ksqldb_match = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-correct"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-OTHER",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-wrong"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_correct = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-correct",
            identity_type="service_account",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_match

        mock_uow.identities.get.return_value = sa_correct

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

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "u-user-789"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        user_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="u-user-789",
            identity_type="user",
            display_name="Human User",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

        mock_uow.identities.get.return_value = user_owner

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

    # --- GAP-15 tests: owner_id direct field (not metadata) ---

    def test_resource_found_owner_id_direct_field_resolves_owner(self, mock_uow: MagicMock) -> None:
        """owner_id set on Resource.owner_id field (not metadata) resolves correctly."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            owner_id="sa-direct-456",  # set on direct field, NOT in metadata
            metadata={},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-direct-456",
            identity_type="service_account",
            display_name="Direct Field SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

        mock_uow.identities.get.return_value = sa_owner

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-direct-456" in result.resource_active.ids()
        owner = result.resource_active.get("sa-direct-456")
        assert owner is not None
        assert owner.display_name == "Direct Field SA"
        assert owner.identity_type == "service_account"

    def test_resource_direct_owner_id_takes_precedence_over_metadata(self, mock_uow: MagicMock) -> None:
        """Resource.owner_id direct field takes precedence over metadata['owner_id']."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            owner_id="sa-direct-primary",  # direct field — should win
            metadata={"owner_id": "sa-metadata-secondary"},  # metadata — should lose
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_primary = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-direct-primary",
            identity_type="service_account",
            display_name="Primary SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-metadata-secondary",
            identity_type="service_account",
            display_name="Secondary SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

        mock_uow.identities.get.return_value = sa_primary

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-direct-primary" in result.resource_active.ids()
        assert "sa-metadata-secondary" not in result.resource_active.ids()

    def test_resource_direct_owner_id_none_falls_back_to_metadata(self, mock_uow: MagicMock) -> None:
        """When Resource.owner_id is None, metadata['owner_id'] is used as fallback."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            owner_id=None,  # no direct field
            metadata={"owner_id": "sa-meta-fallback"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-meta-fallback",
            identity_type="service_account",
            display_name="Metadata Fallback SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

        mock_uow.identities.get.return_value = sa_owner

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-meta-fallback" in result.resource_active.ids()
        owner = result.resource_active.get("sa-meta-fallback")
        assert owner is not None
        assert owner.display_name == "Metadata Fallback SA"

    def test_resource_both_owner_id_fields_absent_returns_unknown_sentinel(self, mock_uow: MagicMock) -> None:
        """When both Resource.owner_id and metadata['owner_id'] are absent, unknown sentinel returned."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            owner_id=None,  # no direct field
            metadata={},  # no metadata owner_id either
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

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

    # --- GAP-15 verification tests (named per design doc) ---

    def test_toplevel_owner_id_resolves_to_identity(self, mock_uow: MagicMock) -> None:
        """Resource with top-level owner_id (no metadata owner_id) resolves correctly."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            owner_id="sa-12345",  # TOP-LEVEL field only
            metadata={"kafka_cluster_id": "lkc-xxx", "csu_count": 4},  # NO owner_id in metadata
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-12345",
            identity_type="service_account",
            display_name="Top-Level SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

        mock_uow.identities.get.return_value = sa_owner

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-12345" in result.resource_active.ids()
        owner = result.resource_active.get("sa-12345")
        assert owner is not None
        assert owner.display_name == "Top-Level SA"
        assert owner.identity_type == "service_account"

    def test_metadata_owner_id_fallback(self, mock_uow: MagicMock) -> None:
        """Resource with owner_id=None falls back to metadata owner_id."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            resolve_ksqldb_identity,
        )

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            owner_id=None,  # top-level is None
            metadata={"owner_id": "sa-67890"},  # fallback in metadata
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-67890",
            identity_type="service_account",
            display_name="Metadata Fallback",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

        mock_uow.identities.get.return_value = sa_owner

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-67890" in result.resource_active.ids()
        owner = result.resource_active.get("sa-67890")
        assert owner is not None
        assert owner.display_name == "Metadata Fallback"

    def test_no_owner_anywhere_returns_unknown_sentinel(self, mock_uow: MagicMock) -> None:
        """Resource with no owner_id anywhere falls through to unknown sentinel."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            KSQLDB_OWNER_UNKNOWN,
            resolve_ksqldb_identity,
        )

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            owner_id=None,
            metadata={},  # no owner_id
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = ksqldb_app

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert KSQLDB_OWNER_UNKNOWN in result.resource_active.ids()
        sentinel = result.resource_active.get(KSQLDB_OWNER_UNKNOWN)
        assert sentinel is not None
        assert sentinel.identity_type == "ksqldb_credentials"
        assert sentinel.display_name == "ksqlDB Owner Unknown"


# ---------------------------------------------------------------------------
# TASK-028 — Direct lookup tests (TDD RED phase)
# These tests verify that the fixed code uses uow.resources.get() and
# uow.identities.get() instead of full-table find_by_period() scans.
# ---------------------------------------------------------------------------


class TestKsqldbIdentityDirectLookup:
    """GAP-028: ksqldb resolve uses targeted get() calls, never full-table scans."""

    # --- Method-usage assertions ---

    def test_resource_lookup_uses_get_not_find_by_period(self, mock_uow: MagicMock) -> None:
        """resolve_ksqldb_identity calls uow.resources.get() exactly once, never find_by_period."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import resolve_ksqldb_identity

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-owner-123"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.get.return_value = ksqldb_app

        resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.get.assert_called_once_with(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
        )
        mock_uow.resources.find_by_period.assert_not_called()

    def test_identity_lookup_uses_get_not_find_by_period(self, mock_uow: MagicMock) -> None:
        """Owner resolved via uow.identities.get(), never find_by_period."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import resolve_ksqldb_identity

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-owner-123"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-123",
            identity_type="service_account",
            display_name="My SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.get.return_value = ksqldb_app
        mock_uow.identities.get.return_value = sa_owner

        resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.identities.get.assert_called_once_with(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-123",
        )
        mock_uow.identities.find_by_period.assert_not_called()

    def test_identity_lookup_exactly_one_get_call(self, mock_uow: MagicMock) -> None:
        """resolve_ksqldb_identity makes exactly one uow.identities.get() call for the owner."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import resolve_ksqldb_identity

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "u-user-789"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        user_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="u-user-789",
            identity_type="user",
            display_name="Human User",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.get.return_value = ksqldb_app
        mock_uow.identities.get.return_value = user_owner

        resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert mock_uow.identities.get.call_count == 1

    # --- Behavioral parity tests (new mock setup via get()) ---

    def test_deleted_sentinel_parity_resource_get_returns_none(self, mock_uow: MagicMock) -> None:
        """get(resource) returns None → deleted sentinel; get() called, find_by_period not called."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            KSQLDB_DELETED_SENTINEL,
            resolve_ksqldb_identity,
        )

        mock_uow.resources.get.return_value = None  # resource not found

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.get.assert_called_once_with(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
        )
        mock_uow.resources.find_by_period.assert_not_called()
        assert KSQLDB_DELETED_SENTINEL in result.resource_active.ids()
        sentinel = result.resource_active.get(KSQLDB_DELETED_SENTINEL)
        assert sentinel is not None
        assert sentinel.identity_type == "ksqldb_credentials"

    def test_owner_unknown_parity_no_owner_id(self, mock_uow: MagicMock) -> None:
        """Resource found but no owner_id → ksqldb_owner_unknown sentinel; no find_by_period."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import (
            KSQLDB_OWNER_UNKNOWN,
            resolve_ksqldb_identity,
        )

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            owner_id=None,
            metadata={},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.get.return_value = ksqldb_app

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.find_by_period.assert_not_called()
        mock_uow.identities.find_by_period.assert_not_called()
        assert KSQLDB_OWNER_UNKNOWN in result.resource_active.ids()

    def test_happy_path_parity_using_get(self, mock_uow: MagicMock) -> None:
        """Happy path: resource.get + identity.get → owner resolved; no find_by_period calls."""
        from plugins.confluent_cloud.handlers.ksqldb_identity import resolve_ksqldb_identity

        ksqldb_app = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            resource_type="ksqldb",
            metadata={"owner_id": "sa-owner-123"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-123",
            identity_type="service_account",
            display_name="My SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.get.return_value = ksqldb_app
        mock_uow.identities.get.return_value = sa_owner

        result = resolve_ksqldb_identity(
            tenant_id="org-123",
            resource_id="ksqldb-app-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.find_by_period.assert_not_called()
        mock_uow.identities.find_by_period.assert_not_called()
        assert len(result.resource_active) == 1
        assert "sa-owner-123" in result.resource_active.ids()
        assert result.resource_active.get("sa-owner-123").display_name == "My SA"

"""Tests for connector identity resolution helper."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from core.models import Identity, Resource


class TestResolveConnectorIdentity:
    """Tests for resolve_connector_identity function."""

    def test_service_account_mode_resolves_owner(self, mock_uow: MagicMock) -> None:
        """SERVICE_ACCOUNT auth mode resolves to the kafka_service_account_id owner."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "SERVICE_ACCOUNT",
                "kafka_service_account_id": "sa-owner-123",
            },
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

        mock_uow.resources.get.return_value = connector

        mock_uow.identities.get.return_value = sa_owner

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
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

    def test_kafka_api_key_mode_resolves_api_key_owner(self, mock_uow: MagicMock) -> None:
        """KAFKA_API_KEY auth mode looks up API key and resolves its owner."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "api-key-xyz",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        api_key = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-xyz",
            identity_type="api_key",
            metadata={"owner_id": "u-user-456"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        user_owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="u-user-456",
            identity_type="user",
            display_name="Human User",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        identity_map = {i.identity_id: i for i in [api_key, user_owner]}
        mock_uow.identities.get.side_effect = lambda ecosystem, tenant_id, identity_id: identity_map.get(identity_id)

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "u-user-456" in result.resource_active.ids()

    def test_unknown_mode_creates_sentinel(self, mock_uow: MagicMock) -> None:
        """UNKNOWN auth mode creates per-connector sentinel using connector_id."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={"kafka_auth_mode": "UNKNOWN"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector-abc" in result.resource_active.ids()
        assert "connector_credentials_unknown" not in result.resource_active.ids()
        sentinel = result.resource_active.get("connector-abc")
        assert sentinel is not None
        assert sentinel.identity_type == "connector_credentials"

    def test_resource_not_found_creates_masked_sentinel(self, mock_uow: MagicMock) -> None:
        """Missing connector resource creates connector_credentials_masked sentinel."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector_credentials_masked" in result.resource_active.ids()
        sentinel = result.resource_active.get("connector_credentials_masked")
        assert sentinel is not None
        assert sentinel.identity_type == "connector_credentials"

    def test_service_account_mode_owner_not_in_db_creates_sentinel(self, mock_uow: MagicMock) -> None:
        """SERVICE_ACCOUNT mode with owner not in DB creates sentinel identity."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "SERVICE_ACCOUNT",
                "kafka_service_account_id": "sa-unknown-999",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
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

    def test_kafka_api_key_mode_api_key_not_in_db_creates_not_found_sentinel(self, mock_uow: MagicMock) -> None:
        """KAFKA_API_KEY mode with API key not found creates connector_api_key_not_found sentinel."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "api-key-missing",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        # API key found in metadata but not in DB → distinct not-found sentinel
        assert len(result.resource_active) == 1
        assert "connector_api_key_not_found" in result.resource_active.ids()
        assert "connector_credentials_unknown" not in result.resource_active.ids()

    def test_kafka_api_key_mode_api_key_missing_owner_creates_unknown_sentinel(self, mock_uow: MagicMock) -> None:
        """KAFKA_API_KEY mode with API key lacking owner_id creates unknown sentinel."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "api-key-xyz",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        api_key = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-xyz",
            identity_type="api_key",
            metadata={},  # No owner_id
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        mock_uow.identities.get.return_value = api_key

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector_credentials_unknown" in result.resource_active.ids()

    def test_missing_auth_mode_creates_unknown_sentinel(self, mock_uow: MagicMock) -> None:
        """Missing kafka_auth_mode in metadata uses connector_id as per-connector sentinel."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={},  # No kafka_auth_mode
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector-abc" in result.resource_active.ids()
        assert "connector_credentials_unknown" not in result.resource_active.ids()

    def test_service_account_mode_missing_sa_id_creates_unknown_sentinel(self, mock_uow: MagicMock) -> None:
        """SERVICE_ACCOUNT mode without kafka_service_account_id creates unknown sentinel."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={"kafka_auth_mode": "SERVICE_ACCOUNT"},  # No kafka_service_account_id
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector_credentials_unknown" in result.resource_active.ids()

    def test_kafka_api_key_mode_missing_api_key_in_metadata_creates_unknown_sentinel(self, mock_uow: MagicMock) -> None:
        """KAFKA_API_KEY mode without kafka_api_key in metadata creates unknown sentinel."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={"kafka_auth_mode": "KAFKA_API_KEY"},  # No kafka_api_key
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector_credentials_unknown" in result.resource_active.ids()

    def test_tenant_period_and_metrics_derived_are_empty(self, mock_uow: MagicMock) -> None:
        """tenant_period and metrics_derived are returned empty."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "SERVICE_ACCOUNT",
                "kafka_service_account_id": "sa-owner-123",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-123",
            identity_type="service_account",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.tenant_period) == 0
        assert len(result.metrics_derived) == 0

    def test_filters_to_correct_connector_resource(self, mock_uow: MagicMock) -> None:
        """Only the matching connector resource_id is used."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector_match = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "SERVICE_ACCOUNT",
                "kafka_service_account_id": "sa-correct",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-OTHER",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "SERVICE_ACCOUNT",
                "kafka_service_account_id": "sa-wrong",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_correct = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-correct",
            identity_type="service_account",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector_match

        mock_uow.identities.get.return_value = sa_correct

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-correct" in result.resource_active.ids()
        assert "sa-wrong" not in result.resource_active.ids()

    def test_kafka_api_key_mode_masked_key_creates_masked_sentinel(self, mock_uow: MagicMock) -> None:
        """Masked API key (all asterisks) → identity is connector_api_key_masked, not connector_credentials_unknown."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "****",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector_api_key_masked" in result.resource_active.ids()
        assert "connector_credentials_unknown" not in result.resource_active.ids()

    def test_kafka_api_key_mode_empty_key_creates_masked_sentinel(self, mock_uow: MagicMock) -> None:
        """Empty-string API key → identity is connector_api_key_masked (all() on empty iter = True)."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector_api_key_masked" in result.resource_active.ids()
        assert "connector_credentials_unknown" not in result.resource_active.ids()

    def test_kafka_api_key_mode_key_not_in_db_creates_not_found_sentinel(self, mock_uow: MagicMock) -> None:
        """API key present in metadata but not found in DB → identity is connector_api_key_not_found."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "api-key-real-but-missing",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector_api_key_not_found" in result.resource_active.ids()
        assert "connector_credentials_unknown" not in result.resource_active.ids()

    def test_unknown_auth_mode_uses_connector_id_as_identity(self, mock_uow: MagicMock) -> None:
        """UNKNOWN auth mode → identity uses connector.resource_id, not shared connector_credentials_unknown."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={"kafka_auth_mode": "UNKNOWN"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        mock_uow.resources.get.return_value = connector

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "connector-abc" in result.resource_active.ids()
        assert "connector_credentials_unknown" not in result.resource_active.ids()


class TestCreateSentinelFromId:
    """Tests for create_sentinel_from_id function."""

    def test_sa_prefix_service_account(self) -> None:
        """sa- prefix creates service_account type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("sa-123", "org", "confluent_cloud")
        assert result.identity_type == "service_account"
        assert result.identity_id == "sa-123"
        assert result.display_name == "Unknown service_account"

    def test_u_prefix_user(self) -> None:
        """u- prefix creates user type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("u-456", "org", "confluent_cloud")
        assert result.identity_type == "user"
        assert result.display_name == "Unknown user"

    def test_pool_prefix_identity_pool(self) -> None:
        """pool- prefix creates identity_pool type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("pool-789", "org", "confluent_cloud")
        assert result.identity_type == "identity_pool"

    def test_unknown_prefix(self) -> None:
        """Unknown prefix creates unknown type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("xyz-123", "org", "confluent_cloud")
        assert result.identity_type == "unknown"

    def test_no_dash_unknown_type(self) -> None:
        """ID without dash creates unknown type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("nodash", "org", "confluent_cloud")
        assert result.identity_type == "unknown"


class TestCreateConnectorSentinel:
    """Tests for create_connector_sentinel function."""

    def test_creates_connector_credentials_type_unknown(self) -> None:
        """Creates identity with connector_credentials type for unknown."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_connector_sentinel,
        )

        result = create_connector_sentinel(
            "connector_credentials_unknown", "org-123", "confluent_cloud", is_masked=False
        )
        assert result.identity_type == "connector_credentials"
        assert result.identity_id == "connector_credentials_unknown"
        assert result.ecosystem == "confluent_cloud"
        assert result.tenant_id == "org-123"
        assert result.display_name == "Connector Credentials Unknown"

    def test_creates_masked_sentinel(self) -> None:
        """Creates masked sentinel identity."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_connector_sentinel,
        )

        result = create_connector_sentinel("connector_credentials_masked", "org-123", "confluent_cloud", is_masked=True)
        assert result.identity_type == "connector_credentials"
        assert result.identity_id == "connector_credentials_masked"
        assert result.display_name == "Connector Credentials Masked"


# ---------------------------------------------------------------------------
# TASK-028 — Direct lookup tests (TDD RED phase)
# These tests verify that the fixed code uses uow.resources.get() and
# uow.identities.get() instead of full-table find_by_period() scans.
# ---------------------------------------------------------------------------


class TestConnectorIdentityDirectLookup:
    """GAP-028: connector resolve uses targeted get() calls, never full-table scans."""

    # --- Method-usage assertions ---

    def test_resource_lookup_uses_get_not_find_by_period(self, mock_uow: MagicMock) -> None:
        """resolve_connector_identity calls uow.resources.get() exactly once, never find_by_period."""
        from plugins.confluent_cloud.handlers.connector_identity import resolve_connector_identity

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={"kafka_auth_mode": "UNKNOWN"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.get.return_value = connector

        resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.get.assert_called_once_with(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
        )
        mock_uow.resources.find_by_period.assert_not_called()

    def test_identity_lookup_sa_mode_uses_get_not_find_by_period(self, mock_uow: MagicMock) -> None:
        """SERVICE_ACCOUNT mode resolves identity via uow.identities.get(), never find_by_period."""
        from plugins.confluent_cloud.handlers.connector_identity import resolve_connector_identity

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "SERVICE_ACCOUNT",
                "kafka_service_account_id": "sa-owner-123",
            },
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
        mock_uow.resources.get.return_value = connector
        mock_uow.identities.get.return_value = sa_owner

        resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
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

    def test_identity_lookup_api_key_mode_at_most_two_get_calls(self, mock_uow: MagicMock) -> None:
        """KAFKA_API_KEY mode makes at most 2 identities.get() calls (api_key + owner), never find_by_period."""
        from plugins.confluent_cloud.handlers.connector_identity import resolve_connector_identity

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "key-abc",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        api_key = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="key-abc",
            identity_type="api_key",
            metadata={"owner_id": "u-user-1"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        user = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="u-user-1",
            identity_type="user",
            display_name="Human User",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        def identity_get_side_effect(ecosystem: str, tenant_id: str, identity_id: str) -> Identity | None:
            if identity_id == "key-abc":
                return api_key
            if identity_id == "u-user-1":
                return user
            return None

        mock_uow.resources.get.return_value = connector
        mock_uow.identities.get.side_effect = identity_get_side_effect

        resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert mock_uow.identities.get.call_count <= 2
        mock_uow.identities.find_by_period.assert_not_called()

    # --- Behavioral parity tests (new mock setup via get()) ---

    def test_masked_sentinel_parity_resource_get_returns_none(self, mock_uow: MagicMock) -> None:
        """get(resource) returns None → masked sentinel; get() is called, find_by_period is not."""
        from plugins.confluent_cloud.handlers.connector_identity import resolve_connector_identity

        mock_uow.resources.get.return_value = None  # resource not found

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.get.assert_called_once_with(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
        )
        mock_uow.resources.find_by_period.assert_not_called()
        assert "connector_credentials_masked" in result.resource_active.ids()

    def test_api_key_not_found_parity_identity_get_returns_none(self, mock_uow: MagicMock) -> None:
        """API key not in DB (get returns None) → connector_api_key_not_found; no find_by_period."""
        from plugins.confluent_cloud.handlers.connector_identity import resolve_connector_identity

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "real-key-missing",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.get.return_value = connector
        mock_uow.identities.get.return_value = None  # api key not found

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.find_by_period.assert_not_called()
        mock_uow.identities.find_by_period.assert_not_called()
        assert "connector_api_key_not_found" in result.resource_active.ids()

    def test_owner_unknown_parity_api_key_has_no_owner_id(self, mock_uow: MagicMock) -> None:
        """API key found but no owner_id in metadata → connector_credentials_unknown; no find_by_period."""
        from plugins.confluent_cloud.handlers.connector_identity import resolve_connector_identity

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "key-abc",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        api_key = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="key-abc",
            identity_type="api_key",
            metadata={},  # no owner_id
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.get.return_value = connector
        mock_uow.identities.get.return_value = api_key

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.find_by_period.assert_not_called()
        mock_uow.identities.find_by_period.assert_not_called()
        assert "connector_credentials_unknown" in result.resource_active.ids()

    def test_happy_path_sa_mode_parity_using_get(self, mock_uow: MagicMock) -> None:
        """SERVICE_ACCOUNT happy path: resource.get + identity.get → SA owner resolved."""
        from plugins.confluent_cloud.handlers.connector_identity import resolve_connector_identity

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "SERVICE_ACCOUNT",
                "kafka_service_account_id": "sa-happy",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-happy",
            identity_type="service_account",
            display_name="Happy Owner",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.get.return_value = connector
        mock_uow.identities.get.return_value = sa_owner

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.find_by_period.assert_not_called()
        mock_uow.identities.find_by_period.assert_not_called()
        assert len(result.resource_active) == 1
        assert "sa-happy" in result.resource_active.ids()
        assert result.resource_active.get("sa-happy").display_name == "Happy Owner"

    def test_happy_path_api_key_mode_parity_using_get(self, mock_uow: MagicMock) -> None:
        """KAFKA_API_KEY happy path: api_key.get + owner.get → user owner resolved."""
        from plugins.confluent_cloud.handlers.connector_identity import resolve_connector_identity

        connector = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="connector-abc",
            resource_type="connector",
            metadata={
                "kafka_auth_mode": "KAFKA_API_KEY",
                "kafka_api_key": "key-abc",
            },
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        api_key = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="key-abc",
            identity_type="api_key",
            metadata={"owner_id": "u-user-1"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        user = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="u-user-1",
            identity_type="user",
            display_name="Human User",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        def identity_get_side_effect(ecosystem: str, tenant_id: str, identity_id: str) -> Identity | None:
            if identity_id == "key-abc":
                return api_key
            if identity_id == "u-user-1":
                return user
            return None

        mock_uow.resources.get.return_value = connector
        mock_uow.identities.get.side_effect = identity_get_side_effect

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        mock_uow.resources.find_by_period.assert_not_called()
        mock_uow.identities.find_by_period.assert_not_called()
        assert len(result.resource_active) == 1
        assert "u-user-1" in result.resource_active.ids()

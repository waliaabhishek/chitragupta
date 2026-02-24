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
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = [sa_owner]

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

    def test_kafka_api_key_mode_resolves_api_key_owner(
        self, mock_uow: MagicMock
    ) -> None:
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
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = [api_key, user_owner]

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
        """UNKNOWN auth mode creates connector_credentials_unknown sentinel."""
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
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = []

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
        sentinel = result.resource_active.get("connector_credentials_unknown")
        assert sentinel is not None
        assert sentinel.identity_type == "connector_credentials"

    def test_resource_not_found_creates_masked_sentinel(
        self, mock_uow: MagicMock
    ) -> None:
        """Missing connector resource creates connector_credentials_masked sentinel."""
        from plugins.confluent_cloud.handlers.connector_identity import (
            resolve_connector_identity,
        )

        mock_uow.resources.find_by_period.return_value = []
        mock_uow.identities.find_by_period.return_value = []

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

    def test_service_account_mode_owner_not_in_db_creates_sentinel(
        self, mock_uow: MagicMock
    ) -> None:
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
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = []

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

    def test_kafka_api_key_mode_api_key_not_in_db_creates_unknown_sentinel(
        self, mock_uow: MagicMock
    ) -> None:
        """KAFKA_API_KEY mode with API key not found creates unknown sentinel."""
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
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = []

        result = resolve_connector_identity(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        # API key not found means we can't determine owner -> unknown sentinel
        assert len(result.resource_active) == 1
        assert "connector_credentials_unknown" in result.resource_active.ids()

    def test_kafka_api_key_mode_api_key_missing_owner_creates_unknown_sentinel(
        self, mock_uow: MagicMock
    ) -> None:
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
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = [api_key]

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

    def test_missing_auth_mode_creates_unknown_sentinel(
        self, mock_uow: MagicMock
    ) -> None:
        """Missing kafka_auth_mode in metadata creates unknown sentinel."""
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
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = []

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

    def test_service_account_mode_missing_sa_id_creates_unknown_sentinel(
        self, mock_uow: MagicMock
    ) -> None:
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
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = []

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

    def test_kafka_api_key_mode_missing_api_key_in_metadata_creates_unknown_sentinel(
        self, mock_uow: MagicMock
    ) -> None:
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
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = []

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

    def test_tenant_period_and_metrics_derived_are_empty(
        self, mock_uow: MagicMock
    ) -> None:
        """tenant_period and metrics_derived are returned empty."""
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
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.resources.find_by_period.return_value = [connector]
        mock_uow.identities.find_by_period.return_value = [sa_owner]

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

    def test_filters_to_correct_connector_resource(
        self, mock_uow: MagicMock
    ) -> None:
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
        connector_other = Resource(
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
        mock_uow.resources.find_by_period.return_value = [
            connector_match,
            connector_other,
        ]
        mock_uow.identities.find_by_period.return_value = [sa_correct]

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

        result = create_connector_sentinel(
            "connector_credentials_masked", "org-123", "confluent_cloud", is_masked=True
        )
        assert result.identity_type == "connector_credentials"
        assert result.identity_id == "connector_credentials_masked"
        assert result.display_name == "Connector Credentials Masked"

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.models.chargeback import CostType
from core.storage.backends.sqlmodel.mappers import chargeback_to_dimension, chargeback_to_domain
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable

_NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)


def _make_dim(env_id: str = "") -> ChargebackDimensionTable:
    return ChargebackDimensionTable(
        dimension_id=1,
        ecosystem="confluent_cloud",
        tenant_id="t-1",
        resource_id="cluster-1",
        product_category="kafka",
        product_type="kafka_num_ckus",
        identity_id="user-1",
        cost_type="usage",
        allocation_method="direct",
        allocation_detail=None,
        env_id=env_id,
    )


def _make_fact(dimension_id: int = 1) -> ChargebackFactTable:
    return ChargebackFactTable(
        timestamp=_NOW,
        dimension_id=dimension_id,
        amount="10.00",
        tags_json="[]",
    )


class TestChargebackToDomainEnvId:
    """Gap 2: chargeback_to_domain() must reconstitute env_id from dim.env_id into metadata."""

    def test_env_id_reconstituted_on_read(self) -> None:
        """Item 3: dim.env_id='env-xyz' → result.metadata['env_id'] == 'env-xyz'."""
        dim = _make_dim(env_id="env-xyz")
        fact = _make_fact()

        result = chargeback_to_domain(dim, fact)

        assert result.metadata["env_id"] == "env-xyz"

    def test_empty_env_id_not_propagated_to_metadata(self) -> None:
        """Item 4: dim.env_id='' → result.metadata == {}."""
        dim = _make_dim(env_id="")
        fact = _make_fact()

        result = chargeback_to_domain(dim, fact)

        assert result.metadata == {}

    def test_round_trip_env_id_survives_write_and_read(self) -> None:
        """Item 7: chargeback_to_dimension then chargeback_to_domain preserves env_id round-trip."""
        from core.models.chargeback import ChargebackRow

        row = ChargebackRow(
            ecosystem="confluent_cloud",
            tenant_id="t-1",
            timestamp=_NOW,
            resource_id="cluster-rt",
            product_category="kafka",
            product_type="kafka_num_ckus",
            identity_id="user-rt",
            cost_type=CostType.USAGE,
            amount=Decimal("42.00"),
            allocation_method="direct",
            allocation_detail=None,
            tags=[],
            metadata={"env_id": "env-rt"},
        )

        dim = chargeback_to_dimension(row)
        # Assign a dimension_id (normally DB-assigned) for round-trip
        dim.dimension_id = 99
        fact = ChargebackFactTable(
            timestamp=_NOW,
            dimension_id=99,
            amount=str(row.amount),
            tags_json="[]",
        )

        result = chargeback_to_domain(dim, fact)

        assert result.metadata["env_id"] == "env-rt"

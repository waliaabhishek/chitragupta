"""Tests for the narrow public chargeback metadata projection."""

from __future__ import annotations

import ast
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from core.models.chargeback import ChargebackRow, CostType


def _row(*, metadata: dict[str, str], principal_team: str | None) -> ChargebackRow:
    return ChargebackRow(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        resource_id="cluster-1",
        product_category="kafka",
        product_type="SELF_KAFKA_NETWORK_INGRESS",
        identity_id="User:alice",
        cost_type=CostType.USAGE,
        amount=Decimal("10.0000"),
        metadata=metadata,
        principal_team=principal_team,
    )


def test_chargeback_public_metadata_projects_only_non_null_team_without_mutating_the_row() -> None:
    from core.api.chargeback_serialization import chargeback_public_metadata

    row_without_team = _row(metadata={"env_id": "env-1"}, principal_team=None)
    row_with_team = _row(metadata={"team": "transient", "env_id": "env-1"}, principal_team="team-data")

    assert chargeback_public_metadata(row_without_team) == {"env_id": "env-1"}
    assert chargeback_public_metadata(row_with_team) == {"team": "team-data", "env_id": "env-1"}
    assert row_with_team.metadata == {"team": "transient", "env_id": "env-1"}


def test_chargeback_serialization_uses_future_annotations_as_its_first_executable_import() -> None:
    source_path = Path(__file__).resolve().parents[4] / "src" / "core" / "api" / "chargeback_serialization.py"
    module = ast.parse(source_path.read_text(encoding="utf-8"))

    first_statement = module.body[0]

    assert isinstance(first_statement, ast.ImportFrom)
    assert first_statement.module == "__future__"
    assert [alias.name for alias in first_statement.names] == ["annotations"]

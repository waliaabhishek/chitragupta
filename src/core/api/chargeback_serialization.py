from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.models.chargeback import ChargebackRow


def chargeback_public_metadata(row: ChargebackRow) -> dict[str, Any]:
    """Return the existing public metadata representation for a chargeback row."""
    metadata = dict(row.metadata)
    if row.principal_team is not None:
        metadata["team"] = row.principal_team
    return metadata

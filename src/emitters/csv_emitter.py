from __future__ import annotations

import csv
import logging
from collections.abc import Sequence
from datetime import date as date_type
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.models.chargeback import ChargebackRow

logger = logging.getLogger(__name__)


class CsvEmitter:
    """Writes chargeback rows for one tenant/date (or period) to a CSV file.

    Receives rows that have already been aggregated by EmitPhase — no
    aggregation logic here. Overwrites on each call — idempotent for re-runs
    and monthly accumulation rewrites.

    Output columns (in order):
        ecosystem, tenant_id, timestamp, resource_id, product_category,
        product_type, identity_id, cost_type, amount, allocation_method,
        allocation_detail
    """

    _FIELDNAMES = (
        "ecosystem",
        "tenant_id",
        "timestamp",
        "resource_id",
        "product_category",
        "product_type",
        "identity_id",
        "cost_type",
        "amount",
        "allocation_method",
        "allocation_detail",
    )

    def __init__(self, output_dir: str, filename_template: str = "{tenant_id}_{date}.csv") -> None:
        self._output_dir = Path(output_dir)
        self._filename_template = filename_template

    def __call__(self, tenant_id: str, date: date_type, rows: Sequence[ChargebackRow]) -> None:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        filename = self._filename_template.format(tenant_id=tenant_id, date=date.isoformat())
        out_path = self._output_dir / filename

        with out_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=self._FIELDNAMES)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "ecosystem": row.ecosystem,
                        "tenant_id": row.tenant_id,
                        "timestamp": row.timestamp.isoformat(),
                        "resource_id": row.resource_id or "",
                        "product_category": row.product_category,
                        "product_type": row.product_type,
                        "identity_id": row.identity_id,
                        "cost_type": str(row.cost_type),
                        "amount": str(row.amount),
                        "allocation_method": row.allocation_method or "",
                        "allocation_detail": row.allocation_detail or "",
                    }
                )

        logger.info("CSV emitter wrote %d rows to %s", len(rows), out_path)


def make_csv_emitter(
    output_dir: str,
    filename_template: str = "{tenant_id}_{date}.csv",
) -> CsvEmitter:
    """Factory registered as ``"csv"`` in EmitterRegistry.

    Example YAML:
        emitters:
          - type: csv
            aggregation: daily
            params:
              output_dir: "/data/csv"
    """
    return CsvEmitter(output_dir=output_dir, filename_template=filename_template)

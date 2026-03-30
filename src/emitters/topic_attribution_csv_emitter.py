from __future__ import annotations

import csv
import logging
from collections.abc import Sequence
from datetime import date as date_type
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.models.topic_attribution import TopicAttributionRow

logger = logging.getLogger(__name__)


class TopicAttributionCsvEmitter:
    """Write TopicAttributionRow list for one tenant/date to a CSV file.

    Overwrites on each call — idempotent for re-runs.
    """

    _FIELDNAMES = (
        "ecosystem",
        "tenant_id",
        "timestamp",
        "env_id",
        "cluster_resource_id",
        "topic_name",
        "product_category",
        "product_type",
        "attribution_method",
        "amount",
    )

    def __init__(self, output_dir: str, filename_template: str = "topic_attr_{tenant_id}_{date}.csv") -> None:
        self._output_dir = Path(output_dir)
        self._filename_template = filename_template

    def __call__(self, tenant_id: str, date: date_type, rows: Sequence[TopicAttributionRow]) -> None:
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
                        "env_id": row.env_id,
                        "cluster_resource_id": row.cluster_resource_id,
                        "topic_name": row.topic_name,
                        "product_category": row.product_category,
                        "product_type": row.product_type,
                        "attribution_method": row.attribution_method,
                        "amount": str(row.amount),
                    }
                )

        logger.info("TopicAttributionCsvEmitter wrote %d rows to %s", len(rows), out_path)

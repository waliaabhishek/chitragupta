from __future__ import annotations

import csv
import logging
from collections.abc import Sequence
from datetime import date as date_type
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _serialize(value: Any) -> str:
    """Serialize a field value to CSV string."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, StrEnum):
        return str(value)
    return str(value)


class CsvEmitter:
    """Generic CSV emitter — reads __csv_fields__ from the row type at emit time.

    Works for any row type that declares __csv_fields__: ClassVar[tuple[str, ...]].
    Overwrites on each call — idempotent for re-runs and monthly accumulation rewrites.

    If the row type has empty __csv_fields__ (e.g. BillingEmitRow — Prometheus-only),
    the emitter is a no-op. This is intentional: billing/resource/identity rows only
    feed Prometheus; their EmitterRunner instances only receive prometheus-type specs,
    so CsvEmitter will never be built for those pipelines in practice.
    """

    def __init__(self, output_dir: str, filename_template: str = "{tenant_id}_{date}.csv") -> None:
        self._output_dir = Path(output_dir)
        self._filename_template = filename_template

    def __call__(self, tenant_id: str, date: date_type, rows: Sequence[Any]) -> None:
        if not rows:
            # Practically unreachable: EmitterRunner only calls emitters for dates that
            # have data (date source returns only dates with rows). Defensive guard only.
            return
        fields: tuple[str, ...] = type(rows[0]).__csv_fields__
        if not fields:
            return  # row type opts out of CSV (e.g. BillingEmitRow)

        self._output_dir.mkdir(parents=True, exist_ok=True)
        filename = self._filename_template.format(tenant_id=tenant_id, date=date.isoformat())
        out_path = self._output_dir / filename

        with out_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow({f: _serialize(getattr(row, f)) for f in fields})

        logger.info("CsvEmitter wrote %d rows to %s", len(rows), out_path)


def make_csv_emitter(
    output_dir: str,
    filename_template: str = "{tenant_id}_{date}.csv",
) -> CsvEmitter:
    """Factory registered as ``"csv"`` in EmitterRegistry.

    The default filename_template is chargeback-oriented. For topic attribution,
    the TA overlay config (TopicAttributionConfig) injects
    ``filename_template="topic_attr_{tenant_id}_{date}.csv"`` as a per-spec default
    so callers never see the chargeback default for TA CSV files.
    """
    return CsvEmitter(output_dir=output_dir, filename_template=filename_template)

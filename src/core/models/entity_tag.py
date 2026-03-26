from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class EntityTag:
    """A user-defined tag attached to a core entity (resource or identity)."""

    tag_id: int | None
    tenant_id: str
    entity_type: str  # "resource" | "identity"
    entity_id: str
    tag_key: str
    tag_value: str
    created_by: str
    created_at: datetime | None = None

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import and_, func
from sqlalchemy.orm import aliased
from sqlmodel import col

from core.storage.backends.sqlmodel.tables import EntityTagTable

logger = logging.getLogger(__name__)


@dataclass
class TagJoinSpec:
    """Pre-built join artifacts for one tag key."""

    tag_key: str
    label: str  # SQL column label, e.g. "taggb_owner"
    resource_alias: Any  # aliased(EntityTagTable) for resource side
    resource_join_cond: Any  # ON clause for the resource LEFT JOIN
    identity_alias: Any | None  # aliased(EntityTagTable) for identity side; None for resource-only entities
    identity_join_cond: Any | None
    resolved_expr: Any  # COALESCE(r.tag_value, i.tag_value) — NULL when untagged, for WHERE filters
    group_expr: Any  # COALESCE(r.tag_value, i.tag_value, 'UNTAGGED') — for SELECT/GROUP BY


def build_tag_join_specs(
    tag_keys: list[str],
    tenant_id: str,
    resource_id_col: Any,
    identity_id_col: Any | None = None,
) -> list[TagJoinSpec]:
    """
    Build aliased LEFT JOIN specs for each tag key.

    For chargeback (both entity types): pass both resource_id_col and identity_id_col.
    For topic attribution (TASK-215, resource only): pass only resource_id_col.

    Resource tag takes precedence over identity tag on collision — COALESCE(resource, identity).
    This matches the _overlay_tags() behavior.
    """
    specs: list[TagJoinSpec] = []
    for key in tag_keys:
        safe = re.sub(r"[^a-zA-Z0-9]", "_", key)
        label = f"taggb_{safe}"

        r_alias = aliased(EntityTagTable, name=f"rt_{safe}")
        r_cond = and_(
            col(r_alias.entity_type) == "resource",
            col(r_alias.entity_id) == resource_id_col,
            col(r_alias.tag_key) == key,
            col(r_alias.tenant_id) == tenant_id,
        )

        i_alias = None
        i_cond = None
        if identity_id_col is not None:
            i_alias = aliased(EntityTagTable, name=f"it_{safe}")
            i_cond = and_(
                col(i_alias.entity_type) == "identity",
                col(i_alias.entity_id) == identity_id_col,
                col(i_alias.tag_key) == key,
                col(i_alias.tenant_id) == tenant_id,
            )

        resolved_expr: Any
        if i_alias is not None:
            resolved_expr = func.coalesce(r_alias.tag_value, i_alias.tag_value)
            group_expr = func.coalesce(r_alias.tag_value, i_alias.tag_value, "UNTAGGED")
        else:
            resolved_expr = r_alias.tag_value
            group_expr = func.coalesce(r_alias.tag_value, "UNTAGGED")

        specs.append(
            TagJoinSpec(
                tag_key=key,
                label=label,
                resource_alias=r_alias,
                resource_join_cond=r_cond,
                identity_alias=i_alias,
                identity_join_cond=i_cond,
                resolved_expr=resolved_expr,
                group_expr=group_expr,
            )
        )
    return specs

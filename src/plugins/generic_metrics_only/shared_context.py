from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.models import Resource


@dataclass(frozen=True)
class GenericSharedContext:
    """Pre-built cluster resource for a single gather cycle."""

    cluster_resource: Resource

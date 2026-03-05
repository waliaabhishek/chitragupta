"""Base convenience class for service handlers.

Eliminates three repeated patterns across CCloud handlers:
1. Standard 3-field __init__ (connection, config, ecosystem)
2. Dict-lookup get_allocator() via class-level _ALLOCATOR_MAP
3. Empty gather_identities() default

Protocol compliance is structural — this class does not inherit from ServiceHandler.
Subclasses must still define service_type, handles_product_types, gather_resources,
resolve_identities, and get_metrics_for_product_type.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from core.models import Identity
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork


class BaseServiceHandler[ConnT, CfgT]:
    """Opt-in base class for service handlers that store connection, config, ecosystem.

    Handlers needing extra __init__ logic (e.g. FlinkHandler._flink_regions) call
    super().__init__() then add their own fields.
    """

    _ALLOCATOR_MAP: ClassVar[dict[str, CostAllocator]] = {}

    def __init__(
        self,
        connection: ConnT,
        config: CfgT,
        ecosystem: str,
    ) -> None:
        self._connection = connection
        self._config = config
        self._ecosystem = ecosystem

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
        """Default: return empty iterable. Override when handler gathers identities."""
        return iter(())

    def get_allocator(self, product_type: str) -> CostAllocator:
        """Look up allocator from _ALLOCATOR_MAP. Raises ValueError if unknown."""
        allocator = self._ALLOCATOR_MAP.get(product_type)
        if allocator is None:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg)
        return allocator

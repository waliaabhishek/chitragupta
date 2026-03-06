from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.plugin.protocols import EcosystemPlugin, ServiceHandler
logger = logging.getLogger(__name__)


@dataclass
class EcosystemBundle:
    """Built by the orchestrator after initialize(), not by the registry."""

    plugin: EcosystemPlugin
    handlers: dict[str, ServiceHandler]
    product_type_to_handler: dict[str, ServiceHandler]

    @staticmethod
    def build(plugin: EcosystemPlugin) -> EcosystemBundle:
        """Build from an initialized plugin. Call after plugin.initialize()."""
        logger.debug("Building EcosystemBundle for plugin %s", plugin.ecosystem)
        handlers = plugin.get_service_handlers()
        product_type_to_handler: dict[str, ServiceHandler] = {}
        for handler in handlers.values():
            for pt in handler.handles_product_types:
                if pt in product_type_to_handler:
                    raise ValueError(
                        f"Duplicate product_type '{pt}': claimed by both "
                        f"'{product_type_to_handler[pt].service_type}' and "
                        f"'{handler.service_type}'"
                    )
                product_type_to_handler[pt] = handler
        logger.info(
            "EcosystemBundle built ecosystem=%s handlers=%d product_types=%d",
            plugin.ecosystem,
            len(handlers),
            len(product_type_to_handler),
        )
        return EcosystemBundle(
            plugin=plugin,
            handlers=handlers,
            product_type_to_handler=product_type_to_handler,
        )


class PluginRegistry:
    """Factory lookup: ecosystem name -> plugin factory. Stores no initialized state."""

    _factories: dict[str, Callable[[], EcosystemPlugin]]

    def __init__(self) -> None:
        self._factories = {}

    def register(self, ecosystem: str, factory: Callable[[], EcosystemPlugin]) -> None:
        logger.debug("Registering plugin ecosystem=%r", ecosystem)
        if ecosystem in self._factories:
            raise ValueError(f"Ecosystem '{ecosystem}' is already registered")
        self._factories[ecosystem] = factory

    def create(self, ecosystem: str) -> EcosystemPlugin:
        logger.debug("Creating plugin for ecosystem=%r", ecosystem)
        try:
            factory = self._factories[ecosystem]
        except KeyError:
            raise KeyError(f"Unknown ecosystem '{ecosystem}'") from None
        return factory()

    def list_ecosystems(self) -> list[str]:
        return list(self._factories)

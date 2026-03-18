from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.plugin.protocols import Emitter

logger = logging.getLogger(__name__)

_REGISTRY: dict[str, Callable[..., Emitter]] = {}


def register(name: str, factory: Callable[..., Emitter]) -> None:
    """Register an emitter factory under a short name.

    ``factory`` must accept keyword arguments matching the ``params`` dict
    from ``EmitterSpec`` and return an object satisfying the ``Emitter`` protocol.
    """
    logger.debug("Registering emitter %r", name)
    _REGISTRY[name] = factory


def get_factory(name: str) -> Callable[..., Emitter] | None:
    """Return the registered factory for *name*, or None if not registered."""
    return _REGISTRY.get(name)


def get(name: str, params: dict[str, Any], extra: dict[str, Any] | None = None) -> Emitter:
    """Instantiate an emitter by registered name.

    Raises:
        ValueError: If *name* is not registered — includes list of available names.
    """
    logger.debug("Creating emitter %r", name)
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY)) or "(none)"
        logger.error("Unknown emitter %r registered=%s", name, list(_REGISTRY))
        raise ValueError(f"Unknown emitter type {name!r}. Available: {available}")
    merged = {**params, **(extra or {})}
    return _REGISTRY[name](**merged)

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.plugin.protocols import Emitter

_REGISTRY: dict[str, Callable[..., Emitter]] = {}


def register(name: str, factory: Callable[..., Emitter]) -> None:
    """Register an emitter factory under a short name.

    ``factory`` must accept keyword arguments matching the ``params`` dict
    from ``EmitterSpec`` and return an object satisfying the ``Emitter`` protocol.
    """
    _REGISTRY[name] = factory


def get(name: str, params: dict[str, Any]) -> Emitter:
    """Instantiate an emitter by registered name.

    Raises:
        ValueError: If *name* is not registered — includes list of available names.
    """
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise ValueError(f"Unknown emitter type {name!r}. Available: {available}")
    return _REGISTRY[name](**params)

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.plugin.protocols import EcosystemPlugin

logger = logging.getLogger(__name__)


def discover_plugins(
    plugins_path: Path,
) -> list[tuple[str, Callable[[], EcosystemPlugin]]]:
    """Scan plugins_path for packages containing a register() function.

    Each plugin package must expose a callable ``register()`` that returns
    a ``(ecosystem_name, factory)`` tuple.
    """
    results: list[tuple[str, Callable[[], EcosystemPlugin]]] = []

    if not plugins_path.is_dir():
        return results

    for entry in sorted(plugins_path.iterdir()):
        if not entry.is_dir():
            continue
        # Skip __pycache__ and hidden dirs
        if entry.name.startswith(("_", ".")):
            continue

        module_name = f"plugins.{entry.name}"
        try:
            module = importlib.import_module(module_name)
        except Exception:  # Plugins can raise any exception type on import
            logger.warning("Failed to import plugin package %r", module_name, exc_info=True)
            continue

        register_fn = getattr(module, "register", None)
        if not callable(register_fn):
            logger.debug("Plugin package %r has no register() function, skipping", module_name)
            continue

        try:
            result = register_fn()
        except Exception:  # Plugin register() can raise any exception type
            logger.warning("register() failed for plugin package %r", module_name, exc_info=True)
            continue

        # Validate result shape before accepting
        if (
            not isinstance(result, tuple)
            or len(result) != 2
            or not isinstance(result[0], str)
            or not callable(result[1])
        ):
            logger.warning(
                "register() in %r returned malformed result (expected (str, callable), got %r)",
                module_name,
                type(result).__name__,
            )
            continue

        results.append(result)

    return results

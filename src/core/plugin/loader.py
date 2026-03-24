from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

    from core.plugin.protocols import EcosystemPlugin

logger = logging.getLogger(__name__)


def _import_plugin_module(entry: Path, plugins_path: Path, *, on_sys_path: bool | None = None) -> ModuleType:
    """Import a plugin package directory as a Python module.

    Uses package-based import (importlib.import_module) when plugins_path's
    parent directory is on sys.path — covers the built-in src/plugins/ package
    without special-casing it.

    Falls back to file-based import (spec_from_file_location) for external
    plugin directories not on sys.path. Requires __init__.py to be present.

    ``on_sys_path`` may be pre-computed by the caller to avoid recomputation
    on every call in a loop. When ``None``, it is computed here.
    """
    if on_sys_path is None:
        resolved_parent = plugins_path.parent.resolve()
        on_sys_path = any(Path(p).resolve() == resolved_parent for p in sys.path if p)

    if on_sys_path:
        # Derive module name from the actual package name, not a hardcoded prefix.
        # e.g. for src/plugins/my_plugin the module name becomes "plugins.my_plugin".
        module_name = f"{plugins_path.name}.{entry.name}"
        return importlib.import_module(module_name)

    # External path — load from filesystem directly, no sys.path mutation.
    init_file = entry / "__init__.py"
    if not init_file.exists():
        raise ImportError(
            f"External plugin {entry.name!r} is missing __init__.py "
            f"(expected at {init_file}). "
            f"Add an __init__.py to make the directory a Python package."
        )

    # Namespaced module name avoids collisions with built-ins or other
    # external plugins that share the same directory name.
    module_name = f"chitragupta_plugin_{entry.name}"
    spec = importlib.util.spec_from_file_location(
        module_name,
        init_file,
        submodule_search_locations=[str(entry)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create import spec for {init_file}. Ensure the file is a valid Python source file.")

    module = importlib.util.module_from_spec(spec)
    # Register before exec_module so intra-plugin relative imports resolve.
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise

    return module


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

    resolved_parent = plugins_path.parent.resolve()
    on_sys_path = any(Path(p).resolve() == resolved_parent for p in sys.path if p)

    for entry in sorted(plugins_path.iterdir()):
        if not entry.is_dir():
            continue
        # Skip __pycache__ and hidden dirs
        if entry.name.startswith(("_", ".")):
            continue

        try:
            module = _import_plugin_module(entry, plugins_path, on_sys_path=on_sys_path)
        except Exception:
            logger.warning("Failed to import plugin %r", entry.name, exc_info=True)
            continue

        register_fn = getattr(module, "register", None)
        if not callable(register_fn):
            logger.debug("Plugin package %r has no register() function, skipping", entry.name)
            continue

        try:
            result = register_fn()
        except Exception:  # Plugin register() can raise any exception type
            logger.warning("register() failed for plugin %r", entry.name, exc_info=True)
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
                entry.name,
                type(result).__name__,
            )
            continue

        results.append(result)

    return results

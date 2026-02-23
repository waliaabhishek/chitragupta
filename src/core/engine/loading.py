from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from typing import Any


def load_protocol_callable(
    dotted_path: str,
    protocol: type,
) -> Callable[..., Any]:
    """Load a callable from a dotted path and validate against a protocol.

    Security: ``dotted_path`` must come from trusted configuration only.
    ``importlib.import_module`` executes arbitrary module-level code on import.
    Never pass user-supplied or untrusted strings as ``dotted_path``.

    Args:
        dotted_path: ``"module.path:function_name"`` format.
        protocol: A ``runtime_checkable`` Protocol type for validation.

    Returns:
        The loaded callable.

    Raises:
        ValueError: If *dotted_path* is empty or missing the colon separator.
        ImportError: If the module cannot be imported.
        AttributeError: If the attribute is not found in the module.
        TypeError: If the loaded object does not satisfy *protocol* or has
            an incompatible signature.
    """
    if not dotted_path:
        msg = "dotted_path must not be empty"
        raise ValueError(msg)

    if ":" not in dotted_path:
        msg = f"Expected 'module:attribute' format, got {dotted_path!r}"
        raise ValueError(msg)

    module_path, attr_name = dotted_path.rsplit(":", 1)

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        msg = f"Could not import module {module_path!r}"
        raise ImportError(msg) from exc

    try:
        obj = getattr(module, attr_name)
    except AttributeError as exc:
        msg = f"Module {module_path!r} has no attribute {attr_name!r}"
        raise AttributeError(msg) from exc

    # Reject bare classes — must be a function or callable instance
    if inspect.isclass(obj):
        msg = f"Loaded object {obj!r} is a class; provide a function or callable instance, not an uninstantiated class"
        raise TypeError(msg)

    if not isinstance(obj, protocol):
        msg = f"Loaded object {obj!r} does not satisfy protocol {protocol.__name__}"
        raise TypeError(msg)

    # Validate signature parameter count
    _validate_signature(obj, protocol)

    assert callable(obj)  # guaranteed by isinstance check above; narrows type for mypy
    return obj


def _validate_signature(obj: Any, protocol: type) -> None:
    """Validate that obj's signature matches the protocol's __call__ parameter count."""
    if not callable(protocol):
        return
    proto_call = protocol.__call__

    try:
        proto_sig = inspect.signature(proto_call)
    except (ValueError, TypeError):  # fmt: skip  # parens required for 3.12/3.13 compat
        return

    # Count positional parameters (excluding 'self')
    proto_params = [
        p
        for name, p in proto_sig.parameters.items()
        if name != "self" and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    expected_count = len(proto_params)

    # Determine what to inspect for the loaded object
    target = obj.__call__ if callable(obj) and not inspect.isfunction(obj) else obj

    try:
        obj_sig = inspect.signature(target)
    except ValueError as exc:
        msg = f"Cannot validate signature of {obj!r}; ensure it is a Python function or callable instance"
        raise TypeError(msg) from exc

    obj_params = [
        p
        for name, p in obj_sig.parameters.items()
        if name != "self" and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]

    # If the object accepts *args, it's permissive — allow it
    has_var_positional = any(p.kind == p.VAR_POSITIONAL for p in obj_sig.parameters.values())
    if has_var_positional:
        return

    actual_count = len(obj_params)
    if actual_count != expected_count:
        msg = (
            f"Signature mismatch: protocol {protocol.__name__} expects "
            f"{expected_count} positional parameter(s) but {obj!r} accepts "
            f"{actual_count}"
        )
        raise TypeError(msg)

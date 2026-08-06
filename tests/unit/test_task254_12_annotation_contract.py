from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NEW_MODULES = (
    "src/core/preview/evidence_capture.py",
    "src/core/preview/organization_authority.py",
    "src/core/preview/storage_availability.py",
    "src/core/storage/backend_provider.py",
    "src/core/storage/migrations/preview_hook.py",
    "src/plugins/confluent_cloud/source_capture.py",
    "src/plugins/confluent_cloud/preview_bootstrap.py",
    "src/plugins/confluent_cloud/storage/organization_authority.py",
    "src/plugins/confluent_cloud/storage/preview_tables.py",
    "src/plugins/confluent_cloud/storage/preview_schema.py",
)


@pytest.mark.parametrize("relative_path", NEW_MODULES)
def test_new_preview_boundary_module_uses_future_annotations_and_typed_public_callables(
    relative_path: str,
) -> None:
    path = ROOT / relative_path
    assert path.is_file(), f"missing designed module: {relative_path}"
    tree = ast.parse(path.read_text())
    future_imports = [node for node in tree.body if isinstance(node, ast.ImportFrom) and node.module == "__future__"]
    assert future_imports
    assert any(alias.name == "annotations" for node in future_imports for alias in node.names)

    missing: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if node.name.startswith("_") and node.name != "__init__":
            continue
        arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        untyped = [
            argument.arg
            for argument in arguments
            if argument.arg not in {"self", "cls"} and argument.annotation is None
        ]
        if node.args.vararg is not None and node.args.vararg.annotation is None:
            untyped.append(f"*{node.args.vararg.arg}")
        if node.args.kwarg is not None and node.args.kwarg.annotation is None:
            untyped.append(f"**{node.args.kwarg.arg}")
        if untyped or node.returns is None:
            missing.append(f"{node.name}({', '.join(untyped)})")

    assert missing == [], f"public callables missing annotations in {relative_path}: {missing}"

"""Tests for TASK-031: Comprehensive logging coverage across all src/ modules.

All tests in this file are structural/static checks — they parse source files
without importing them, so no side effects or import errors can interfere.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).parent.parent.parent / "src"


def _get_python_files() -> list[Path]:
    """Return all .py files in src/ excluding __init__.py and __pycache__."""
    return [p for p in SRC_ROOT.rglob("*.py") if p.name != "__init__.py" and "__pycache__" not in p.parts]


# ---------------------------------------------------------------------------
# 1. Every non-init Python file must have `import logging`
# ---------------------------------------------------------------------------


def test_all_python_files_have_logging_import() -> None:
    """Every .py file in src/ (excl. __init__.py) must contain `import logging`."""
    files = _get_python_files()
    assert files, "No Python files found in src/ — check SRC_ROOT"

    missing: list[str] = []
    for path in sorted(files):
        content = path.read_text()
        if "import logging" not in content:
            missing.append(str(path.relative_to(SRC_ROOT)))

    assert not missing, f"{len(missing)} file(s) missing `import logging`:\n" + "\n".join(f"  {f}" for f in missing)


# ---------------------------------------------------------------------------
# 2. Every non-init Python file must have `logger = logging.getLogger(__name__)`
# ---------------------------------------------------------------------------


def test_all_python_files_have_logger_declaration() -> None:
    """Every .py file in src/ (excl. __init__.py) must declare `logger = logging.getLogger(__name__)`."""
    files = _get_python_files()
    assert files, "No Python files found in src/ — check SRC_ROOT"

    missing: list[str] = []
    for path in sorted(files):
        content = path.read_text()
        if "logger = logging.getLogger(__name__)" not in content:
            missing.append(str(path.relative_to(SRC_ROOT)))

    assert not missing, f"{len(missing)} file(s) missing `logger = logging.getLogger(__name__)`:\n" + "\n".join(
        f"  {f}" for f in missing
    )


# ---------------------------------------------------------------------------
# 3. No f-string logging — % formatting required
# ---------------------------------------------------------------------------

# Matches any logger level call with an f-string argument:
#   logger.debug(f"...
#   logger.info(f"...
#   logger.warning(f"...
#   logger.error(f"...
#   logger.exception(f"...
_FSTRING_LOG_PATTERN = re.compile(r'logger\.(debug|info|warning|error|exception)\(f["\']')


def test_no_fstring_logging() -> None:
    """Log calls must use % formatting, not f-strings (lazy evaluation requirement)."""
    files = _get_python_files()

    violations: list[str] = []
    for path in sorted(files):
        content = path.read_text()
        for lineno, line in enumerate(content.splitlines(), start=1):
            if _FSTRING_LOG_PATTERN.search(line):
                rel = path.relative_to(SRC_ROOT)
                violations.append(f"  {rel}:{lineno}: {line.strip()}")

    assert not violations, f"{len(violations)} f-string log call(s) found (use % formatting instead):\n" + "\n".join(
        violations
    )


# ---------------------------------------------------------------------------
# 4. Except blocks that log must use logger.exception(), not logger.error()
# ---------------------------------------------------------------------------

# Files in src/ with non-trivial except blocks that log errors.
# We verify a representative set of high-value files from the design doc.
_KEY_FILES_WITH_EXCEPT: list[str] = [
    "core/storage/registry.py",
    "core/engine/loading.py",
    "core/emitters/registry.py",
    "core/config/loader.py",
    "plugins/confluent_cloud/config.py",
]

# Matches `logger.error(` inside an except block context.
# Strategy: detect any logger.error call in a file that also has `except` —
# files should use logger.exception() in those paths.
_LOGGER_ERROR_PATTERN = re.compile(r"\blogger\.error\(")
_EXCEPT_PATTERN = re.compile(r"^\s*except\b", re.MULTILINE)


@pytest.mark.parametrize("rel_path", _KEY_FILES_WITH_EXCEPT)
def test_exception_uses_logger_exception(rel_path: str) -> None:
    """These files must never use logger.error() anywhere — use logger.exception() instead.

    logger.exception() auto-captures the full traceback; logger.error() loses it.
    Sampling test on the key files identified in the design doc.
    """
    path = SRC_ROOT / rel_path
    assert path.exists(), f"Expected file not found: {rel_path}"

    content = path.read_text()
    has_except = bool(_EXCEPT_PATTERN.search(content))

    if not has_except:
        pytest.skip(f"{rel_path} has no except blocks — nothing to verify")

    # Within a file that has except blocks, logger.error() is forbidden.
    # All error-level logging in except contexts must use logger.exception().
    error_calls = _LOGGER_ERROR_PATTERN.findall(content)
    assert not error_calls, (
        f"{rel_path}: found {len(error_calls)} logger.error() call(s) in a file with "
        f"except blocks — use logger.exception() to preserve traceback"
    )

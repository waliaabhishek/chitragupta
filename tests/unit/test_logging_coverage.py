"""Static logging policy checks that stay at behavior level."""

from __future__ import annotations

import re
from pathlib import Path

SRC_ROOT = Path(__file__).parent.parent.parent / "src"


def _get_python_files() -> list[Path]:
    """Return all .py files in src/ excluding __init__.py, __pycache__, and alembic migrations."""
    return [
        p
        for p in SRC_ROOT.rglob("*.py")
        if p.name != "__init__.py"
        and "__pycache__" not in p.parts
        and "migrations/versions" not in str(p)  # Alembic migration scripts don't need logging
    ]


# ---------------------------------------------------------------------------
# 1. No f-string logging — % formatting required
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
# 2. Representative sanitized-error contract stays helper-based, not API-based
# ---------------------------------------------------------------------------


def test_global_exception_handler_uses_sanitized_logging_helpers() -> None:
    path = SRC_ROOT / "core/api/exception_handler.py"
    content = path.read_text()

    assert "safe_log_context(" in content
    assert "safe_exception_context(" in content

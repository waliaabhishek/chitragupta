from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version


def get_version() -> str:
    """Return the installed package version, or '0.0.0-dev' when running from source."""
    try:
        return _pkg_version("chitragupta")
    except PackageNotFoundError:
        return "0.0.0-dev"


# Module-level alias so existing importers (`from core.api import API_VERSION`)
# continue to work without modification.
API_VERSION: str = get_version()

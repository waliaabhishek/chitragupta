from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, cast

import yaml
from dotenv import load_dotenv

from core.config.models import AppSettings

# .*? for default value: defaults containing literal } are not supported (matches reference behavior)
_ENV_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::-(.*?))?\}")


def substitute_env_vars(data: Any) -> Any:
    """Recursively substitute ${VAR} and ${VAR:-default} in nested data."""
    if isinstance(data, dict):
        return {k: substitute_env_vars(v) for k, v in data.items()}
    if isinstance(data, list):
        return [substitute_env_vars(item) for item in data]
    if isinstance(data, str):

        def _replacer(match: re.Match[str]) -> str:
            var_name = match.group(1)
            has_default = match.group(2) is not None
            default_value = match.group(2) if has_default else None

            if var_name in os.environ:
                return os.environ[var_name]
            if has_default:
                return cast("str", default_value)
            raise ValueError(
                f"Required environment variable '{var_name}' is not set. "
                f"Set the variable or provide a default: ${{VAR:-default}}"
            )

        return _ENV_VAR_PATTERN.sub(_replacer, data)
    return data


def load_config(
    config_path: str | Path,
    env_file: str | Path | None = None,
) -> AppSettings:
    """Load YAML config with ${VAR} env substitution -> validated AppSettings."""
    config_path = Path(config_path)

    # Load .env: explicit > auto-discover > skip
    if env_file is not None:
        load_dotenv(str(env_file), override=False)
    else:
        candidate = config_path.parent / ".env"
        if candidate.is_file():
            load_dotenv(str(candidate), override=False)

    # Read and parse YAML
    raw_text = config_path.read_text()
    try:
        raw_data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ValueError(f"Malformed YAML in '{config_path}': {exc}") from exc

    if raw_data is None:
        raw_data = {}

    # Substitute env vars and validate
    resolved = substitute_env_vars(raw_data)
    return AppSettings.model_validate(resolved)

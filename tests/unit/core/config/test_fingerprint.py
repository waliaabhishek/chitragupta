from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from core.config.models import StorageConfig


def test_storage_backend_fingerprint_uses_only_validated_backend_identity(tmp_path: Path) -> None:
    from core.config.fingerprint import storage_backend_fingerprint

    connection_string = f"sqlite:///{tmp_path / 'preview.db'}"
    config = StorageConfig(connection_string=connection_string)
    expected_payload = json.dumps(
        {"backend": "sqlmodel", "connection_string": connection_string},
        sort_keys=True,
        separators=(",", ":"),
    )

    fingerprint = storage_backend_fingerprint(config)

    assert fingerprint == hashlib.sha256(expected_payload.encode()).hexdigest()
    assert re.fullmatch(r"[0-9a-f]{64}", fingerprint)
    assert connection_string not in fingerprint
    assert str(tmp_path) not in fingerprint


def test_storage_backend_fingerprint_changes_for_distinct_sqlite_databases(tmp_path: Path) -> None:
    from core.config.fingerprint import storage_backend_fingerprint

    first = storage_backend_fingerprint(StorageConfig(connection_string=f"sqlite:///{tmp_path / 'a.db'}"))
    second = storage_backend_fingerprint(StorageConfig(connection_string=f"sqlite:///{tmp_path / 'b.db'}"))

    assert first != second

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from core.preview.artifacts import LocalPreviewArtifactStore
from core.preview.models import PreviewArtifactPayload


def _owner(tenant_name: str, tenant_id: str):
    from core.preview.artifacts import PreviewArtifactOwner

    return PreviewArtifactOwner(
        tenant_name=tenant_name,
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
    )


def test_preview_owner_token_is_full_sha256_of_canonical_identity() -> None:
    from core.preview.artifacts import preview_owner_token

    owner = _owner("production", "tenant-1")
    canonical = json.dumps(
        {
            "ecosystem": "confluent_cloud",
            "tenant_id": "tenant-1",
            "tenant_name": "production",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()

    assert preview_owner_token(owner) == hashlib.sha256(canonical).hexdigest()


def test_new_staging_is_owner_scoped_but_published_layout_is_unchanged(tmp_path: Path) -> None:
    from core.preview.artifacts import preview_owner_token

    owner = _owner("production", "tenant-1")
    store = LocalPreviewArtifactStore(tmp_path)
    staged = store.stage_data_files(
        owner=owner,
        request_id="request-1",
        data_files=(PreviewArtifactPayload(name="data.csv", media_type="text/csv", body=b"a\n1\n", order=1),),
    )

    owner_root = tmp_path / ".staging" / preview_owner_token(owner)
    assert len(tuple(owner_root.glob(".*.staging"))) == 1
    manifest = {
        "files": [
            {
                "name": item.name,
                "media_type": item.media_type,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "order": item.order,
            }
            for item in staged.files
        ]
    }
    package = staged.publish(manifest_body=json.dumps(manifest, separators=(",", ":")).encode())

    assert (tmp_path / package.storage_key / "data.csv").read_bytes() == b"a\n1\n"
    assert not (tmp_path / ".staging" / preview_owner_token(owner) / package.storage_key).exists()
    staged.close()


def test_cleanup_staging_never_scans_or_deletes_other_owner_or_legacy_entries(tmp_path: Path) -> None:
    from core.preview.artifacts import preview_owner_token

    enabled = _owner("enabled", "enabled-id")
    disabled = _owner("disabled", "disabled-id")
    enabled_root = tmp_path / ".staging" / preview_owner_token(enabled)
    disabled_root = tmp_path / ".staging" / preview_owner_token(disabled)
    enabled_staging = enabled_root / f".{('a' * 32)}.staging"
    disabled_staging = disabled_root / f".{('b' * 32)}.staging"
    legacy_staging = tmp_path / f".{('c' * 32)}.staging"
    enabled_staging.mkdir(parents=True)
    disabled_staging.mkdir(parents=True)
    legacy_staging.mkdir()
    store = LocalPreviewArtifactStore(tmp_path)

    assert store.cleanup_staging(enabled) == 1
    assert not enabled_staging.exists()
    assert disabled_staging.exists()
    assert legacy_staging.exists()

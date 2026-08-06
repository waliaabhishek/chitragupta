from __future__ import annotations

import hashlib
import json
from pathlib import Path

from core.preview.artifacts import LocalPreviewArtifactStore
from core.preview.models import PreviewArtifactPayload


def _owner(tenant_name: str, tenant_id: str, *, storage_fingerprint: str = "a" * 64):
    from core.preview.artifacts import PreviewArtifactOwner

    return PreviewArtifactOwner(
        tenant_name=tenant_name,
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        storage_backend_fingerprint=storage_fingerprint,
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


def test_new_staging_lock_and_final_layout_use_durable_storage_owner_token(tmp_path: Path) -> None:
    from core.preview.artifacts import preview_storage_owner_token

    owner = _owner("production", "tenant-1")
    store = LocalPreviewArtifactStore(tmp_path)
    staged = store.stage_data_files(
        owner=owner,
        request_id="request-1",
        data_files=(PreviewArtifactPayload(name="data.csv", media_type="text/csv", body=b"a\n1\n", order=1),),
    )

    owner_token = preview_storage_owner_token(owner)
    owner_root = tmp_path / ".staging" / "v1" / owner_token
    staging = tuple(owner_root.glob("*.staging"))
    locks = tuple((tmp_path / ".locks" / "v1" / owner_token).glob("*.lock"))
    assert len(staging) == 1
    assert len(locks) == 1
    package_id = staging[0].name.removesuffix(".staging")
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

    assert package.storage_key == f"v1-{owner_token}-{package_id}"
    assert (tmp_path / package.storage_key / "data.csv").read_bytes() == b"a\n1\n"
    assert not staging[0].exists()
    assert locks[0].is_file()
    staged.close()
    assert not locks[0].exists()


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


def test_storage_owner_token_survives_label_rename_and_isolates_same_provider_id_databases() -> None:
    from core.preview.artifacts import preview_storage_owner_token

    old_label = _owner("old-label", "shared-provider-id", storage_fingerprint="a" * 64)
    new_label = _owner("new-label", "shared-provider-id", storage_fingerprint="a" * 64)
    other_database = _owner("new-label", "shared-provider-id", storage_fingerprint="b" * 64)

    assert preview_storage_owner_token(old_label) == preview_storage_owner_token(new_label)
    assert preview_storage_owner_token(new_label) != preview_storage_owner_token(other_database)

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Protocol, runtime_checkable

from core.config.models import TenantConfig  # noqa: TC001 - resolved by runtime protocol tests
from core.preview.artifacts import (  # noqa: TC001 - resolved by runtime protocol tests
    PreviewArchiveStream,
    PreviewArtifactStore,
)
from core.preview.eligibility import policy_from_tenant_config
from core.preview.generator import PreviewGenerationError, PreviewPackageGenerator, utc_now
from core.preview.mapping import (
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    build_preview_revision_manifest,
    preview_revision_content_sha256,
    preview_revision_source_snapshot,
)
from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewRevision,
    PreviewRevisionCandidate,
    validate_preview_revision_invariant,
)
from core.preview.persistence import (
    PreviewRevisionConflictError,
    PreviewStorageBackend,  # noqa: TC001 - resolved by runtime protocol tests
)
from core.preview.request import canonicalize_monthly_interval

logger = logging.getLogger(__name__)

__all__ = ["PreviewRevisionConflictError"]


class PreviewRevisionArtifactUnavailableError(RuntimeError):
    """A stored revision package failed validation or retrieval."""


@runtime_checkable
class PreviewScheduledRevisionPublisher(Protocol):
    def publish_eligible_months(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        now: datetime,
    ) -> tuple[PreviewRevision, ...]: ...


@runtime_checkable
class PreviewCurrentRevisionReader(Protocol):
    def get_current(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        month_start: date,
    ) -> PreviewRevision | None: ...

    def read_manifest(self, revision: PreviewRevision) -> bytes: ...

    def read_file(
        self,
        revision: PreviewRevision,
        file_name: str,
    ) -> tuple[PreviewArtifactMetadata, bytes]: ...

    def open_archive(self, revision: PreviewRevision) -> PreviewArchiveStream: ...


def masked_preview_owner(*, ecosystem: str, tenant_id: str) -> str:
    digest = hashlib.sha256(f"{ecosystem}\0{tenant_id}".encode()).hexdigest()
    return f"owner:v1:{digest}"


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _month_start(value: date) -> date:
    return value.replace(day=1)


class PreviewRevisionService:
    def __init__(
        self,
        *,
        artifact_store: PreviewArtifactStore,
        package_generator: PreviewPackageGenerator,
        clock: Callable[[], datetime] = utc_now,
        revision_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
    ) -> None:
        self._artifact_store = artifact_store
        self._package_generator = package_generator
        self._clock = clock
        self._revision_id_factory = revision_id_factory
        self._staging_recovery_pending = True
        self._staging_recovery_lock = threading.Lock()

    def ensure_staging_recovered(self) -> None:
        if not self._staging_recovery_pending:
            return
        with self._staging_recovery_lock:
            if not self._staging_recovery_pending:
                return
            self._artifact_store.cleanup_staging()
            self._staging_recovery_pending = False

    def _eligible_months(self, tenant_config: TenantConfig, now: datetime) -> tuple[str, ...]:
        focus = tenant_config.focus_preview
        if focus is None:
            return ()
        policy = policy_from_tenant_config(tenant_config, created_at=now)
        start = max(_month_start(policy.acquisition_start_date), focus.effective_start_date)
        end = min(policy.acquisition_end_date, focus.effective_end_date)
        current = _month_start(start)
        if current < start:
            current = _next_month(current)
        months: list[str] = []
        while current < end:
            months.append(f"{current.year:04d}-{current.month:02d}")
            current = _next_month(current)
        return tuple(months)

    def publish_eligible_months(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        now: datetime,
    ) -> tuple[PreviewRevision, ...]:
        try:
            self.ensure_staging_recovered()
        except Exception as exc:
            logger.error(
                "FOCUS Mapping Preview revision staging recovery failed tenant=%s error_type=%s",
                tenant_name,
                type(exc).__name__,
            )
            return ()
        published: list[PreviewRevision] = []
        normalized_now = now.astimezone(UTC).replace(microsecond=0)
        policy = policy_from_tenant_config(tenant_config, created_at=normalized_now)
        for month in self._eligible_months(tenant_config, normalized_now):
            try:
                interval = canonicalize_monthly_interval(month=month)
                request = PreviewRequest(
                    request_id=f"revision-generation-{uuid.uuid4()}",
                    tenant_name=tenant_name,
                    ecosystem=tenant_config.ecosystem,
                    tenant_id=tenant_config.tenant_id,
                    grain="monthly",
                    start_date=interval.start_date,
                    end_date=interval.end_date,
                    column_profile="full",
                    status=PreviewRequestStatus.RUNNING,
                    created_at=normalized_now,
                    started_at=normalized_now,
                    completed_at=None,
                    expires_at=None,
                    source_snapshot=None,
                    diagnostic=None,
                    storage_key=None,
                    package=None,
                    effective_columns=FOCUS_1_4_FULL_PROFILE_COLUMNS,
                )
                snapshot, draft = self._package_generator.generate(
                    backend=backend,
                    request=request,
                    policy=policy,
                )
                if snapshot.monthly_status is None:
                    raise ValueError("scheduled monthly generation requires monthly status")
                material = preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256)
                with backend.create_preview_read_unit_of_work() as read_uow:
                    current = read_uow.revisions.get_current_for_owner(
                        ecosystem=tenant_config.ecosystem,
                        tenant_id=tenant_config.tenant_id,
                        month_start=interval.start_date,
                    )
                if current is not None:
                    if current.monthly_status == "settled" and snapshot.monthly_status == "provisional":
                        continue
                    if current.monthly_status == snapshot.monthly_status and current.material_sha256 == material:
                        continue
                revision_id = self._revision_id_factory()
                expected_current = None if current is None else current.revision_id
                candidate = PreviewRevisionCandidate(
                    revision_id=revision_id,
                    tenant_name_at_publication=tenant_name,
                    ecosystem=tenant_config.ecosystem,
                    tenant_id=tenant_config.tenant_id,
                    month=month,
                    start_date=interval.start_date,
                    end_date=interval.end_date,
                    monthly_status=snapshot.monthly_status,
                    material_sha256=material,
                    source_snapshot=snapshot,
                    published_at=self._clock().astimezone(UTC).replace(microsecond=0),
                    supersedes_revision_id=expected_current,
                )
                if candidate.supersedes_revision_id != expected_current:
                    raise ValueError("candidate supersedes identity does not match expected current revision")
                stored = None
                try:
                    with self._artifact_store.stage_data_files(
                        request_id=revision_id,
                        data_files=draft.data_files,
                    ) as staged:
                        manifest = build_preview_revision_manifest(
                            revision_id=candidate.revision_id,
                            tenant_name_at_publication=candidate.tenant_name_at_publication,
                            month=candidate.month,
                            start_date=candidate.start_date,
                            end_date=candidate.end_date,
                            monthly_status=candidate.monthly_status,
                            material_sha256=candidate.material_sha256,
                            supersedes_revision_id=candidate.supersedes_revision_id,
                            snapshot=candidate.source_snapshot,
                            draft=draft,
                            files=staged.files,
                            published_at=candidate.published_at,
                        )
                        stored = staged.publish(manifest_body=manifest)
                    with backend.create_preview_write_unit_of_work() as write_uow:
                        revision = write_uow.revisions.replace_current(
                            candidate=candidate,
                            package=stored,
                            expected_current_revision_id=expected_current,
                        )
                        write_uow.commit()
                    published.append(revision)
                except Exception as publication_error:
                    if stored is not None:
                        try:
                            self._artifact_store.delete_package(storage_key=stored.storage_key)
                        except Exception as cleanup_error:
                            logger.error(
                                "FOCUS Mapping Preview revision candidate cleanup failed "
                                "publication_error_type=%s cleanup_error_type=%s",
                                type(publication_error).__name__,
                                type(cleanup_error).__name__,
                            )
                    raise
            except PreviewGenerationError as exc:
                logger.warning(
                    "FOCUS Mapping Preview revision generation skipped tenant=%s month=%s diagnostic_code=%s",
                    tenant_name,
                    month,
                    exc.diagnostic.code,
                )
            except Exception as exc:
                logger.error(
                    "FOCUS Mapping Preview revision publication failed tenant=%s month=%s error_type=%s",
                    tenant_name,
                    month,
                    type(exc).__name__,
                )
        return tuple(published)


def _artifact_declarations(revision: PreviewRevision) -> list[dict[str, object]]:
    return [
        {
            "name": item.name,
            "media_type": item.media_type,
            "size_bytes": item.size_bytes,
            "sha256": item.sha256,
            "order": item.order,
        }
        for item in revision.package.files
    ]


class PreviewRevisionReadService:
    def __init__(self, *, artifact_store: PreviewArtifactStore) -> None:
        self._artifact_store = artifact_store

    def get_current(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        month_start: date,
    ) -> PreviewRevision | None:
        with backend.create_preview_read_unit_of_work() as uow:
            return uow.revisions.get_current_for_owner(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                month_start=month_start,
            )

    def _validated_manifest(self, revision: PreviewRevision) -> bytes:
        try:
            validate_preview_revision_invariant(
                month=revision.month,
                start_date=revision.start_date,
                end_date=revision.end_date,
                monthly_status=revision.monthly_status,
                source_snapshot=revision.source_snapshot,
            )
            body = self._artifact_store.read_manifest(
                revision.package.storage_key,
                revision.package.manifest,
            )
            manifest = json.loads(body)
            if not isinstance(manifest, dict):
                raise ValueError("manifest must be an object")
            for name in ("mapping_profile_version", "target_focus_version", "column_profile"):
                if not isinstance(manifest.get(name), str):
                    raise ValueError("invalid material preimage")
            columns = manifest.get("effective_columns")
            if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
                raise ValueError("invalid material preimage")
            logical = manifest.get("logical_data_sha256")
            manifest_material = manifest.get("material_sha256")
            if not isinstance(logical, str) or re.fullmatch(r"[0-9a-f]{64}", logical) is None:
                raise ValueError("invalid material preimage")
            if not isinstance(manifest_material, str) or re.fullmatch(r"[0-9a-f]{64}", manifest_material) is None:
                raise ValueError("invalid material digest")
            recomputed = preview_revision_content_sha256(
                mapping_profile_version=manifest["mapping_profile_version"],
                target_focus_version=manifest["target_focus_version"],
                column_profile=manifest["column_profile"],
                effective_columns=tuple(columns),
                logical_data_sha256=logical,
            )
            if recomputed != manifest_material or recomputed != revision.material_sha256:
                raise ValueError("material digest mismatch")
            expected = {
                "revision_id": revision.revision_id,
                "tenant_name": revision.tenant_name_at_publication,
                "grain": "monthly",
                "month": revision.month,
                "start_date": revision.start_date.isoformat(),
                "end_date": revision.end_date.isoformat(),
                "monthly_status": revision.monthly_status,
                "supersedes_revision_id": revision.supersedes_revision_id,
                "published_at": revision.published_at.isoformat().replace("+00:00", "Z"),
                "source_snapshot": preview_revision_source_snapshot(revision.source_snapshot),
                "files": _artifact_declarations(revision),
            }
            for key, value in expected.items():
                if manifest.get(key) != value:
                    raise ValueError("manifest correlation mismatch")
            return body
        except PreviewRevisionArtifactUnavailableError:
            raise
        except Exception:
            raise PreviewRevisionArtifactUnavailableError(
                "Stored FOCUS Mapping Preview revision artifact is unavailable"
            ) from None

    def read_manifest(self, revision: PreviewRevision) -> bytes:
        return self._validated_manifest(revision)

    def read_file(
        self,
        revision: PreviewRevision,
        file_name: str,
    ) -> tuple[PreviewArtifactMetadata, bytes]:
        self._validated_manifest(revision)
        metadata = next((item for item in revision.package.files if item.name == file_name), None)
        if metadata is None:
            raise FileNotFoundError("FOCUS Mapping Preview file not found for current revision")
        try:
            return metadata, self._artifact_store.read_file(revision.package.storage_key, metadata)
        except Exception:
            raise PreviewRevisionArtifactUnavailableError(
                "Stored FOCUS Mapping Preview revision artifact is unavailable"
            ) from None

    def open_archive(self, revision: PreviewRevision) -> PreviewArchiveStream:
        self._validated_manifest(revision)
        try:
            return self._artifact_store.open_archive(
                storage_key=revision.package.storage_key,
                manifest=revision.package.manifest,
                files=revision.package.files,
            )
        except Exception:
            raise PreviewRevisionArtifactUnavailableError(
                "Stored FOCUS Mapping Preview revision artifact is unavailable"
            ) from None

from __future__ import annotations

import hashlib
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Protocol, runtime_checkable

from core.config.models import TenantConfig  # noqa: TC001 - resolved by runtime protocol tests
from core.preview.artifacts import (  # noqa: TC001 - resolved by runtime protocol tests
    PreviewArchiveStream,
    PreviewArtifactOwner,
    PreviewGenerationArtifactStore,
    PreviewStreamingArtifactStore,
    PreviewVerifiedArtifactStream,
    find_preview_artifact_metadata,
    preview_artifact_owner,
)
from core.preview.eligibility import policy_from_tenant_config
from core.preview.focus_metadata import (
    build_revision_focus_metadata_artifact,
    compose_package_artifacts,
    validate_revision_focus_metadata_artifact,
)
from core.preview.generator import PreviewGenerationError, PreviewPackageGenerator, utc_now
from core.preview.manifest_validation import validate_revision_manifest
from core.preview.mapping import (
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    build_preview_revision_manifest,
    preview_revision_content_sha256,
)
from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewRevision,
    PreviewRevisionCandidate,
    PreviewRevisionValidationSummary,
    resolve_monthly_evidence,
    validate_preview_revision_invariant,
)
from core.preview.persistence import (
    PreviewRetentionCandidate,
    PreviewRevisionConflictError,
    PreviewRevisionPage,
    PreviewStorageBackend,  # noqa: TC001 - resolved by runtime protocol tests
)
from core.preview.request import canonicalize_monthly_interval
from core.preview.spooling import PreviewGenerationSpoolLimitError

logger = logging.getLogger(__name__)

__all__ = ["PreviewRevisionConflictError"]


class PreviewRevisionArtifactUnavailableError(RuntimeError):
    """A stored revision package failed validation or retrieval."""


@dataclass(frozen=True)
class PreviewRevisionCleanupResult:
    claimed_count: int
    deleted_count: int
    deferred_count: int

    def __post_init__(self) -> None:
        for field, value in (
            ("claimed_count", self.claimed_count),
            ("deleted_count", self.deleted_count),
            ("deferred_count", self.deferred_count),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{field} must be a non-negative integer")


@runtime_checkable
class PreviewScheduledRevisionManager(Protocol):
    def eligible_months(
        self,
        *,
        tenant_config: TenantConfig,
        now: datetime,
    ) -> tuple[str, ...]: ...

    def publish_eligible_months(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        now: datetime,
    ) -> tuple[PreviewRevision, ...]: ...

    def publish_eligible_month(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        now: datetime,
        month: str,
    ) -> PreviewRevision | None: ...

    def cleanup_retention(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        now: datetime,
    ) -> PreviewRevisionCleanupResult: ...


@runtime_checkable
class PreviewRevisionReader(Protocol):
    def get_current(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        month_start: date,
    ) -> PreviewRevision | None: ...

    def get_for_owner(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        revision_id: str,
    ) -> PreviewRevision | None: ...

    def list_for_owner_month(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        month_start: date,
        limit: int,
        cursor_revision_id: str | None,
    ) -> PreviewRevisionPage: ...

    def validation_summary(
        self,
        *,
        revision: PreviewRevision,
    ) -> PreviewRevisionValidationSummary: ...

    def read_manifest(self, *, revision: PreviewRevision) -> bytes: ...

    def read_file(
        self,
        *,
        revision: PreviewRevision,
        file_name: str,
    ) -> tuple[PreviewArtifactMetadata, bytes]: ...

    def open_manifest_stream(
        self,
        *,
        revision: PreviewRevision,
    ) -> PreviewVerifiedArtifactStream: ...

    def open_file_stream(
        self,
        *,
        revision: PreviewRevision,
        file_name: str,
    ) -> tuple[PreviewArtifactMetadata, PreviewVerifiedArtifactStream]: ...

    def open_archive(self, *, revision: PreviewRevision) -> PreviewArchiveStream: ...


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
    RETENTION_ATTEMPT_LIMIT = 100
    RETENTION_LANE_RESERVE = 50

    def __init__(
        self,
        *,
        artifact_store: PreviewGenerationArtifactStore,
        package_generator: PreviewPackageGenerator,
        clock: Callable[[], datetime] = utc_now,
        revision_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
    ) -> None:
        self._artifact_store = artifact_store
        self._package_generator = package_generator
        self._clock = clock
        self._revision_id_factory = revision_id_factory

    def _recover_artifacts(
        self,
        *,
        owner: PreviewArtifactOwner,
        backend: PreviewStorageBackend,
    ) -> None:
        self._artifact_store.cleanup_staging(owner)
        with backend.create_preview_metadata_read_unit_of_work() as read_uow:
            references = frozenset(
                read_uow.artifact_references.list_for_owner(
                    ecosystem=owner.ecosystem,
                    tenant_id=owner.tenant_id,
                )
            )

        def is_referenced(storage_key: str) -> bool:
            with backend.create_preview_metadata_read_unit_of_work() as read_uow:
                return read_uow.artifact_references.is_referenced(
                    ecosystem=owner.ecosystem,
                    tenant_id=owner.tenant_id,
                    storage_key=storage_key,
                )

        self._artifact_store.reconcile_finalized(
            owner=owner,
            referenced_storage_keys=references,
            is_referenced=is_referenced,
        )

    def _eligible_months(self, tenant_config: TenantConfig, now: datetime) -> tuple[str, ...]:
        focus = tenant_config.focus_preview
        if focus is None:
            return ()
        policy = policy_from_tenant_config(tenant_config, created_at=now)
        assert policy.effective_start_date is not None
        assert policy.effective_end_date is not None
        start = max(
            _month_start(policy.acquisition_start_date),
            policy.effective_start_date,
        )
        end = min(
            policy.acquisition_end_date,
            policy.effective_end_date,
        )
        current = _month_start(start)
        if current < start:
            current = _next_month(current)
        months: list[str] = []
        while current < end:
            next_month = _next_month(current)
            resolution = resolve_monthly_evidence(
                start_date=current,
                end_date=next_month,
                submitted_at=now,
                availability_cutoff_end_date=policy.acquisition_end_date,
            )
            if resolution.monthly_stage == "settlement_candidate":
                months.append(f"{current.year:04d}-{current.month:02d}")
            current = next_month
        return tuple(months)

    def eligible_months(
        self,
        *,
        tenant_config: TenantConfig,
        now: datetime,
    ) -> tuple[str, ...]:
        return self._eligible_months(tenant_config, now.astimezone(UTC).replace(microsecond=0))

    def publish_eligible_months(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        now: datetime,
        _eligible_months: tuple[str, ...] | None = None,
    ) -> tuple[PreviewRevision, ...]:
        owner = preview_artifact_owner(tenant_name, tenant_config)
        normalized_now = now.astimezone(UTC).replace(microsecond=0)
        eligible_months = (
            self._eligible_months(tenant_config, normalized_now) if _eligible_months is None else _eligible_months
        )
        if not eligible_months:
            return ()
        try:
            self._recover_artifacts(owner=owner, backend=backend)
        except Exception as exc:
            logger.error(
                "FOCUS Mapping Preview revision publication failed tenant=%s month=%s error_type=%s",
                tenant_name,
                eligible_months[0],
                type(exc).__name__,
            )
            return ()
        published: list[PreviewRevision] = []
        policy = policy_from_tenant_config(tenant_config, created_at=normalized_now)
        for month in eligible_months:
            draft = None
            generation = None
            try:
                interval = canonicalize_monthly_interval(month=month)
                cutoff_date = (normalized_now - timedelta(days=tenant_config.retention_days)).date()
                if interval.end_date <= cutoff_date:
                    continue
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
                generation = self._artifact_store.begin_generation(
                    owner=owner,
                    request_id=request.request_id,
                    max_spool_bytes=self._package_generator.max_generation_spool_bytes,
                )
                snapshot, draft = self._package_generator.generate(
                    backend=backend,
                    request=request,
                    policy=policy,
                    workspace=generation.workspace,
                )
                if snapshot.monthly_status != "settled":
                    raise ValueError("scheduled monthly generation requires settled status")
                material = preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256)
                with backend.create_preview_metadata_read_unit_of_work() as read_uow:
                    current = read_uow.revisions.get_current_for_publication(
                        ecosystem=tenant_config.ecosystem,
                        tenant_id=tenant_config.tenant_id,
                        month_start=interval.start_date,
                    )
                if current is not None and current.retention_pending_at is not None:
                    continue
                if (
                    current is not None
                    and current.monthly_status == snapshot.monthly_status
                    and current.material_sha256 == material
                ):
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
                    data_files = tuple(draft.data_files)
                    focus_metadata = build_revision_focus_metadata_artifact(
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
                        data_files=data_files,
                        published_at=candidate.published_at,
                    )
                    package_files = compose_package_artifacts(
                        data_files=data_files,
                        focus_metadata=focus_metadata,
                    )
                    generation.stage_data_files(package_files)
                    staged_files = generation.files
                    validate_revision_focus_metadata_artifact(
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
                        package_files=package_files,
                        staged_files=staged_files,
                        published_at=candidate.published_at,
                    )
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
                        files=staged_files,
                        published_at=candidate.published_at,
                    )
                    stored = generation.publish(manifest_body=manifest)
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
                            generation.close()
                            generation = None
                            self._recover_artifacts(owner=owner, backend=backend)
                        except Exception as cleanup_error:
                            logger.error(
                                "FOCUS Mapping Preview revision candidate cleanup failed "
                                "publication_error_type=%s cleanup_error_type=%s",
                                type(publication_error).__name__,
                                type(cleanup_error).__name__,
                            )
                    raise
            except (PreviewGenerationError, PreviewGenerationSpoolLimitError) as exc:
                diagnostic_code = (
                    exc.diagnostic.code
                    if isinstance(exc, PreviewGenerationError)
                    else "preview_generation_spool_limit_exceeded"
                )
                logger.warning(
                    "FOCUS Mapping Preview revision generation skipped tenant=%s month=%s diagnostic_code=%s",
                    tenant_name,
                    month,
                    diagnostic_code,
                )
            except Exception as exc:
                logger.error(
                    "FOCUS Mapping Preview revision publication failed tenant=%s month=%s error_type=%s",
                    tenant_name,
                    month,
                    type(exc).__name__,
                )
            finally:
                if generation is not None:
                    try:
                        generation.close()
                    except OSError:
                        logger.exception(
                            "FOCUS Mapping Preview revision generation workspace cleanup failed tenant=%s month=%s",
                            tenant_name,
                            month,
                        )
                if draft is not None:
                    try:
                        draft.close()
                    except OSError:
                        logger.exception(
                            "FOCUS Mapping Preview revision draft workspace cleanup failed tenant=%s month=%s",
                            tenant_name,
                            month,
                        )
        return tuple(published)

    def publish_eligible_month(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        now: datetime,
        month: str,
    ) -> PreviewRevision | None:
        if month not in self.eligible_months(tenant_config=tenant_config, now=now):
            return None
        published = self.publish_eligible_months(
            tenant_name=tenant_name,
            tenant_config=tenant_config,
            backend=backend,
            now=now,
            _eligible_months=(month,),
        )
        return published[0] if published else None

    def cleanup_retention(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        now: datetime,
    ) -> PreviewRevisionCleanupResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        normalized_now = now.astimezone(UTC)
        ecosystem = tenant_config.ecosystem
        tenant_id = tenant_config.tenant_id
        cutoff_date = (normalized_now - timedelta(days=tenant_config.retention_days)).date()
        owner = preview_artifact_owner(tenant_name, tenant_config)
        try:
            self._recover_artifacts(owner=owner, backend=backend)
        except Exception as exc:
            logger.error(
                "FOCUS Mapping Preview revision recovery failed tenant=%s error_type=%s",
                tenant_name,
                type(exc).__name__,
            )
            return PreviewRevisionCleanupResult(
                claimed_count=0,
                deleted_count=0,
                deferred_count=0,
            )

        with backend.create_preview_metadata_read_unit_of_work() as read_uow:
            retry_snapshot = read_uow.revisions.list_retention_pending(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                limit=self.RETENTION_ATTEMPT_LIMIT,
            )
        retry_reserve = min(self.RETENTION_LANE_RESERVE, len(retry_snapshot))
        new_capacity = self.RETENTION_ATTEMPT_LIMIT - retry_reserve
        with backend.create_preview_write_unit_of_work() as write_uow:
            new_candidates = write_uow.revisions.mark_retention_due(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                cutoff_date=cutoff_date,
                pending_at=normalized_now,
                limit=new_capacity,
            )
            write_uow.commit()

        retry_capacity = self.RETENTION_ATTEMPT_LIMIT - len(new_candidates)
        retries = retry_snapshot[:retry_capacity]
        attempts: list[PreviewRetentionCandidate] = []
        retry_iter = iter(retries)
        new_iter = iter(new_candidates)
        while True:
            retry_candidate = next(retry_iter, None)
            new_candidate = next(new_iter, None)
            if retry_candidate is None and new_candidate is None:
                break
            if retry_candidate is not None:
                attempts.append(retry_candidate)
            if new_candidate is not None:
                attempts.append(new_candidate)

        deleted_count = 0
        deferred_count = 0
        for candidate in attempts:
            try:
                self._artifact_store.delete_package(storage_key=candidate.storage_key)
                with backend.create_preview_write_unit_of_work() as write_uow:
                    deleted = write_uow.revisions.delete_retention_pending(candidate=candidate)
                    if not deleted:
                        raise PreviewRevisionConflictError("retention candidate changed")
                    write_uow.commit()
                deleted_count += 1
            except Exception as exc:
                logger.error(
                    "FOCUS Mapping Preview revision retention deferred tenant=%s owner=%s revision_id=%s error_type=%s",
                    tenant_name,
                    masked_preview_owner(ecosystem=ecosystem, tenant_id=tenant_id),
                    candidate.revision_id,
                    type(exc).__name__,
                )
                try:
                    with backend.create_preview_write_unit_of_work() as write_uow:
                        deferred = write_uow.revisions.defer_retention_pending(
                            candidate=candidate,
                        )
                        write_uow.commit()
                    if deferred:
                        deferred_count += 1
                except Exception as defer_error:
                    logger.error(
                        "FOCUS Mapping Preview revision retention deferral failed tenant=%s owner=%s "
                        "revision_id=%s error_type=%s",
                        tenant_name,
                        masked_preview_owner(ecosystem=ecosystem, tenant_id=tenant_id),
                        candidate.revision_id,
                        type(defer_error).__name__,
                    )

        return PreviewRevisionCleanupResult(
            claimed_count=len(new_candidates),
            deleted_count=deleted_count,
            deferred_count=deferred_count,
        )


class PreviewRevisionReadService:
    def __init__(self, *, artifact_store: PreviewStreamingArtifactStore) -> None:
        self._artifact_store = artifact_store

    def get_current(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        month_start: date,
    ) -> PreviewRevision | None:
        with backend.create_preview_metadata_read_unit_of_work() as uow:
            return uow.revisions.get_current_for_owner(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                month_start=month_start,
            )

    def get_for_owner(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        revision_id: str,
    ) -> PreviewRevision | None:
        with backend.create_preview_metadata_read_unit_of_work() as uow:
            return uow.revisions.get_for_owner(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                revision_id=revision_id,
            )

    def list_for_owner_month(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        month_start: date,
        limit: int,
        cursor_revision_id: str | None,
    ) -> PreviewRevisionPage:
        with backend.create_preview_metadata_read_unit_of_work() as uow:
            return uow.revisions.list_for_owner_month(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                month_start=month_start,
                limit=limit,
                cursor_revision_id=cursor_revision_id,
            )

    def _open_validated_manifest(
        self,
        revision: PreviewRevision,
    ) -> tuple[PreviewVerifiedArtifactStream, PreviewRevisionValidationSummary]:
        stream: PreviewVerifiedArtifactStream | None = None
        try:
            validate_preview_revision_invariant(
                month=revision.month,
                start_date=revision.start_date,
                end_date=revision.end_date,
                monthly_status=revision.monthly_status,
                source_snapshot=revision.source_snapshot,
            )
            stream = self._artifact_store.open_verified(
                revision.package.storage_key,
                revision.package.manifest,
            )
            return stream, validate_revision_manifest(stream, revision)
        except PreviewRevisionArtifactUnavailableError:
            if stream is not None:
                stream.close()
            raise
        except Exception:
            if stream is not None:
                stream.close()
            raise PreviewRevisionArtifactUnavailableError(
                "Stored FOCUS Mapping Preview revision artifact is unavailable"
            ) from None

    def validation_summary(self, *, revision: PreviewRevision) -> PreviewRevisionValidationSummary:
        stream, summary = self._open_validated_manifest(revision)
        stream.close()
        return summary

    def read_manifest(self, *, revision: PreviewRevision) -> bytes:
        stream = self.open_manifest_stream(revision=revision)
        with stream:
            return b"".join(stream.iter_chunks())

    def open_manifest_stream(self, *, revision: PreviewRevision) -> PreviewVerifiedArtifactStream:
        stream, _summary = self._open_validated_manifest(revision)
        return stream

    def read_file(
        self,
        *,
        revision: PreviewRevision,
        file_name: str,
    ) -> tuple[PreviewArtifactMetadata, bytes]:
        metadata, stream = self.open_file_stream(revision=revision, file_name=file_name)
        with stream:
            return metadata, b"".join(stream.iter_chunks())

    def open_file_stream(
        self,
        *,
        revision: PreviewRevision,
        file_name: str,
    ) -> tuple[PreviewArtifactMetadata, PreviewVerifiedArtifactStream]:
        manifest_stream, _summary = self._open_validated_manifest(revision)
        manifest_stream.close()
        metadata = find_preview_artifact_metadata(revision.package.files, file_name)
        if metadata is None:
            raise FileNotFoundError("FOCUS Mapping Preview file not found for current revision")
        try:
            return (
                metadata,
                self._artifact_store.open_verified(revision.package.storage_key, metadata),
            )
        except Exception:
            raise PreviewRevisionArtifactUnavailableError(
                "Stored FOCUS Mapping Preview revision artifact is unavailable"
            ) from None

    def open_archive(self, *, revision: PreviewRevision) -> PreviewArchiveStream:
        manifest_stream, _summary = self._open_validated_manifest(revision)
        manifest_stream.close()
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

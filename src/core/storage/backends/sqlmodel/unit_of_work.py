from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Self

from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

from core.logging_context import safe_exception_context, safe_log_context
from core.storage.backends.sqlmodel.engine import get_or_create_engine, get_or_create_read_only_engine
from core.storage.backends.sqlmodel.repositories import (
    SQLModelEmissionRepository,
    SQLModelEntityTagRepository,
    SQLModelGraphRepository,
    SQLModelPipelineRunRepository,
    SQLModelPipelineStateRepository,
)
from core.storage.backends.sqlmodel.repositories import (
    TopicAttributionRepository as TopicAttributionRepositoryImpl,
)

if TYPE_CHECKING:
    from core.emitters.repository import EmissionRepository
    from core.plugin.protocols import StorageModule
    from core.preview.persistence import (
        PreviewArtifactReferenceRepository,
        PreviewEvidenceBootstrap,
        PreviewEvidenceWriteUnitOfWork,
        PreviewGenerationReadUnitOfWork,
        PreviewRequestRepository,
        PreviewRevisionRepository,
    )
    from core.preview.storage_availability import PreviewEvidenceAvailability, PreviewEvidenceIssue
    from core.storage.interface import (
        BillingRepository,
        ChargebackRepository,
        EntityTagRepository,
        GraphRepository,
        IdentityRepository,
        PipelineRunRepository,
        PipelineStateRepository,
        ResourceRepository,
        TopicAttributionRepository,
    )

logger = logging.getLogger(__name__)


class SQLModelUnitOfWork:
    """SQLModel implementation of UnitOfWork protocol."""

    def __init__(
        self,
        connection_string: str,
        storage_module: StorageModule,
        *,
        preview_evidence_enabled: bool = False,
    ) -> None:
        self._engine = get_or_create_engine(connection_string)
        self._storage_module = storage_module
        self._session: Session | None = None
        self.preview_evidence_enabled = preview_evidence_enabled
        from core.plugin.protocols import PreviewSourceAttemptFallbackStorageModule

        self._preview_source_fallback_module = (
            storage_module
            if preview_evidence_enabled and isinstance(storage_module, PreviewSourceAttemptFallbackStorageModule)
            else None
        )
        # Initialized to None; overridden in __enter__ with real repo instances.
        # Must be assigned (not just annotated) so isinstance(self, UnitOfWork) works
        # outside a context block (UnitOfWork is @runtime_checkable Protocol).
        self.resources: ResourceRepository = None  # type: ignore[assignment]
        self.identities: IdentityRepository = None  # type: ignore[assignment]
        self.tags: EntityTagRepository = None  # type: ignore[assignment]
        self.billing: BillingRepository = None  # type: ignore[assignment]
        self.chargebacks: ChargebackRepository = None  # type: ignore[assignment]
        self.pipeline_state: PipelineStateRepository = None  # type: ignore[assignment]
        self.pipeline_runs: PipelineRunRepository = None  # type: ignore[assignment]
        self.emissions: EmissionRepository = None  # type: ignore[assignment]
        self.graph: GraphRepository = None  # type: ignore[assignment]
        self._topic_attributions: TopicAttributionRepositoryImpl | None = None

    def __enter__(self) -> Self:
        self._session = Session(self._engine)
        self._committed = False
        self._topic_attributions = None
        self.resources = self._storage_module.create_resource_repository(self._session)
        self.identities = self._storage_module.create_identity_repository(self._session)
        self.billing = self._storage_module.create_billing_repository(self._session)
        self.chargebacks = self._storage_module.create_chargeback_repository(self._session)  # plugin-extensible
        self.pipeline_state = SQLModelPipelineStateRepository(self._session)
        self.pipeline_runs = SQLModelPipelineRunRepository(self._session)
        self.tags = SQLModelEntityTagRepository(self._session)
        self.graph = SQLModelGraphRepository(self._session, self.tags)
        self.emissions = SQLModelEmissionRepository(self._session)
        from core.plugin.protocols import UnitOfWorkRepositoryAttachment

        if isinstance(self._storage_module, UnitOfWorkRepositoryAttachment):
            self._storage_module.attach_unit_of_work_repositories(self, self._session)
        if self._preview_source_fallback_module is not None:
            self.source_attempt_fallback = (
                self._preview_source_fallback_module.create_preview_source_attempt_fallback_repository(self._session)
            )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._session is None:
            return
        try:
            if not self._committed:
                self._session.rollback()
        finally:
            self._session.close()
            self._session = None

    def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("Cannot commit outside of a transaction")
        self._session.commit()
        self._committed = True

    def rollback(self) -> None:
        if self._session is None:
            raise RuntimeError("Cannot rollback outside of a transaction")
        self._session.rollback()

    @property
    def topic_attributions(self) -> TopicAttributionRepository:
        if self._topic_attributions is None:
            if self._session is None:
                raise RuntimeError("Cannot access topic_attributions outside of a transaction")
            self._topic_attributions = TopicAttributionRepositoryImpl(self._session)
        return self._topic_attributions

    @topic_attributions.setter
    def topic_attributions(self, value: TopicAttributionRepository) -> None:
        self._topic_attributions = value  # type: ignore[assignment]


class ReadOnlySQLModelUnitOfWork(SQLModelUnitOfWork):
    """Read-only UnitOfWork backed by the query_only engine.

    commit() raises RuntimeError as defense-in-depth: accidentally calling
    commit() on the default read-only dependency fails loudly at dev-time
    rather than silently acquiring a write lock at runtime.
    """

    def __init__(self, connection_string: str, storage_module: StorageModule) -> None:
        super().__init__(connection_string, storage_module, preview_evidence_enabled=False)
        self._engine = get_or_create_read_only_engine(connection_string)

    def commit(self) -> None:
        raise RuntimeError("Cannot commit on a read-only UnitOfWork — use get_write_unit_of_work dependency")


class PreviewWriteSQLModelUnitOfWork:
    def __init__(self, connection_string: str) -> None:
        self._engine = get_or_create_engine(connection_string)
        self._session: Session | None = None
        self.requests: PreviewRequestRepository = None  # type: ignore[assignment]
        self.revisions: PreviewRevisionRepository = None  # type: ignore[assignment]

    def __enter__(self) -> Self:
        from core.preview.persistence import SQLModelPreviewRequestRepository, SQLModelPreviewRevisionRepository

        self._session = Session(self._engine)
        self._committed = False
        self.requests = SQLModelPreviewRequestRepository(self._session)
        self.revisions = SQLModelPreviewRevisionRepository(self._session)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        if self._session is not None:
            try:
                if not self._committed:
                    self._session.rollback()
            finally:
                self._session.close()
                self._session = None

    def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("Cannot commit outside of a transaction")
        self._session.commit()
        self._committed = True

    def rollback(self) -> None:
        if self._session is None:
            raise RuntimeError("Cannot rollback outside of a transaction")
        self._session.rollback()


class PreviewMetadataReadSQLModelUnitOfWork:
    def __init__(self, connection_string: str) -> None:
        self._engine = get_or_create_read_only_engine(connection_string)
        self._session: Session | None = None
        self.requests: PreviewRequestRepository = None  # type: ignore[assignment]
        self.revisions: PreviewRevisionRepository = None  # type: ignore[assignment]
        self.artifact_references: PreviewArtifactReferenceRepository = None  # type: ignore[assignment]

    def __enter__(self) -> Self:
        from core.preview.persistence import (
            SQLModelPreviewArtifactReferenceRepository,
            SQLModelPreviewRequestRepository,
            SQLModelPreviewRevisionRepository,
        )

        self._session = Session(self._engine)
        self.requests = SQLModelPreviewRequestRepository(self._session)
        self.revisions = SQLModelPreviewRevisionRepository(self._session)
        self.artifact_references = SQLModelPreviewArtifactReferenceRepository(self._session)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None


class SQLModelBackend:
    """SQLModel implementation of StorageBackend protocol."""

    def __init__(
        self,
        connection_string: str,
        storage_module: StorageModule,
        *,
        use_migrations: bool = True,
        focus_preview_enabled: bool = False,
    ) -> None:
        self._connection_string = connection_string
        self._storage_module = storage_module
        self._use_migrations = use_migrations
        self._focus_preview_enabled = focus_preview_enabled
        self._engine = get_or_create_engine(connection_string)
        self._ro_engine = get_or_create_read_only_engine(connection_string)
        self._tables_created = False
        from core.preview.storage_availability import (
            PreviewEvidenceAvailability,
            PreviewEvidenceAvailabilityState,
        )

        self._preview_evidence_availability = PreviewEvidenceAvailability(PreviewEvidenceAvailabilityState.UNAVAILABLE)

    def create_unit_of_work(self) -> SQLModelUnitOfWork:
        return SQLModelUnitOfWork(
            self._connection_string,
            self._storage_module,
            preview_evidence_enabled=self._focus_preview_enabled,
        )

    def create_read_only_unit_of_work(self) -> ReadOnlySQLModelUnitOfWork:
        return ReadOnlySQLModelUnitOfWork(self._connection_string, self._storage_module)

    def create_preview_write_unit_of_work(self) -> PreviewWriteSQLModelUnitOfWork:
        return PreviewWriteSQLModelUnitOfWork(self._connection_string)

    def create_preview_metadata_read_unit_of_work(self) -> PreviewMetadataReadSQLModelUnitOfWork:
        return PreviewMetadataReadSQLModelUnitOfWork(self._connection_string)

    @property
    def preview_evidence_availability(self) -> PreviewEvidenceAvailability:
        return self._preview_evidence_availability

    def create_preview_evidence_unit_of_work(self) -> PreviewEvidenceWriteUnitOfWork:
        from core.plugin.protocols import PreviewEvidenceStorageModule
        from core.preview.storage_availability import PreviewEvidenceUnavailableError

        if not self._focus_preview_enabled or not isinstance(self._storage_module, PreviewEvidenceStorageModule):
            raise PreviewEvidenceUnavailableError("FOCUS Mapping Preview evidence storage is unavailable")
        return self._storage_module.create_preview_evidence_unit_of_work(
            self._connection_string,
            self._preview_evidence_availability,
        )

    def create_preview_generation_read_unit_of_work(self) -> PreviewGenerationReadUnitOfWork:
        from core.plugin.protocols import PreviewEvidenceStorageModule
        from core.preview.storage_availability import PreviewEvidenceUnavailableError

        if not self._focus_preview_enabled or not isinstance(self._storage_module, PreviewEvidenceStorageModule):
            raise PreviewEvidenceUnavailableError("FOCUS Mapping Preview evidence storage is unavailable")
        return self._storage_module.create_preview_generation_read_unit_of_work(
            self._connection_string,
            self._preview_evidence_availability,
        )

    def create_preview_evidence_bootstrap(self) -> PreviewEvidenceBootstrap:
        from core.plugin.protocols import PreviewEvidenceStorageModule
        from core.preview.storage_availability import PreviewEvidenceUnavailableError

        if not self._focus_preview_enabled or not isinstance(self._storage_module, PreviewEvidenceStorageModule):
            raise PreviewEvidenceUnavailableError("FOCUS Mapping Preview evidence storage is unavailable")
        return self._storage_module.create_preview_evidence_bootstrap(self)

    def mark_preview_evidence_bootstrap_unavailable(self, error_type: str) -> None:
        from core.preview.storage_availability import (
            PreviewEvidenceAvailability,
            PreviewEvidenceAvailabilityState,
            PreviewEvidenceIssue,
            PreviewEvidenceIssueKind,
        )

        if not error_type.strip():
            raise ValueError("bootstrap unavailability error type must not be blank")
        issue = PreviewEvidenceIssue(
            revision="bootstrap",
            kind=PreviewEvidenceIssueKind.BOOTSTRAP_FAILED,
            error_type=error_type,
        )
        self._preview_evidence_availability = PreviewEvidenceAvailability(
            PreviewEvidenceAvailabilityState.UNAVAILABLE,
            tuple(dict.fromkeys((*self._preview_evidence_availability.issues, issue))),
        )

    def create_tables(self) -> None:
        if self._tables_created:
            return
        if self._use_migrations:
            issues = self._run_migrations()
        else:
            from core.storage.backends.sqlmodel.module import CoreStorageModule

            # Always create core orchestration tables (chargeback, pipeline, etc.)
            # before the plugin registers its own tables.
            CoreStorageModule().register_tables(self._engine)
            self._storage_module.register_tables(self._engine)
            issues = ()
            logger.warning(
                "migration_degraded storage_module=%s%s",
                type(self._storage_module).__name__,
                safe_log_context(
                    stage="storage_migration",
                    operation="direct_table_registration",
                    outcome="direct_table_registration",
                    retryable=False,
                ),
            )
        self._prepare_preview_evidence(issues)
        self._tables_created = True

    def _run_migrations(self) -> tuple[PreviewEvidenceIssue, ...]:
        import pathlib

        from alembic import command
        from alembic.config import Config

        from core.plugin.protocols import PreviewEvidenceStorageModule
        from core.preview.storage_availability import (
            CFG_PREVIEW_EVIDENCE_ENABLED,
            CFG_PREVIEW_EVIDENCE_ISSUES,
            CFG_PREVIEW_EVIDENCE_MODULE,
            PreviewEvidenceIssue,
            PreviewEvidenceIssueCollector,
            PreviewEvidenceIssueKind,
        )
        from core.storage.migrations.config import set_alembic_database_url

        # Locate alembic.ini relative to this package
        migrations_dir = pathlib.Path(__file__).resolve().parent.parent.parent / "migrations"
        alembic_ini = migrations_dir / "alembic.ini"

        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(migrations_dir))
        set_alembic_database_url(cfg, self._connection_string)
        collector = PreviewEvidenceIssueCollector()
        module = None
        if self._focus_preview_enabled:
            if isinstance(self._storage_module, PreviewEvidenceStorageModule):
                module = self._storage_module
            else:
                collector.record(
                    PreviewEvidenceIssue(
                        revision="033",
                        kind=PreviewEvidenceIssueKind.CAPABILITY_MISSING,
                        error_type="PreviewEvidenceStorageModule",
                    )
                )
        cfg.attributes[CFG_PREVIEW_EVIDENCE_ENABLED] = module is not None
        cfg.attributes[CFG_PREVIEW_EVIDENCE_MODULE] = module
        cfg.attributes[CFG_PREVIEW_EVIDENCE_ISSUES] = collector

        # Preserve root logger state — alembic's fileConfig() overwrites it
        root = logging.root
        saved_level = root.level
        saved_handlers = root.handlers[:]
        try:
            logger.info(
                "migration_started storage_module=%s%s",
                type(self._storage_module).__name__,
                safe_log_context(stage="storage_migration", operation="alembic_upgrade", outcome="started"),
            )
            try:
                command.upgrade(cfg, "head")
            finally:
                root.setLevel(saved_level)
                root.handlers[:] = saved_handlers
        except Exception as exc:
            logger.error(
                "migration_failed storage_module=%s%s",
                type(self._storage_module).__name__,
                safe_log_context(
                    stage="storage_migration",
                    operation="alembic_upgrade",
                    outcome="failed",
                    retryable=False,
                    **safe_exception_context(exc),
                ),
            )
            raise
        logger.info(
            "migration_completed storage_module=%s%s",
            type(self._storage_module).__name__,
            safe_log_context(stage="storage_migration", operation="alembic_upgrade", outcome="completed"),
        )
        return collector.snapshot()

    def _prepare_preview_evidence(
        self,
        migration_issues: tuple[PreviewEvidenceIssue, ...],
    ) -> None:
        from core.plugin.protocols import PreviewEvidenceStorageModule
        from core.preview.storage_availability import (
            PreviewEvidenceAvailability,
            PreviewEvidenceAvailabilityState,
            PreviewEvidenceIssue,
            PreviewEvidenceIssueKind,
            PreviewEvidenceSchemaError,
        )

        if not self._focus_preview_enabled:
            self._preview_evidence_availability = PreviewEvidenceAvailability(
                PreviewEvidenceAvailabilityState.UNAVAILABLE
            )
            return
        if not isinstance(self._storage_module, PreviewEvidenceStorageModule):
            issue = PreviewEvidenceIssue(
                revision="033",
                kind=PreviewEvidenceIssueKind.CAPABILITY_MISSING,
                error_type="PreviewEvidenceStorageModule",
            )
            self._preview_evidence_availability = PreviewEvidenceAvailability(
                PreviewEvidenceAvailabilityState.UNAVAILABLE,
                (*migration_issues, issue),
            )
            return
        issues = list(migration_issues)
        try:
            if not self._use_migrations:
                self._storage_module.register_preview_evidence_tables(self._engine)
            with self._engine.begin() as connection:
                self._storage_module.prepare_preview_evidence_migration(
                    connection,
                    target_revision="033",
                )
        except (PreviewEvidenceSchemaError, SQLAlchemyError) as exc:
            issue_kind = (
                PreviewEvidenceIssueKind.SCHEMA_INCOMPATIBLE
                if isinstance(exc, PreviewEvidenceSchemaError)
                else PreviewEvidenceIssueKind.DDL_FAILED
            )
            issues.append(
                PreviewEvidenceIssue(
                    revision="033",
                    kind=issue_kind,
                    error_type=type(exc).__name__,
                )
            )
            logger.warning(
                "preview_evidence_prepare_unavailable revision=033 issue_kind=%s%s",
                issue_kind.value,
                safe_log_context(
                    stage="preview_evidence_prepare",
                    outcome="unavailable",
                    retryable=False,
                    **safe_exception_context(exc),
                ),
            )
        self._preview_evidence_availability = PreviewEvidenceAvailability(
            PreviewEvidenceAvailabilityState.READY if not issues else PreviewEvidenceAvailabilityState.UNAVAILABLE,
            tuple(dict.fromkeys(issues)),
        )

    def dispose(self) -> None:
        self._engine.dispose()
        self._ro_engine.dispose()

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

from sqlmodel import SQLModel

from core.storage.backends.sqlmodel.module import CoreStorageModule
from plugins.self_managed_kafka.storage import tables as _tables  # noqa: F401

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlmodel import Session

    from core.storage.interface import UnitOfWork

logger = logging.getLogger(__name__)


class SelfManagedKafkaStorageModule(CoreStorageModule):
    """Storage module for self-managed Kafka plugin state."""

    def attach_unit_of_work_repositories(self, uow: UnitOfWork, session: Session) -> None:
        from plugins.self_managed_kafka.storage.repositories import SelfManagedKafkaScopeStateRepository

        cast("Any", uow).self_managed_kafka_scope_state = SelfManagedKafkaScopeStateRepository(session)

    def register_tables(self, engine: Engine) -> None:
        from plugins.self_managed_kafka.storage.tables import SelfManagedKafkaScopeStateTable

        scope_state_table: Any = SelfManagedKafkaScopeStateTable
        SQLModel.metadata.create_all(engine, tables=[scope_state_table.__table__])

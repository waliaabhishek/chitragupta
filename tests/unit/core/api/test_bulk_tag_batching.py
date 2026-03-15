from __future__ import annotations

import math
from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from sqlmodel import Session, SQLModel, create_engine

if TYPE_CHECKING:
    from sqlalchemy import Engine

# _BULK_CHUNK_SIZE does not exist yet — importing it will raise ImportError (red state)
from core.api.routes.tags import _BULK_CHUNK_SIZE, _run_bulk_tag
from core.config.models import TenantConfig
from core.models.chargeback import ChargebackDimensionInfo, ChargebackRow, CostType
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository, SQLModelTagRepository

# ---------------------------------------------------------------------------
# DB fixtures for integration tests
# ---------------------------------------------------------------------------


@pytest.fixture
def engine() -> Generator[Engine]:
    eng = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(eng)
    yield eng
    eng.dispose(close=True)


@pytest.fixture
def session(engine: Engine) -> Generator[Session]:
    with Session(engine) as s:
        yield s


def _make_tenant_config(ecosystem: str = "eco", tenant_id: str = "t1") -> TenantConfig:
    return TenantConfig(ecosystem=ecosystem, tenant_id=tenant_id)


def _make_dim(
    session: Session,
    *,
    identity_id: str,
    product_type: str = "kafka",
) -> int:
    """Insert one dimension+fact, return dimension_id."""
    row = ChargebackRow(
        ecosystem="eco",
        tenant_id="t1",
        timestamp=datetime(2026, 2, 15, tzinfo=UTC),
        resource_id="r1",
        product_category="compute",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=Decimal("1.00"),
    )
    repo = SQLModelChargebackRepository(session)
    result = repo.upsert(row)
    session.commit()
    assert result.dimension_id is not None
    return result.dimension_id


class _FakeUoW:
    """Minimal UoW wrapper for testing _run_bulk_tag with real repositories."""

    def __init__(self, session: Session) -> None:
        self.chargebacks = SQLModelChargebackRepository(session)
        self.tags = SQLModelTagRepository(session)
        self._session = session

    def commit(self) -> None:
        self._session.commit()


# ---------------------------------------------------------------------------
# Issue #2: Mock-based — verify batch call counts (not N+1)
# ---------------------------------------------------------------------------


class TestRunBulkTagQueryCount:
    def test_run_bulk_tag_query_count(self) -> None:
        """With 1000 dimension IDs, get_dimensions_batch and
        find_tags_by_dimensions_and_key must each be called ceil(1000/CHUNK)=2
        times, not 1000 times (N+1).
        """
        n = 1000
        expected_chunks = math.ceil(n / _BULK_CHUNK_SIZE)
        dimension_ids = list(range(1, n + 1))

        # Build mock dim info for every ID
        def _make_dim_info(dim_id: int) -> ChargebackDimensionInfo:
            return ChargebackDimensionInfo(
                dimension_id=dim_id,
                ecosystem="eco",
                tenant_id="t1",
                resource_id="r1",
                product_category="compute",
                product_type="kafka",
                identity_id=f"user-{dim_id}",
                cost_type="usage",
                allocation_method=None,
                allocation_detail=None,
            )

        # Mock UoW with batch methods
        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)

        # get_dimensions_batch returns a full mapping for any batch
        def _get_dims_batch(ids: list[int]) -> dict[int, ChargebackDimensionInfo]:
            return {i: _make_dim_info(i) for i in ids}

        mock_uow.chargebacks.get_dimensions_batch.side_effect = _get_dims_batch

        # find_tags_by_dimensions_and_key returns empty dict (no existing tags)
        mock_uow.tags.find_tags_by_dimensions_and_key.return_value = {}
        mock_uow.tags.add_tag.return_value = MagicMock()
        mock_uow.commit.return_value = None

        tenant_config = _make_tenant_config()

        _run_bulk_tag(
            uow=mock_uow,
            tenant_config=tenant_config,
            dimension_ids=dimension_ids,
            tag_key="env",
            display_name="Production",
            created_by="admin",
            override_existing=False,
        )

        # Each batch method should be called exactly expected_chunks times
        assert mock_uow.chargebacks.get_dimensions_batch.call_count == expected_chunks
        assert mock_uow.tags.find_tags_by_dimensions_and_key.call_count == expected_chunks

        # add_tag should be called once per dimension (no existing tags)
        assert mock_uow.tags.add_tag.call_count == n


# ---------------------------------------------------------------------------
# Integration tests: _run_bulk_tag correctness
# ---------------------------------------------------------------------------


class TestRunBulkTagCorrectness:
    def test_run_bulk_tag_correctness(self, session: Session) -> None:
        """10 dims, 3 have existing tags → created=7, updated=3, skipped=0, errors=[]."""
        uow = _FakeUoW(session)
        tag_repo = SQLModelTagRepository(session)

        dim_ids: list[int] = []
        for i in range(10):
            dim_id = _make_dim(session, identity_id=f"user-{i}")
            dim_ids.append(dim_id)

        # Add existing tags to the first 3 dimensions
        for i in range(3):
            tag_repo.add_tag(dim_ids[i], "env", f"Old-{i}", "admin")
        session.commit()

        tenant_config = _make_tenant_config()

        result = _run_bulk_tag(
            uow=uow,
            tenant_config=tenant_config,
            dimension_ids=dim_ids,
            tag_key="env",
            display_name="Production",
            created_by="admin",
            override_existing=True,
        )

        assert result.created_count == 7
        assert result.updated_count == 3
        assert result.skipped_count == 0
        assert result.errors == []

    def test_run_bulk_tag_invalid_dimensions(self, session: Session) -> None:
        """Invalid dimension IDs are added to errors; valid ones are processed."""
        uow = _FakeUoW(session)

        # Create 5 valid dimensions
        dim_ids: list[int] = []
        for i in range(5):
            dim_id = _make_dim(session, identity_id=f"user-{i}")
            dim_ids.append(dim_id)

        # Add 2 invalid dimension IDs
        invalid_ids = [99998, 99999]
        all_ids = dim_ids + invalid_ids

        tenant_config = _make_tenant_config()

        result = _run_bulk_tag(
            uow=uow,
            tenant_config=tenant_config,
            dimension_ids=all_ids,
            tag_key="env",
            display_name="Production",
            created_by="admin",
            override_existing=False,
        )

        assert result.created_count == 5
        assert result.skipped_count == 0
        assert set(result.errors) == {str(99998), str(99999)}
        assert len(result.errors) == 2

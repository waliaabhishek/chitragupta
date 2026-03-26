from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import Engine, inspect
from sqlmodel import Session, SQLModel, create_engine

from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable
from core.storage.backends.sqlmodel.tables import (
    ChargebackDimensionTable,
    ChargebackFactTable,
    EntityTagTable,
    PipelineStateTable,
)


@pytest.fixture
def engine() -> Generator[Engine]:
    eng = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(eng)
    yield eng
    eng.dispose(close=True)


class TestTableCreation:
    def test_all_tables_created(self, engine: Engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        assert "resources" in table_names
        assert "identities" in table_names
        assert "billing" in table_names
        assert "chargeback_dimensions" in table_names
        assert "chargeback_facts" in table_names
        assert "pipeline_state" in table_names
        assert "tags" in table_names

    def test_resource_table_columns(self, engine: Engine):
        inspector = inspect(engine)
        cols = {c["name"] for c in inspector.get_columns("resources")}
        expected = {
            "ecosystem",
            "tenant_id",
            "resource_id",
            "resource_type",
            "display_name",
            "parent_id",
            "owner_id",
            "status",
            "cloud",
            "region",
            "created_at",
            "deleted_at",
            "last_seen_at",
            "metadata_json",
        }
        assert expected <= cols

    def test_resource_composite_pk(self, engine: Engine):
        inspector = inspect(engine)
        pk = inspector.get_pk_constraint("resources")
        assert set(pk["constrained_columns"]) == {"ecosystem", "tenant_id", "resource_id"}

    def test_identity_table_columns(self, engine: Engine):
        inspector = inspect(engine)
        cols = {c["name"] for c in inspector.get_columns("identities")}
        expected = {
            "ecosystem",
            "tenant_id",
            "identity_id",
            "identity_type",
            "display_name",
            "created_at",
            "deleted_at",
            "last_seen_at",
            "metadata_json",
        }
        assert expected <= cols

    def test_billing_table_composite_pk(self, engine: Engine):
        inspector = inspect(engine)
        pk = inspector.get_pk_constraint("billing")
        assert set(pk["constrained_columns"]) == {
            "ecosystem",
            "tenant_id",
            "timestamp",
            "resource_id",
            "product_type",
            "product_category",
        }

    def test_chargeback_dimension_unique_constraint(self, engine: Engine):
        inspector = inspect(engine)
        uqs = inspector.get_unique_constraints("chargeback_dimensions")
        uq_names = [u["name"] for u in uqs]
        assert "uq_chargeback_dimensions" in uq_names

    def test_chargeback_fact_foreign_key(self, engine: Engine):
        inspector = inspect(engine)
        fks = inspector.get_foreign_keys("chargeback_facts")
        assert any(fk["referred_table"] == "chargeback_dimensions" for fk in fks)

    def test_pipeline_state_composite_pk(self, engine: Engine):
        inspector = inspect(engine)
        pk = inspector.get_pk_constraint("pipeline_state")
        assert set(pk["constrained_columns"]) == {"ecosystem", "tenant_id", "tracking_date"}

    def test_tags_table_unique_constraint(self, engine: Engine):
        inspector = inspect(engine)
        uqs = inspector.get_unique_constraints("tags")
        uq_names = [u["name"] for u in uqs]
        assert "uq_tags_entity_key" in uq_names


class TestTableInsert:
    def test_insert_resource(self, engine: Engine):
        with Session(engine) as session:
            r = ResourceTable(
                ecosystem="ccloud",
                tenant_id="t1",
                resource_id="r1",
                resource_type="kafka",
                status="active",
            )
            session.add(r)
            session.commit()
            result = session.get(ResourceTable, ("ccloud", "t1", "r1"))
            assert result is not None
            assert result.resource_type == "kafka"

    def test_insert_identity(self, engine: Engine):
        with Session(engine) as session:
            i = IdentityTable(
                ecosystem="ccloud",
                tenant_id="t1",
                identity_id="u1",
                identity_type="user",
            )
            session.add(i)
            session.commit()
            result = session.get(IdentityTable, ("ccloud", "t1", "u1"))
            assert result is not None

    def test_insert_billing(self, engine: Engine):
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        with Session(engine) as session:
            b = BillingTable(
                ecosystem="ccloud",
                tenant_id="t1",
                timestamp=ts,
                resource_id="r1",
                product_type="kafka",
                product_category="compute",
                quantity="100",
                unit_price="0.01",
                total_cost="1.00",
            )
            session.add(b)
            session.commit()
            result = session.get(BillingTable, ("ccloud", "t1", ts, "r1", "kafka", "compute"))
            assert result is not None
            assert result.total_cost == "1.00"

    def test_insert_pipeline_state(self, engine: Engine):
        with Session(engine) as session:
            ps = PipelineStateTable(
                ecosystem="ccloud",
                tenant_id="t1",
                tracking_date=date(2026, 1, 1),
            )
            session.add(ps)
            session.commit()
            result = session.get(PipelineStateTable, ("ccloud", "t1", date(2026, 1, 1)))
            assert result is not None
            assert result.billing_gathered is False

    def test_insert_dimension_and_fact(self, engine: Engine):
        with Session(engine) as session:
            dim = ChargebackDimensionTable(
                ecosystem="ccloud",
                tenant_id="t1",
                resource_id="r1",
                product_category="compute",
                product_type="kafka",
                identity_id="u1",
                cost_type="usage",
            )
            session.add(dim)
            session.flush()
            fact = ChargebackFactTable(
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                dimension_id=dim.dimension_id,  # type: ignore[arg-type]
                amount="100.50",
                tags_json='["tag1"]',
            )
            session.add(fact)
            session.commit()
            assert dim.dimension_id is not None
            result = session.get(ChargebackFactTable, (datetime(2026, 1, 1, tzinfo=UTC), dim.dimension_id))
            assert result is not None
            assert result.amount == "100.50"

    def test_insert_entity_tag(self, engine: Engine):
        with Session(engine) as session:
            tag = EntityTagTable(
                tenant_id="t1",
                entity_type="resource",
                entity_id="r1",
                tag_key="team",
                tag_value="platform",
                created_by="admin",
            )
            session.add(tag)
            session.commit()
            assert tag.tag_id is not None

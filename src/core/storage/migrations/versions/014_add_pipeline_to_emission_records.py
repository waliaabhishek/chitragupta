"""Add pipeline discriminator column to emission_records.

Revision ID: 014
Revises: 013
Create Date: 2026-03-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    dialect = conn.dialect.name

    if dialect == "sqlite":
        # SQLite cannot drop named constraints via ALTER TABLE — use table-copy strategy.
        conn.execute(
            sa.text("""
            CREATE TABLE emission_records_new (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ecosystem     TEXT    NOT NULL,
                tenant_id     TEXT    NOT NULL,
                emitter_name  TEXT    NOT NULL,
                pipeline      TEXT    NOT NULL DEFAULT 'chargeback',
                date          DATE    NOT NULL,
                status        TEXT    NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 1,
                CONSTRAINT uq_emission_records
                    UNIQUE (ecosystem, tenant_id, emitter_name, pipeline, date)
            )
            """)
        )
        conn.execute(
            sa.text("""
            INSERT INTO emission_records_new
                (id, ecosystem, tenant_id, emitter_name, pipeline, date, status, attempt_count)
            SELECT id, ecosystem, tenant_id, emitter_name, 'chargeback', date, status, attempt_count
            FROM emission_records
            """)
        )
        conn.execute(sa.text("DROP TABLE emission_records"))
        conn.execute(sa.text("ALTER TABLE emission_records_new RENAME TO emission_records"))
        op.create_index("ix_emission_records_ecosystem", "emission_records", ["ecosystem"])
        op.create_index("ix_emission_records_tenant_id", "emission_records", ["tenant_id"])
        op.create_index("ix_emission_records_emitter_name", "emission_records", ["emitter_name"])
        op.create_index("ix_emission_records_pipeline", "emission_records", ["pipeline"])
        op.create_index("ix_emission_records_date", "emission_records", ["date"])
    else:
        # PostgreSQL: add column with default, drop old constraint, create new one.
        op.add_column(
            "emission_records",
            sa.Column("pipeline", sa.String(), nullable=False, server_default="chargeback"),
        )
        op.drop_constraint("uq_emission_records", "emission_records", type_="unique")
        op.create_unique_constraint(
            "uq_emission_records",
            "emission_records",
            ["ecosystem", "tenant_id", "emitter_name", "pipeline", "date"],
        )
        op.create_index("ix_emission_records_pipeline", "emission_records", ["pipeline"])


def downgrade() -> None:
    conn = op.get_bind()
    dialect = conn.dialect.name

    if dialect == "sqlite":
        conn.execute(
            sa.text("""
            CREATE TABLE emission_records_old (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ecosystem     TEXT    NOT NULL,
                tenant_id     TEXT    NOT NULL,
                emitter_name  TEXT    NOT NULL,
                date          DATE    NOT NULL,
                status        TEXT    NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 1,
                CONSTRAINT uq_emission_records
                    UNIQUE (ecosystem, tenant_id, emitter_name, date)
            )
            """)
        )
        conn.execute(
            sa.text("""
            INSERT INTO emission_records_old
                (id, ecosystem, tenant_id, emitter_name, date, status, attempt_count)
            SELECT id, ecosystem, tenant_id, emitter_name, date, status, attempt_count
            FROM emission_records
            """)
        )
        conn.execute(sa.text("DROP TABLE emission_records"))
        conn.execute(sa.text("ALTER TABLE emission_records_old RENAME TO emission_records"))
        op.create_index("ix_emission_records_ecosystem", "emission_records", ["ecosystem"])
        op.create_index("ix_emission_records_tenant_id", "emission_records", ["tenant_id"])
        op.create_index("ix_emission_records_emitter_name", "emission_records", ["emitter_name"])
        op.create_index("ix_emission_records_date", "emission_records", ["date"])
    else:
        op.drop_index("ix_emission_records_pipeline", "emission_records")
        op.drop_constraint("uq_emission_records", "emission_records", type_="unique")
        op.create_unique_constraint(
            "uq_emission_records",
            "emission_records",
            ["ecosystem", "tenant_id", "emitter_name", "date"],
        )
        op.drop_column("emission_records", "pipeline")

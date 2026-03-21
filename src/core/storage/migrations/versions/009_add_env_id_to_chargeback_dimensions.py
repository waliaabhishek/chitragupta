"""Add env_id to chargeback_dimensions and backfill from ccloud_billing

Revision ID: 009
Revises: 008
Create Date: 2026-03-21

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add env_id column with default empty string (non-nullable)
    op.add_column(
        "chargeback_dimensions",
        sa.Column("env_id", sa.String(), nullable=False, server_default=""),
    )

    # 2. Backfill env_id for CCloud rows from ccloud_billing.
    #    Uses MAX(env_id) to handle the rare case of multiple matches;
    #    a given resource_id always maps to exactly one env_id in CCloud.
    conn = op.get_bind()
    conn.execute(
        sa.text("""
        UPDATE chargeback_dimensions
        SET env_id = (
            SELECT COALESCE(MAX(cb.env_id), '')
            FROM ccloud_billing cb
            WHERE cb.ecosystem   = chargeback_dimensions.ecosystem
              AND cb.tenant_id   = chargeback_dimensions.tenant_id
              AND cb.resource_id = chargeback_dimensions.resource_id
        )
        WHERE ecosystem = 'confluent_cloud'
    """)
    )

    # 3. Rebuild unique constraint to include env_id.
    #    SQLite does not support ALTER CONSTRAINT, so we recreate via table copy.
    conn.execute(
        sa.text("""
        CREATE TABLE chargeback_dimensions_new (
            dimension_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            ecosystem         TEXT    NOT NULL,
            tenant_id         TEXT    NOT NULL,
            resource_id       TEXT,
            product_category  TEXT    NOT NULL DEFAULT '',
            product_type      TEXT    NOT NULL DEFAULT '',
            identity_id       TEXT    NOT NULL DEFAULT '',
            cost_type         TEXT    NOT NULL DEFAULT '',
            allocation_method TEXT,
            allocation_detail TEXT,
            env_id            TEXT    NOT NULL DEFAULT ''
        )
    """)
    )
    conn.execute(
        sa.text("""
        INSERT INTO chargeback_dimensions_new
            (dimension_id, ecosystem, tenant_id, resource_id, product_category,
             product_type, identity_id, cost_type, allocation_method, allocation_detail, env_id)
        SELECT  dimension_id, ecosystem, tenant_id, resource_id, product_category,
                product_type, identity_id, cost_type, allocation_method, allocation_detail, env_id
        FROM chargeback_dimensions
    """)
    )
    conn.execute(sa.text("DROP TABLE chargeback_dimensions"))
    conn.execute(sa.text("ALTER TABLE chargeback_dimensions_new RENAME TO chargeback_dimensions"))

    # 4. Recreate indexes dropped by the table swap (use CREATE UNIQUE INDEX for named unique constraint)
    conn.execute(
        sa.text("""
        CREATE UNIQUE INDEX uq_chargeback_dimensions ON chargeback_dimensions
            (ecosystem, tenant_id, resource_id, product_category,
             product_type, identity_id, cost_type,
             allocation_method, allocation_detail, env_id)
    """)
    )
    op.create_index("ix_chargeback_dimensions_eco_tenant", "chargeback_dimensions", ["ecosystem", "tenant_id"])
    op.create_index("ix_chargeback_dimensions_ecosystem", "chargeback_dimensions", ["ecosystem"])
    op.create_index("ix_chargeback_dimensions_tenant_id", "chargeback_dimensions", ["tenant_id"])
    op.create_index("ix_chargeback_dimensions_resource_id", "chargeback_dimensions", ["resource_id"])
    op.create_index("ix_chargeback_dimensions_product_type", "chargeback_dimensions", ["product_type"])
    op.create_index("ix_chargeback_dimensions_identity_id", "chargeback_dimensions", ["identity_id"])


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text("""
        CREATE TABLE chargeback_dimensions_old (
            dimension_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            ecosystem         TEXT    NOT NULL,
            tenant_id         TEXT    NOT NULL,
            resource_id       TEXT,
            product_category  TEXT    NOT NULL DEFAULT '',
            product_type      TEXT    NOT NULL DEFAULT '',
            identity_id       TEXT    NOT NULL DEFAULT '',
            cost_type         TEXT    NOT NULL DEFAULT '',
            allocation_method TEXT,
            allocation_detail TEXT
        )
    """)
    )
    conn.execute(
        sa.text("""
        INSERT INTO chargeback_dimensions_old
            (dimension_id, ecosystem, tenant_id, resource_id, product_category,
             product_type, identity_id, cost_type, allocation_method, allocation_detail)
        SELECT  dimension_id, ecosystem, tenant_id, resource_id, product_category,
                product_type, identity_id, cost_type, allocation_method, allocation_detail
        FROM chargeback_dimensions
    """)
    )
    conn.execute(sa.text("DROP TABLE chargeback_dimensions"))
    conn.execute(sa.text("ALTER TABLE chargeback_dimensions_old RENAME TO chargeback_dimensions"))

    # Recreate the 9-field unique index (without env_id)
    conn.execute(
        sa.text("""
        CREATE UNIQUE INDEX uq_chargeback_dimensions ON chargeback_dimensions
            (ecosystem, tenant_id, resource_id, product_category,
             product_type, identity_id, cost_type,
             allocation_method, allocation_detail)
    """)
    )
    op.create_index("ix_chargeback_dimensions_eco_tenant", "chargeback_dimensions", ["ecosystem", "tenant_id"])
    op.create_index("ix_chargeback_dimensions_ecosystem", "chargeback_dimensions", ["ecosystem"])
    op.create_index("ix_chargeback_dimensions_tenant_id", "chargeback_dimensions", ["tenant_id"])
    op.create_index("ix_chargeback_dimensions_resource_id", "chargeback_dimensions", ["resource_id"])
    op.create_index("ix_chargeback_dimensions_product_type", "chargeback_dimensions", ["product_type"])
    op.create_index("ix_chargeback_dimensions_identity_id", "chargeback_dimensions", ["identity_id"])

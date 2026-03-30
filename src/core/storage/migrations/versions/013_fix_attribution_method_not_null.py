"""Fix attribution_method nullable constraint in topic_attribution_dimensions.

Revision ID: 013
Revises: 012
Create Date: 2026-03-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Step 1: backfill any existing NULLs to empty string
    conn.execute(
        sa.text("UPDATE topic_attribution_dimensions SET attribution_method = '' WHERE attribution_method IS NULL")
    )

    dialect = conn.dialect.name

    if dialect == "sqlite":
        # SQLite does not support ALTER COLUMN — use table-copy strategy
        conn.execute(
            sa.text("""
            CREATE TABLE topic_attribution_dimensions_new (
                dimension_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ecosystem             TEXT    NOT NULL,
                tenant_id             TEXT    NOT NULL,
                env_id                TEXT    NOT NULL DEFAULT '',
                cluster_resource_id   TEXT    NOT NULL,
                topic_name            TEXT    NOT NULL DEFAULT '',
                product_category      TEXT    NOT NULL DEFAULT '',
                product_type          TEXT    NOT NULL DEFAULT '',
                attribution_method    TEXT    NOT NULL DEFAULT ''
            )
            """)
        )
        conn.execute(
            sa.text("""
            INSERT INTO topic_attribution_dimensions_new
                (dimension_id, ecosystem, tenant_id, env_id, cluster_resource_id,
                 topic_name, product_category, product_type, attribution_method)
            SELECT dimension_id, ecosystem, tenant_id, env_id, cluster_resource_id,
                   topic_name, product_category, product_type, attribution_method
            FROM topic_attribution_dimensions
            """)
        )
        conn.execute(sa.text("DROP TABLE topic_attribution_dimensions"))
        conn.execute(sa.text("ALTER TABLE topic_attribution_dimensions_new RENAME TO topic_attribution_dimensions"))
        # Recreate unique constraint and indexes dropped by the table swap
        conn.execute(
            sa.text("""
            CREATE UNIQUE INDEX uq_topic_attribution_dimensions
            ON topic_attribution_dimensions
                (ecosystem, tenant_id, env_id, cluster_resource_id,
                 topic_name, product_category, product_type, attribution_method)
            """)
        )
        op.create_index(
            "ix_topic_attr_dim_eco_tenant",
            "topic_attribution_dimensions",
            ["ecosystem", "tenant_id"],
        )
        op.create_index(
            "ix_topic_attr_dim_cluster",
            "topic_attribution_dimensions",
            ["ecosystem", "tenant_id", "cluster_resource_id"],
        )
        op.create_index(
            "ix_topic_attribution_dimensions_ecosystem",
            "topic_attribution_dimensions",
            ["ecosystem"],
        )
        op.create_index(
            "ix_topic_attribution_dimensions_tenant_id",
            "topic_attribution_dimensions",
            ["tenant_id"],
        )
        op.create_index(
            "ix_topic_attribution_dimensions_cluster_resource_id",
            "topic_attribution_dimensions",
            ["cluster_resource_id"],
        )
        op.create_index(
            "ix_topic_attribution_dimensions_product_type",
            "topic_attribution_dimensions",
            ["product_type"],
        )
    else:
        # PostgreSQL: standard ALTER COLUMN
        op.alter_column(
            "topic_attribution_dimensions",
            "attribution_method",
            existing_type=sa.String(),
            nullable=False,
            server_default="",
        )


def downgrade() -> None:
    conn = op.get_bind()
    dialect = conn.dialect.name

    if dialect == "sqlite":
        conn.execute(
            sa.text("""
            CREATE TABLE topic_attribution_dimensions_old (
                dimension_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ecosystem             TEXT    NOT NULL,
                tenant_id             TEXT    NOT NULL,
                env_id                TEXT    NOT NULL DEFAULT '',
                cluster_resource_id   TEXT    NOT NULL,
                topic_name            TEXT    NOT NULL DEFAULT '',
                product_category      TEXT    NOT NULL DEFAULT '',
                product_type          TEXT    NOT NULL DEFAULT '',
                attribution_method    TEXT
            )
            """)
        )
        conn.execute(
            sa.text("""
            INSERT INTO topic_attribution_dimensions_old
                (dimension_id, ecosystem, tenant_id, env_id, cluster_resource_id,
                 topic_name, product_category, product_type, attribution_method)
            SELECT dimension_id, ecosystem, tenant_id, env_id, cluster_resource_id,
                   topic_name, product_category, product_type, attribution_method
            FROM topic_attribution_dimensions
            """)
        )
        conn.execute(sa.text("DROP TABLE topic_attribution_dimensions"))
        conn.execute(sa.text("ALTER TABLE topic_attribution_dimensions_old RENAME TO topic_attribution_dimensions"))
        conn.execute(
            sa.text("""
            CREATE UNIQUE INDEX uq_topic_attribution_dimensions
            ON topic_attribution_dimensions
                (ecosystem, tenant_id, env_id, cluster_resource_id,
                 topic_name, product_category, product_type, attribution_method)
            """)
        )
        op.create_index(
            "ix_topic_attr_dim_eco_tenant",
            "topic_attribution_dimensions",
            ["ecosystem", "tenant_id"],
        )
        op.create_index(
            "ix_topic_attr_dim_cluster",
            "topic_attribution_dimensions",
            ["ecosystem", "tenant_id", "cluster_resource_id"],
        )
        op.create_index(
            "ix_topic_attribution_dimensions_ecosystem",
            "topic_attribution_dimensions",
            ["ecosystem"],
        )
        op.create_index(
            "ix_topic_attribution_dimensions_tenant_id",
            "topic_attribution_dimensions",
            ["tenant_id"],
        )
        op.create_index(
            "ix_topic_attribution_dimensions_cluster_resource_id",
            "topic_attribution_dimensions",
            ["cluster_resource_id"],
        )
        op.create_index(
            "ix_topic_attribution_dimensions_product_type",
            "topic_attribution_dimensions",
            ["product_type"],
        )
    else:
        op.alter_column(
            "topic_attribution_dimensions",
            "attribution_method",
            existing_type=sa.String(),
            nullable=True,
            server_default=None,
        )

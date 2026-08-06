"""billing table: add product_category to primary key

Revision ID: 005
Revises: 004
Create Date: 2026-03-06

"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None

_BILLING_PK_004 = ("ecosystem", "tenant_id", "timestamp", "resource_id", "product_type")
_BILLING_PK_005 = (*_BILLING_PK_004, "product_category")


def _postgresql_pk_name_for_expected_columns(
    conn: Connection,
    table_name: str,
    expected_columns: tuple[str, ...],
) -> str:
    pk = sa.inspect(conn).get_pk_constraint(table_name)
    name = pk.get("name")
    columns = tuple(pk.get("constrained_columns") or ())
    if columns != expected_columns or not name:
        raise RuntimeError(
            f"migration 005 expected {table_name} primary key columns {expected_columns!r}; "
            "database has a different primary key shape. Inspect schema state before retrying."
        )
    return name


def _postgresql_has_billing_product_category_collapse(conn: Connection) -> bool:
    return (
        conn.execute(
            sa.text("""
                SELECT 1
                FROM billing
                GROUP BY ecosystem, tenant_id, timestamp, resource_id, product_type
                HAVING COUNT(*) > 1
                LIMIT 1
            """)
        ).first()
        is not None
    )


def upgrade() -> None:
    conn = op.get_bind()
    existing_pk_name = None
    if conn.dialect.name == "postgresql":
        existing_pk_name = _postgresql_pk_name_for_expected_columns(conn, "billing", _BILLING_PK_004)

    with op.batch_alter_table("billing") as batch_op:
        if existing_pk_name is not None:
            batch_op.drop_constraint(existing_pk_name, type_="primary")
        batch_op.drop_index("ix_billing_product_category")
        batch_op.create_primary_key("pk_billing", list(_BILLING_PK_005))


def downgrade() -> None:
    # WARNING: Downgrade will fail if billing table contains rows that differ
    # only by product_category (i.e., same ecosystem/tenant/timestamp/resource/product_type
    # but different product_category). Such rows cannot coexist under the 5-field PK.
    conn = op.get_bind()
    existing_pk_name = None
    if conn.dialect.name == "postgresql":
        if _postgresql_has_billing_product_category_collapse(conn):
            raise RuntimeError(
                "migration 005 downgrade cannot restore the revision-004 billing primary key "
                "because billing rows differ only by product_category; downgrade aborted before "
                "constraint changes"
            )
        existing_pk_name = _postgresql_pk_name_for_expected_columns(conn, "billing", _BILLING_PK_005)

    with op.batch_alter_table("billing") as batch_op:
        if existing_pk_name is not None:
            batch_op.drop_constraint(existing_pk_name, type_="primary")
        batch_op.create_primary_key("pk_billing", list(_BILLING_PK_004))
        batch_op.create_index("ix_billing_product_category", ["product_category"])

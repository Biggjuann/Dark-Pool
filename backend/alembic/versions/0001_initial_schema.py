"""Initial schema — all four core tables.

Revision ID: 0001
Revises:
Create Date: 2026-03-21
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # dark_pool_prints
    # ------------------------------------------------------------------
    op.create_table(
        "dark_pool_prints",
        sa.Column("id",                 sa.Integer(),    nullable=False),
        sa.Column("ticker",             sa.String(16),   nullable=False),
        sa.Column("week_ending",        sa.Date(),       nullable=False),
        sa.Column("dp_volume",          sa.BigInteger(), nullable=False),
        sa.Column("dp_trade_count",     sa.Integer(),    nullable=False),
        sa.Column("total_volume",       sa.BigInteger(), nullable=False),
        sa.Column("dp_pct",             sa.Float(),      nullable=False),
        sa.Column("dp_volume_4wk_avg",  sa.Float(),      nullable=True),
        sa.Column("volume_spike_ratio", sa.Float(),      nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", "week_ending", name="uq_dp_ticker_week"),
    )
    op.create_index("ix_dark_pool_prints_ticker",      "dark_pool_prints", ["ticker"])
    op.create_index("ix_dark_pool_prints_week_ending", "dark_pool_prints", ["week_ending"])

    # ------------------------------------------------------------------
    # price_snapshots
    # ------------------------------------------------------------------
    op.create_table(
        "price_snapshots",
        sa.Column("id",            sa.Integer(),    nullable=False),
        sa.Column("ticker",        sa.String(16),   nullable=False),
        sa.Column("snapshot_date", sa.Date(),       nullable=False),
        sa.Column("open",          sa.Float(),      nullable=True),
        sa.Column("high",          sa.Float(),      nullable=True),
        sa.Column("low",           sa.Float(),      nullable=True),
        sa.Column("close",         sa.Float(),      nullable=True),
        sa.Column("volume",        sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", "snapshot_date", name="uq_price_ticker_date"),
    )
    op.create_index("ix_price_snapshots_ticker",        "price_snapshots", ["ticker"])
    op.create_index("ix_price_snapshots_snapshot_date", "price_snapshots", ["snapshot_date"])

    # ------------------------------------------------------------------
    # watchlist
    # ------------------------------------------------------------------
    op.create_table(
        "watchlist",
        sa.Column("id",          sa.Integer(),   nullable=False),
        sa.Column("ticker",      sa.String(16),  nullable=False),
        sa.Column("added_date",  sa.Date(),      nullable=False),
        sa.Column("entry_price", sa.Float(),     nullable=True),
        sa.Column("notes",       sa.String(),    nullable=True),
        sa.Column(
            "status",
            sa.Enum("watching", "entered", "closed", name="watchlist_status"),
            nullable=False,
            server_default="watching",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", name="uq_watchlist_ticker"),
    )
    op.create_index("ix_watchlist_ticker", "watchlist", ["ticker"], unique=True)

    # ------------------------------------------------------------------
    # signals
    # ------------------------------------------------------------------
    op.create_table(
        "signals",
        sa.Column("id",          sa.Integer(),   nullable=False),
        sa.Column("ticker",      sa.String(16),  nullable=False),
        sa.Column("week_ending", sa.Date(),      nullable=False),
        sa.Column("signal_type", sa.String(64),  nullable=False),
        sa.Column("score",       sa.Float(),     nullable=False),
        sa.Column(
            "triggered_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
        ),
        sa.Column("alerted",     sa.Boolean(),   nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "ticker", "week_ending", "signal_type",
            name="uq_signal_ticker_week_type",
        ),
    )
    op.create_index("ix_signals_ticker",      "signals", ["ticker"])
    op.create_index("ix_signals_week_ending", "signals", ["week_ending"])


def downgrade() -> None:
    op.drop_table("signals")
    op.drop_table("watchlist")
    op.drop_table("price_snapshots")
    op.drop_table("dark_pool_prints")

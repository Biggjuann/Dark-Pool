"""
SQLAlchemy ORM models for the Dark Pool Tracker.

Tables
------
dark_pool_prints  — weekly FINRA OTC dark pool volume per ticker
price_snapshots   — daily OHLCV used to measure post-print price moves
watchlist         — user-managed watch list with trade-status tracking
signals           — scanner-generated alerts with score and alert state
twitter_accounts  — Twitter handles monitored for sentiment
tweets            — raw tweets from monitored accounts (cashtag filter)
ticker_sentiment  — daily aggregated sentiment scores per ticker
"""

import enum
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
    UniqueConstraint,
    func,
)

from database import Base


# ---------------------------------------------------------------------------
# Enum types
# ---------------------------------------------------------------------------

class WatchlistStatus(str, enum.Enum):
    watching = "watching"
    entered  = "entered"
    closed   = "closed"


# ---------------------------------------------------------------------------
# dark_pool_prints
# ---------------------------------------------------------------------------

class DarkPoolPrint(Base):
    """
    One row per (ticker, week_ending).

    Populated by the FINRA ATS weekly ingest.
    dp_pct           = dp_volume / total_volume * 100
    dp_volume_4wk_avg = rolling 4-week mean of dp_volume for this ticker
    volume_spike_ratio = dp_volume / dp_volume_4wk_avg  (>1 = above average)
    """

    __tablename__ = "dark_pool_prints"
    __table_args__ = (
        UniqueConstraint("ticker", "week_ending", name="uq_dp_ticker_week"),
    )

    id                 = Column(Integer,  primary_key=True, index=True)
    ticker             = Column(String(16), nullable=False, index=True)
    week_ending        = Column(Date,     nullable=False, index=True)   # Friday of FINRA week
    dp_volume          = Column(BigInteger, nullable=False)             # shares traded off-exchange
    dp_trade_count     = Column(Integer,  nullable=False)               # number of dark pool trades
    total_volume       = Column(BigInteger, nullable=False)             # total reported shares
    dp_pct             = Column(Float,    nullable=False)               # dp_volume / total_volume * 100
    dp_volume_4wk_avg  = Column(Float,    nullable=True)                # NULL until 4 weeks of history exist
    volume_spike_ratio = Column(Float,    nullable=True)                # dp_volume / dp_volume_4wk_avg
    created_at         = Column(DateTime, nullable=False, server_default=func.now())


# ---------------------------------------------------------------------------
# price_snapshots
# ---------------------------------------------------------------------------

class PriceSnapshot(Base):
    """
    Daily OHLCV snapshot.

    Fetched for any ticker that appears in dark_pool_prints or watchlist so
    we can measure price movement in the 5–10 days after a dark pool print.
    """

    __tablename__ = "price_snapshots"
    __table_args__ = (
        UniqueConstraint("ticker", "snapshot_date", name="uq_price_ticker_date"),
    )

    id            = Column(Integer,   primary_key=True, index=True)
    ticker        = Column(String(16), nullable=False, index=True)
    snapshot_date = Column(Date,      nullable=False, index=True)
    open          = Column(Float,     nullable=True)
    high          = Column(Float,     nullable=True)
    low           = Column(Float,     nullable=True)
    close         = Column(Float,     nullable=True)
    volume        = Column(BigInteger, nullable=True)
    created_at    = Column(DateTime,  nullable=False, server_default=func.now())


# ---------------------------------------------------------------------------
# watchlist
# ---------------------------------------------------------------------------

class WatchlistEntry(Base):
    """
    User-managed watch list.

    status transitions: watching → entered → closed
    entry_price is recorded when the user marks status = "entered".
    """

    __tablename__ = "watchlist"

    id           = Column(Integer,   primary_key=True, index=True)
    ticker       = Column(String(16), nullable=False, unique=True, index=True)
    added_date   = Column(Date,      nullable=False, default=datetime.utcnow)
    entry_price  = Column(Float,     nullable=True)   # price when position was entered
    notes        = Column(String,    nullable=True)
    status       = Column(
        Enum(WatchlistStatus, name="watchlist_status"),
        nullable=False,
        default=WatchlistStatus.watching,
    )


# ---------------------------------------------------------------------------
# daily_dark_pool_prints
# ---------------------------------------------------------------------------

class DailyDarkPoolPrint(Base):
    """
    One row per (ticker, print_date) — daily granularity.

    Populated alongside the weekly aggregates so the screener can show
    individual daily prints rather than only weekly summaries.
    """

    __tablename__ = "daily_dark_pool_prints"
    __table_args__ = (
        UniqueConstraint("ticker", "print_date", name="uq_daily_dp_ticker_date"),
    )

    id           = Column(Integer,    primary_key=True, index=True)
    ticker       = Column(String(16),  nullable=False, index=True)
    print_date   = Column(Date,        nullable=False, index=True)
    week_ending  = Column(Date,        nullable=False, index=True)
    dp_volume    = Column(BigInteger,  nullable=False)
    total_volume = Column(BigInteger,  nullable=False)
    dp_pct       = Column(Float,       nullable=False)
    created_at   = Column(DateTime,    nullable=False, server_default=func.now())


# ---------------------------------------------------------------------------
# ticker_meta
# ---------------------------------------------------------------------------

class TickerMeta(Base):
    """
    Reference data for a ticker: company name, sector, industry, market cap.

    Populated by yfinance during the ingest pipeline's meta-fetch stage.
    """

    __tablename__ = "ticker_meta"

    id         = Column(Integer,    primary_key=True, index=True)
    ticker     = Column(String(16),  nullable=False, unique=True, index=True)
    name       = Column(String(256), nullable=True)
    sector     = Column(String(64),  nullable=True)
    industry   = Column(String(128), nullable=True)
    market_cap = Column(BigInteger,  nullable=True)
    updated_at = Column(DateTime,    nullable=False, server_default=func.now())


# ---------------------------------------------------------------------------
# signals
# ---------------------------------------------------------------------------

class Signal(Base):
    """
    One row per scanner run per ticker.

    signal_type examples: "dp_spike", "accumulation_cluster", "repeat_print"
    score is 0–100; higher = stronger conviction.
    alerted flips to True once the Discord webhook fires for this signal.
    """

    __tablename__ = "signals"
    __table_args__ = (
        UniqueConstraint("ticker", "week_ending", "signal_type", name="uq_signal_ticker_week_type"),
    )

    id           = Column(Integer,   primary_key=True, index=True)
    ticker       = Column(String(16), nullable=False, index=True)
    week_ending  = Column(Date,      nullable=False, index=True)
    signal_type  = Column(String(64), nullable=False)
    score        = Column(Float,     nullable=False)   # 0–100
    triggered_at = Column(DateTime,  nullable=False, server_default=func.now())
    alerted      = Column(Boolean,   nullable=False, default=False)


# ---------------------------------------------------------------------------
# twitter_accounts
# ---------------------------------------------------------------------------

class TwitterAccount(Base):
    """
    Twitter handles monitored for sentiment signals.

    Handles are stored in lowercase without the '@' prefix.
    is_active=False is a soft-delete; the row is kept for audit purposes.
    """

    __tablename__ = "twitter_accounts"

    id        = Column(Integer,    primary_key=True, index=True)
    handle    = Column(String(64),  nullable=False, unique=True, index=True)
    is_active = Column(Boolean,    nullable=False, default=True)
    added_at  = Column(Date,       nullable=False, server_default=func.current_date())


# ---------------------------------------------------------------------------
# tweets
# ---------------------------------------------------------------------------

class Tweet(Base):
    """
    Raw tweets from monitored accounts that contain at least one cashtag.

    tickers stores the extracted $TICKER symbols as a comma-separated string
    (e.g. "AAPL,TSLA") so we avoid a separate junction table for this
    moderate-volume workload.
    """

    __tablename__ = "tweets"
    __table_args__ = (
        UniqueConstraint("tweet_id", name="uq_tweet_id"),
    )

    id            = Column(Integer,     primary_key=True, index=True)
    tweet_id      = Column(String(32),   nullable=False, index=True)
    author_handle = Column(String(64),   nullable=False, index=True)
    text          = Column(String(1024), nullable=False)
    tickers       = Column(String(256),  nullable=True)   # "AAPL,TSLA"
    created_at    = Column(DateTime,     nullable=False, index=True)
    ingested_at   = Column(DateTime,     nullable=False, server_default=func.now())


# ---------------------------------------------------------------------------
# ticker_sentiment
# ---------------------------------------------------------------------------

class TickerSentiment(Base):
    """
    Daily aggregated Twitter sentiment score for a ticker.

    sentiment_score  0–100: 50 = neutral, >50 = net bullish, <50 = net bearish.
    tweet_count      total tweets mentioning this ticker in the lookback window.
    bullish_count    tweets with net-positive keyword score.
    bearish_count    tweets with net-negative keyword score.
    """

    __tablename__ = "ticker_sentiment"
    __table_args__ = (
        UniqueConstraint("ticker", "date", name="uq_sentiment_ticker_date"),
    )

    id              = Column(Integer,    primary_key=True, index=True)
    ticker          = Column(String(16),  nullable=False, index=True)
    date            = Column(Date,        nullable=False, index=True)
    sentiment_score = Column(Float,       nullable=False)   # 0–100
    tweet_count     = Column(Integer,     nullable=False)
    bullish_count   = Column(Integer,     nullable=False, default=0)
    bearish_count   = Column(Integer,     nullable=False, default=0)
    last_updated    = Column(DateTime,    nullable=False, server_default=func.now())

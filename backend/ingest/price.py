"""
Price data ingestion via yfinance.

All public functions accept a SQLAlchemy Session and commit nothing —
the caller owns the transaction boundary.  Call ``db.commit()`` after
any of the write functions (fetch_price_snapshot, fetch_bulk_prices).

Rate-limiting
-------------
yfinance batches multiple tickers into a single Yahoo Finance API call
per ``yf.download()`` invocation.  We enforce a ``REQUEST_DELAY_S`` sleep
between *batch* calls (not between individual tickers) to stay polite.

yfinance quirks handled
-----------------------
- Single-ticker downloads return a plain DataFrame; multi-ticker returns a
  MultiIndex.  Both are normalised to ``{ticker: DataFrame}`` internally.
- DatetimeIndex may be UTC-aware or naive depending on yfinance version;
  both are normalised to a plain ``datetime.date`` before DB writes.
- ``auto_adjust=True`` removes the Adj Close column and adjusts OHLCV for
  splits/dividends — preferred for signal research.
- Empty DataFrames signal a delisted / invalid ticker; logged and skipped.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf
from sqlalchemy.orm import Session

from models import PriceSnapshot, TickerMeta

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

BATCH_SIZE: int = 50          # tickers per yf.download() call
REQUEST_DELAY_S: float = 0.5  # seconds to sleep between batch calls
FLAT_THRESHOLD_PCT: float = 3.0  # % move considered "flat" by is_price_flat()


# ---------------------------------------------------------------------------
# Internal: yfinance download
# ---------------------------------------------------------------------------

def _fetch_yf_batch(
    batch: list[str],
    start_date: date,
    end_date: date,
) -> dict[str, pd.DataFrame]:
    """
    Download OHLCV for *batch* from yfinance.

    *end_date* is **exclusive** (yfinance convention) — pass ``signal_date + 1``
    when you want data *through* a specific date.

    Returns
    -------
    dict[str, pd.DataFrame]
        ``{TICKER: df}`` where each df has a plain ``datetime.date`` index and
        lowercase columns ``open, high, low, close, volume``.
        Tickers with no data are omitted silently.
    """
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    try:
        raw: pd.DataFrame = yf.download(
            tickers      = batch,
            start        = start_str,
            end          = end_str,
            progress     = False,
            threads      = False,   # sequential — avoids thundering-herd on rate limits
            auto_adjust  = True,    # adjusts for splits/dividends; drops Adj Close
            group_by     = "ticker",
        )
    except Exception as exc:
        logger.error("yfinance download failed for batch %s: %s", batch, exc)
        return {}

    if raw is None or raw.empty:
        logger.debug("yfinance returned empty DataFrame for batch starting with %s", batch[:3])
        return {}

    # ---- Normalise DatetimeIndex to plain date ----
    idx = pd.to_datetime(raw.index)
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    raw.index = idx.normalize().date  # array of datetime.date

    # ---- Normalise to MultiIndex for uniform handling ----
    # Single-ticker downloads return a plain DataFrame (no MultiIndex),
    # even when group_by='ticker' is set in some yfinance versions.
    if len(batch) == 1 and not isinstance(raw.columns, pd.MultiIndex):
        raw = pd.concat({batch[0]: raw}, axis=1)

    result: dict[str, pd.DataFrame] = {}

    for ticker in batch:
        try:
            t_df = raw[ticker].copy()
        except KeyError:
            logger.debug("Ticker %s absent from yfinance response", ticker)
            continue

        t_df = t_df.dropna(how="all")
        if t_df.empty:
            logger.debug("No data for %s in [%s, %s)", ticker, start_str, end_str)
            continue

        # Normalise column names to lowercase so the rest of the code is
        # insulated from yfinance capitalisation changes between versions.
        t_df.columns = [str(c).lower() for c in t_df.columns]

        # Ensure the columns we care about exist (auto_adjust may drop some)
        for col in ("open", "high", "low", "close", "volume"):
            if col not in t_df.columns:
                t_df[col] = None

        result[ticker] = t_df

    return result


# ---------------------------------------------------------------------------
# Internal: convert yfinance result → list of row dicts
# ---------------------------------------------------------------------------

def _yf_data_to_rows(yf_data: dict[str, pd.DataFrame]) -> list[dict]:
    """
    Flatten ``{ticker: df}`` into a list of dicts ready for DB upsert.

    Each dict has keys: ticker, snapshot_date, open, high, low, close, volume.
    NaN values are converted to None so SQLAlchemy writes NULL.
    Float columns are cast to native Python floats so psycopg2 on Postgres
    doesn't misinterpret numpy repr strings ("np.float64(...)") as SQL.
    """
    rows: list[dict] = []
    for ticker, df in yf_data.items():
        for idx_date, row in df.iterrows():
            if isinstance(idx_date, datetime):
                snap_date = idx_date.date()
            elif hasattr(idx_date, "date") and callable(idx_date.date):
                snap_date = idx_date.date()
            else:
                snap_date = idx_date

            def _val(col: str):
                v = row.get(col)
                if v is None or pd.isna(v):
                    return None
                return float(v)

            vol = row.get("volume")
            rows.append({
                "ticker":        ticker,
                "snapshot_date": snap_date,
                "open":          _val("open"),
                "high":          _val("high"),
                "low":           _val("low"),
                "close":         _val("close"),
                "volume":        int(vol) if vol is not None and not pd.isna(vol) else None,
            })
    return rows



# ---------------------------------------------------------------------------
# Internal: upsert rows into price_snapshots
# ---------------------------------------------------------------------------

def _upsert_snapshots(db: Session, rows: list[dict]) -> int:
    """
    Upsert a flat list of snapshot dicts into ``price_snapshots``.

    Pattern: one bulk query to find existing (ticker, snapshot_date) pairs,
    then split into new inserts vs. in-place updates — avoids N+1 queries.

    Returns the total number of rows touched.
    """
    if not rows:
        return 0

    # Build a set of (ticker, snapshot_date) that are already in the DB
    # for the dates present in this batch.
    dates_in_batch  = list({r["snapshot_date"] for r in rows})
    tickers_in_batch = list({r["ticker"]       for r in rows})

    existing_map: dict[tuple, PriceSnapshot] = {
        (obj.ticker, obj.snapshot_date): obj
        for obj in db.query(PriceSnapshot)
        .filter(
            PriceSnapshot.ticker.in_(tickers_in_batch),
            PriceSnapshot.snapshot_date.in_(dates_in_batch),
        )
        .all()
    }

    new_objects: list[PriceSnapshot] = []
    update_count = 0

    for r in rows:
        key = (r["ticker"], r["snapshot_date"])
        if key in existing_map:
            obj = existing_map[key]
            obj.open   = r["open"]
            obj.high   = r["high"]
            obj.low    = r["low"]
            obj.close  = r["close"]
            obj.volume = r["volume"]
            update_count += 1
        else:
            new_objects.append(
                PriceSnapshot(
                    ticker        = r["ticker"],
                    snapshot_date = r["snapshot_date"],
                    open          = r["open"],
                    high          = r["high"],
                    low           = r["low"],
                    close         = r["close"],
                    volume        = r["volume"],
                )
            )

    if new_objects:
        db.bulk_save_objects(new_objects)
    db.flush()

    logger.debug(
        "_upsert_snapshots: %d inserted, %d updated", len(new_objects), update_count
    )
    return len(new_objects) + update_count


# ---------------------------------------------------------------------------
# Public: single-ticker single-date fetch
# ---------------------------------------------------------------------------

def fetch_price_snapshot(
    ticker: str,
    target_date: date,
    db: Session,
) -> dict | None:
    """
    Fetch OHLCV for *ticker* on *target_date* and upsert into ``price_snapshots``.

    If *target_date* falls on a weekend or market holiday yfinance returns no
    data; this function returns ``None`` in that case (not an error).

    Parameters
    ----------
    ticker:
        Uppercase ticker symbol, e.g. ``"NVDA"``.
    target_date:
        The specific calendar date to fetch.  Must be a past trading day.
    db:
        Active SQLAlchemy session.  Caller must ``db.commit()`` after.

    Returns
    -------
    dict | None
        Row dict ``{ticker, snapshot_date, open, high, low, close, volume}``
        if data was found, else ``None``.
    """
    ticker = ticker.upper().strip()
    # yfinance end is exclusive, so add one day
    yf_data = _fetch_yf_batch([ticker], start_date=target_date, end_date=target_date + timedelta(days=1))
    time.sleep(REQUEST_DELAY_S)

    if ticker not in yf_data or yf_data[ticker].empty:
        logger.warning(
            "fetch_price_snapshot: no data for %s on %s "
            "(weekend, holiday, or delisted ticker)",
            ticker, target_date,
        )
        return None

    rows = _yf_data_to_rows(yf_data)
    # Filter to just the target date in case yfinance returned adjacent rows
    rows = [r for r in rows if r["snapshot_date"] == target_date]

    if not rows:
        return None

    _upsert_snapshots(db, rows)
    logger.info("fetch_price_snapshot: stored %s @ %s", ticker, target_date)
    return rows[0]


# ---------------------------------------------------------------------------
# Public: bulk multi-ticker history fetch
# ---------------------------------------------------------------------------

def fetch_bulk_prices(
    tickers: list[str],
    db: Session,
    lookback_days: int = 30,
) -> int:
    """
    Fetch OHLCV price history for *tickers* over the past *lookback_days*
    calendar days and upsert all rows into ``price_snapshots``.

    Tickers are processed in batches of ``BATCH_SIZE`` (default 50) with a
    ``REQUEST_DELAY_S`` sleep between batch calls to respect rate limits.

    Parameters
    ----------
    tickers:
        List of ticker symbols.  Duplicates and empty strings are removed.
    db:
        Active SQLAlchemy session.  Caller must ``db.commit()`` after.
    lookback_days:
        How many calendar days of history to fetch (default 30).
        Only trading days within this window will have data.

    Returns
    -------
    int
        Total rows upserted across all batches.
    """
    # Deduplicate and normalise
    clean = list({t.upper().strip() for t in tickers if t and t.strip()})
    if not clean:
        logger.warning("fetch_bulk_prices called with empty ticker list")
        return 0

    end_date   = date.today() + timedelta(days=1)   # exclusive upper bound
    start_date = date.today() - timedelta(days=lookback_days)

    batches    = [clean[i : i + BATCH_SIZE] for i in range(0, len(clean), BATCH_SIZE)]
    total_rows = 0
    failed_tickers: list[str] = []

    logger.info(
        "fetch_bulk_prices: %d tickers, %d batches, lookback=%d days",
        len(clean), len(batches), lookback_days,
    )

    for batch_idx, batch in enumerate(batches, start=1):
        logger.debug("Batch %d/%d: %s…", batch_idx, len(batches), batch[:4])

        try:
            yf_data = _fetch_yf_batch(batch, start_date=start_date, end_date=end_date)
        except Exception as exc:
            logger.error("Batch %d failed unexpectedly: %s", batch_idx, exc)
            failed_tickers.extend(batch)
            time.sleep(REQUEST_DELAY_S)
            continue

        missing = set(batch) - set(yf_data.keys())
        if missing:
            logger.warning(
                "Batch %d: no data for %d ticker(s): %s",
                batch_idx, len(missing), sorted(missing),
            )
            failed_tickers.extend(missing)

        rows = _yf_data_to_rows(yf_data)
        upserted = _upsert_snapshots(db, rows)
        total_rows += upserted

        logger.debug("Batch %d: %d rows upserted", batch_idx, upserted)

        # Delay before the *next* batch (skip after the last one)
        if batch_idx < len(batches):
            time.sleep(REQUEST_DELAY_S)

    if failed_tickers:
        logger.warning(
            "fetch_bulk_prices: %d ticker(s) returned no data: %s",
            len(failed_tickers), sorted(failed_tickers),
        )

    logger.info("fetch_bulk_prices complete: %d total rows upserted", total_rows)
    return total_rows


# ---------------------------------------------------------------------------
# Public: signal tracking — price change since print
# ---------------------------------------------------------------------------

def get_price_change_since_signal(
    ticker: str,
    signal_date: date,
    db: Session,
) -> float | None:
    """
    Return the percentage price change from *signal_date* to today.

    Used by the scanner to track whether the thesis played out:
    a large dark pool print on *signal_date* should be followed by
    a price move within 5–10 trading days.

    Lookup order
    ------------
    1. ``price_snapshots`` table (fast, no network call).
    2. yfinance direct fetch if the DB has insufficient data for the range
       (e.g. first run, or signal is very recent).

    Parameters
    ----------
    ticker:
        Uppercase ticker symbol.
    signal_date:
        Date the dark pool signal was generated (the "entry" reference point).
    db:
        Active SQLAlchemy session.

    Returns
    -------
    float | None
        ``(current_close - signal_close) / signal_close * 100``, rounded to
        two decimal places, or ``None`` if either price is unavailable.
    """
    ticker = ticker.upper().strip()

    signal_close = _get_close_from_db(db, ticker, signal_date, direction="forward")
    current_close = _get_close_from_db(db, ticker, date.today(), direction="backward")

    # Fall back to a lightweight yfinance fetch if either end is missing
    if signal_close is None or current_close is None:
        logger.debug(
            "get_price_change_since_signal: DB miss for %s, falling back to yfinance",
            ticker,
        )
        yf_data = _fetch_yf_batch(
            [ticker],
            start_date = signal_date,
            end_date   = date.today() + timedelta(days=1),
        )
        time.sleep(REQUEST_DELAY_S)

        if ticker not in yf_data or yf_data[ticker].empty:
            logger.warning(
                "get_price_change_since_signal: no price data for %s since %s",
                ticker, signal_date,
            )
            return None

        t_df = yf_data[ticker]
        close_series = t_df["close"].dropna()
        if close_series.empty:
            return None

        # signal_close = first available close on or after signal_date
        if signal_close is None:
            signal_close = float(close_series.iloc[0])
        # current_close = last available close
        if current_close is None:
            current_close = float(close_series.iloc[-1])

    if signal_close == 0:
        logger.warning(
            "get_price_change_since_signal: signal_close is 0 for %s, cannot compute change",
            ticker,
        )
        return None

    pct_change = (current_close - signal_close) / signal_close * 100
    return round(pct_change, 2)


def _get_close_from_db(
    db: Session,
    ticker: str,
    target_date: date,
    direction: str,  # "forward" | "backward"
    tolerance_days: int = 5,
) -> float | None:
    """
    Find the closest ``close`` price in ``price_snapshots`` within
    *tolerance_days* of *target_date*.

    direction="forward"  → find the first trading day >= target_date
    direction="backward" → find the last  trading day <= target_date
    """
    if direction == "forward":
        row = (
            db.query(PriceSnapshot)
            .filter(
                PriceSnapshot.ticker == ticker,
                PriceSnapshot.snapshot_date >= target_date,
                PriceSnapshot.snapshot_date <= target_date + timedelta(days=tolerance_days),
                PriceSnapshot.close.isnot(None),
            )
            .order_by(PriceSnapshot.snapshot_date.asc())
            .first()
        )
    else:
        row = (
            db.query(PriceSnapshot)
            .filter(
                PriceSnapshot.ticker == ticker,
                PriceSnapshot.snapshot_date <= target_date,
                PriceSnapshot.snapshot_date >= target_date - timedelta(days=tolerance_days),
                PriceSnapshot.close.isnot(None),
            )
            .order_by(PriceSnapshot.snapshot_date.desc())
            .first()
        )

    return float(row.close) if row else None


# ---------------------------------------------------------------------------
# Public: ticker metadata (sector, industry, name, market cap)
# ---------------------------------------------------------------------------

def fetch_ticker_meta(
    tickers: list[str],
    db: Session,
    batch_size: int = 100,
) -> int:
    """
    Fetch and upsert sector, industry, company name, and market cap for
    *tickers* using ``yfinance.Ticker.fast_info`` and ``Ticker.info``.

    Uses a fast path (fast_info) for market cap to avoid the slow full-info
    call on every ticker; falls back to info only when needed.

    Parameters
    ----------
    tickers:
        List of ticker symbols (will be deduped and uppercased).
    db:
        Active SQLAlchemy session.  Caller must ``db.commit()`` after.
    batch_size:
        Number of tickers to process before flushing to the DB (default 100).

    Returns
    -------
    int
        Number of rows upserted.
    """
    clean = list({t.upper().strip() for t in tickers if t and t.strip()})
    if not clean:
        return 0

    logger.info("fetch_ticker_meta: fetching meta for %d tickers", len(clean))

    existing_map: dict[str, TickerMeta] = {
        r.ticker: r
        for r in db.query(TickerMeta).filter(TickerMeta.ticker.in_(clean)).all()
    }

    new_objects: list[TickerMeta] = []
    update_count = 0
    fetched = 0

    for ticker in clean:
        try:
            info = yf.Ticker(ticker).info
            sector     = info.get("sector")     or None
            industry   = info.get("industry")   or None
            name       = (info.get("longName") or info.get("shortName") or None)
            market_cap = info.get("marketCap")  or None

            if ticker in existing_map:
                obj = existing_map[ticker]
                obj.sector     = sector
                obj.industry   = industry
                obj.name       = name
                obj.market_cap = market_cap
                obj.updated_at = datetime.utcnow()
                update_count  += 1
            else:
                new_objects.append(TickerMeta(
                    ticker     = ticker,
                    sector     = sector,
                    industry   = industry,
                    name       = name,
                    market_cap = market_cap,
                ))
            fetched += 1
        except Exception as exc:
            logger.warning("fetch_ticker_meta: skipping %s — %s", ticker, exc)

        # Flush periodically to avoid very large transactions
        if fetched % batch_size == 0:
            if new_objects:
                db.bulk_save_objects(new_objects)
                new_objects = []
            db.flush()

        time.sleep(0.05)  # light rate-limit

    if new_objects:
        db.bulk_save_objects(new_objects)
    db.flush()

    total = fetched
    logger.info(
        "fetch_ticker_meta complete: %d fetched (%d new, %d updated)",
        total, total - update_count, update_count,
    )
    return total


# ---------------------------------------------------------------------------
# Public: coiling / flat price detector
# ---------------------------------------------------------------------------

def is_price_flat(
    ticker: str,
    db: Session,
    lookback_days: int = 10,
    threshold_pct: float = FLAT_THRESHOLD_PCT,
) -> bool:
    """
    Return ``True`` if *ticker* has moved less than *threshold_pct* percent
    over the past *lookback_days* calendar days.

    "Flat" price action is a key secondary filter for dark pool setups:
    an unusual dark pool print combined with a coiling price suggests
    institutional accumulation before a move (not a reaction to one).

    Lookup order
    ------------
    1. ``price_snapshots`` table.
    2. yfinance direct fetch if the DB has fewer than 3 rows for the window.

    Parameters
    ----------
    ticker:
        Uppercase ticker symbol.
    db:
        Active SQLAlchemy session (read-only for this function).
    lookback_days:
        Calendar-day window to examine (default 10).
    threshold_pct:
        Maximum % range considered "flat" (default ``FLAT_THRESHOLD_PCT`` = 3.0).

    Returns
    -------
    bool
        ``True`` if the high–low range over the window is < *threshold_pct* %.
        Returns ``False`` (not flat / unknown) if price data cannot be found.
    """
    ticker = ticker.upper().strip()
    start  = date.today() - timedelta(days=lookback_days)

    closes = _get_closes_from_db(db, ticker, start, date.today())

    if len(closes) < 3:
        logger.debug(
            "is_price_flat: only %d DB rows for %s in window, fetching from yfinance",
            len(closes), ticker,
        )
        yf_data = _fetch_yf_batch(
            [ticker],
            start_date = start,
            end_date   = date.today() + timedelta(days=1),
        )
        time.sleep(REQUEST_DELAY_S)

        if ticker not in yf_data or yf_data[ticker].empty:
            logger.warning("is_price_flat: no price data for %s, returning False", ticker)
            return False

        close_series = yf_data[ticker]["close"].dropna()
        closes = close_series.tolist()

    if len(closes) < 2:
        return False

    price_min = min(closes)
    price_max = max(closes)

    if price_min == 0:
        return False

    range_pct = (price_max - price_min) / price_min * 100
    is_flat   = range_pct < threshold_pct

    logger.debug(
        "is_price_flat %s: range=%.2f%% (min=%.4f max=%.4f) → %s",
        ticker, range_pct, price_min, price_max, is_flat,
    )
    return is_flat


def _get_closes_from_db(
    db: Session,
    ticker: str,
    start: date,
    end: date,
) -> list[float]:
    """Return a list of close prices from ``price_snapshots`` for a date range."""
    rows = (
        db.query(PriceSnapshot.close)
        .filter(
            PriceSnapshot.ticker == ticker,
            PriceSnapshot.snapshot_date >= start,
            PriceSnapshot.snapshot_date <= end,
            PriceSnapshot.close.isnot(None),
        )
        .order_by(PriceSnapshot.snapshot_date.asc())
        .all()
    )
    return [float(r.close) for r in rows]

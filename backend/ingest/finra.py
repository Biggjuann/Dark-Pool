"""
FINRA weekly short-sale / dark-pool volume ingestion — local file workflow.

Source
------
FINRA publishes weekly Consolidated NMS (CNMS) short-sale volume files.
Download the latest file from:
  https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/weekly-short-sale-volume-data

File format
-----------
Pipe-delimited .txt with one row per (Symbol, Market) pair per week:
  Date | Symbol | ShortVolume | ShortExemptVolume | TotalVolume | Market

Dark pool proxy
---------------
Rows where Market == 'TRF' (Trade Reporting Facility) represent off-exchange
prints — the best free public proxy for dark pool activity.

  dp_volume    = sum(TotalVolume) for TRF rows, per ticker
  total_volume = sum(TotalVolume) across ALL markets, per ticker
  dp_pct       = dp_volume / total_volume * 100

Public API
----------
  parse_finra_file(filepath)        -> pd.DataFrame  (TRF rows only, cleaned)
  ingest_from_file(filepath, db)    -> dict           (tickers_processed, week_ending, rows_ingested)
  calculate_4wk_averages(db, week_ending) -> None
  get_ingested_weeks(db)            -> list[dict]
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import time

import pandas as pd
import yfinance as yf
from sqlalchemy import func
from sqlalchemy.orm import Session

from models import DailyDarkPoolPrint, DarkPoolPrint, PriceSnapshot

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# FINRA files sometimes append a totals/summary row with a non-ticker symbol.
_SYMBOL_BLOCKLIST: frozenset[str] = frozenset({"FINRA", "TOTAL", "TOTALS"})

# Valid US equity ticker: 1–6 uppercase letters (covers NYSE, NASDAQ, OTC)
_TICKER_PATTERN = r"^[A-Z]{1,6}$"

# Minimum prior weeks of data required before computing rolling averages.
_MIN_HISTORY_WEEKS = 2
_ROLLING_WINDOW = 4   # weeks


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_week_ending(df: pd.DataFrame, filepath: str) -> date:
    """
    Derive the week-ending date from the file's Date column.

    Takes the maximum date value in the column as the canonical week_ending.
    Falls back to today if the column cannot be parsed.
    """
    try:
        raw = df["Date"].dropna()
        if raw.empty:
            raise ValueError("Date column is empty")
        parsed = pd.to_datetime(raw.astype(str).str.strip(), format="%Y%m%d")
        return parsed.max().date()
    except Exception as exc:
        logger.warning(
            "Could not parse Date column (%s) in %s; falling back to today", exc, filepath
        )
        return date.today()


def _read_and_clean_file(filepath: str) -> pd.DataFrame:
    """
    Read a FINRA weekly pipe-delimited .txt file and return a cleaned DataFrame
    with ALL markets (not filtered to TRF).

    Raises
    ------
    ValueError
        If the file is not pipe-delimited, has missing columns, or is empty.
    """
    path = Path(filepath)
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise ValueError(f"Cannot read file {filepath}: {exc}") from exc

    if not content or "|" not in content:
        raise ValueError(
            f"{filepath} does not appear to be a pipe-delimited FINRA file "
            "(empty or wrong format)."
        )

    try:
        df = pd.read_csv(
            pd.io.common.StringIO(content),
            sep="|",
            dtype={
                "Date":              str,
                "Symbol":            str,
                "ShortVolume":       "Int64",
                "ShortExemptVolume": "Int64",
                "TotalVolume":       "Int64",
                "Market":            str,
            },
            skip_blank_lines=True,
        )
    except Exception as exc:
        raise ValueError(f"Failed to parse {filepath}: {exc}") from exc

    required = {"Date", "Symbol", "ShortVolume", "ShortExemptVolume", "TotalVolume", "Market"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{filepath} is missing expected columns: {sorted(missing)}. "
            f"Found: {sorted(df.columns.tolist())}"
        )

    original_len = len(df)

    # Clean symbols
    df = df[df["Symbol"].notna()]
    df["Symbol"] = df["Symbol"].str.strip().str.upper()
    df = df[~df["Symbol"].isin(_SYMBOL_BLOCKLIST)]
    df = df[df["Symbol"].str.match(_TICKER_PATTERN, na=False)]

    # Drop rows with non-positive TotalVolume
    df = df[df["TotalVolume"].notna() & (df["TotalVolume"] > 0)]

    dropped = original_len - len(df)
    if dropped:
        logger.debug("Dropped %d non-ticker/invalid rows from %s", dropped, filepath)

    logger.info(
        "Parsed %s: %d rows, %d unique tickers, %d markets",
        path.name,
        len(df),
        df["Symbol"].nunique(),
        df["Market"].nunique() if not df.empty else 0,
    )
    return df.reset_index(drop=True)


def _parse_date_str(date_str: str) -> date:
    """Parse YYYYMMDD or YYYY-MM-DD string to a date object."""
    s = str(date_str).strip().replace("-", "")
    return datetime.strptime(s, "%Y%m%d").date()


def _upsert_daily_prints(
    db: Session,
    daily: pd.DataFrame,
    week_ending: date,
) -> int:
    """
    Upsert daily DP rows (one per ticker per day) into ``daily_dark_pool_prints``.

    *daily* must have columns: Symbol, Date (YYYYMMDD str), dp_volume,
    total_volume, dp_pct.  Returns the number of rows touched.
    """
    if daily.empty:
        return 0

    # Build lookup by (ticker, print_date) — NOT by week_ending, because a prior
    # run may have committed rows with a different week_ending value (e.g. the run
    # happened mid-week when Thursday was the max date).  The unique constraint on
    # the table is (ticker, print_date), so we must find existing rows by those
    # keys to avoid spurious UNIQUE constraint violations on re-ingest.
    all_tickers    = daily["Symbol"].str.strip().str.upper().unique().tolist()
    all_dates      = [_parse_date_str(str(d)) for d in daily["Date"].unique()]
    existing: dict[tuple, DailyDarkPoolPrint] = {
        (r.ticker, r.print_date): r
        for r in db.query(DailyDarkPoolPrint)
        .filter(
            DailyDarkPoolPrint.ticker.in_(all_tickers),
            DailyDarkPoolPrint.print_date.in_(all_dates),
        )
        .all()
    }

    new_objects: list[DailyDarkPoolPrint] = []
    update_count = 0

    for _, row in daily.iterrows():
        ticker     = str(row["Symbol"])
        print_date = _parse_date_str(str(row["Date"]))
        dp_vol     = int(row["dp_volume"])
        total_vol  = int(row["total_volume"])
        dp_pct_val = float(row["dp_pct"])

        key = (ticker, print_date)
        if key in existing:
            obj = existing[key]
            obj.week_ending  = week_ending   # correct if stored mid-week
            obj.dp_volume    = dp_vol
            obj.total_volume = total_vol
            obj.dp_pct       = dp_pct_val
            update_count += 1
        else:
            new_objects.append(DailyDarkPoolPrint(
                ticker       = ticker,
                print_date   = print_date,
                week_ending  = week_ending,
                dp_volume    = dp_vol,
                total_volume = total_vol,
                dp_pct       = dp_pct_val,
            ))

    if new_objects:
        db.bulk_save_objects(new_objects)
    db.flush()

    logger.info(
        "_upsert_daily_prints week_ending=%s: %d inserted, %d updated",
        week_ending, len(new_objects), update_count,
    )
    return len(new_objects) + update_count


def _upsert_prints(
    db: Session,
    merged: pd.DataFrame,
    week_ending: date,
) -> int:
    """
    Upsert aggregated dark pool rows into ``dark_pool_prints``.

    Uses a bulk-query-then-split pattern:
      1. Fetch all existing (ticker, week_ending) primary keys in one query.
      2. Split *merged* into new rows vs. updates.
      3. bulk_save_objects for inserts; direct attribute updates for updates.

    Returns the total number of rows touched (inserts + updates).
    """
    tickers_in_batch = merged["Symbol"].tolist()

    existing_rows: dict[str, DarkPoolPrint] = {
        row.ticker: row
        for row in db.query(DarkPoolPrint)
        .filter(
            DarkPoolPrint.week_ending == week_ending,
            DarkPoolPrint.ticker.in_(tickers_in_batch),
        )
        .all()
    }

    new_objects: list[DarkPoolPrint] = []
    update_count = 0

    for _, row in merged.iterrows():
        ticker     = str(row["Symbol"])
        dp_vol     = int(row["dp_volume"])
        total_vol  = int(row["total_volume"])
        dp_pct_val = float(row["dp_pct"])

        if ticker in existing_rows:
            obj = existing_rows[ticker]
            obj.dp_volume    = dp_vol
            obj.total_volume = total_vol
            obj.dp_pct       = dp_pct_val
            update_count += 1
        else:
            new_objects.append(
                DarkPoolPrint(
                    ticker         = ticker,
                    week_ending    = week_ending,
                    dp_volume      = dp_vol,
                    dp_trade_count = 0,   # not available in CNMS weekly file
                    total_volume   = total_vol,
                    dp_pct         = dp_pct_val,
                )
            )

    if new_objects:
        db.bulk_save_objects(new_objects)

    db.flush()

    logger.info(
        "_upsert_prints week_ending=%s: %d inserted, %d updated",
        week_ending, len(new_objects), update_count,
    )
    return len(new_objects) + update_count


# ---------------------------------------------------------------------------
# Public: parse local file
# ---------------------------------------------------------------------------

def parse_finra_file(filepath: str) -> pd.DataFrame:
    """
    Parse a local FINRA weekly .txt file and return only the TRF rows.

    Parameters
    ----------
    filepath:
        Absolute or relative path to the pipe-delimited .txt file.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame containing only rows where ``Market == 'TRF'``.
        Columns: Date, Symbol, ShortVolume, ShortExemptVolume, TotalVolume, Market.

    Raises
    ------
    ValueError
        If the file cannot be read, is not pipe-delimited, or is missing columns.
    """
    df = _read_and_clean_file(filepath)
    trf = df[df["Market"].str.strip().str.upper() == "TRF"].copy()
    logger.debug(
        "parse_finra_file: %d TRF rows out of %d total in %s",
        len(trf), len(df), Path(filepath).name,
    )
    return trf.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Public: full file ingest
# ---------------------------------------------------------------------------

def ingest_from_file(filepath: str, db: Session) -> dict:
    """
    Ingest a local FINRA weekly file into ``dark_pool_prints``.

    Steps
    -----
    1. Read and clean the full file (all markets).
    2. Aggregate TRF (dark pool) volume per ticker.
    3. Aggregate total volume across all markets per ticker.
    4. Compute ``dp_pct = dp_volume / total_volume * 100``.
    5. Derive week_ending from the max Date in the file.
    6. Upsert into ``dark_pool_prints``.
    7. Compute/update rolling 4-week averages.
    8. Commit.

    Parameters
    ----------
    filepath:
        Path to the FINRA weekly .txt file (pipe-delimited).
    db:
        Active SQLAlchemy session.

    Returns
    -------
    dict
        ``{tickers_processed, week_ending, rows_ingested}``

    Raises
    ------
    ValueError
        If the file cannot be parsed or contains no TRF rows.
    """
    logger.info("=== Starting FINRA file ingest: %s ===", Path(filepath).name)

    # Read full file (all markets) for total_volume computation
    df = _read_and_clean_file(filepath)

    # TRF (dark pool) volume per ticker
    trf_df = df[df["Market"].str.strip().str.upper() == "TRF"].copy()
    if trf_df.empty:
        raise ValueError(f"No TRF rows found in {filepath}. Check that the file format is correct.")

    # ---- Daily prints (per ticker per day) ----
    daily_trf = (
        trf_df.groupby(["Symbol", "Date"], as_index=False)
        .agg(dp_volume=("TotalVolume", "sum"))
    )
    daily_total = (
        df.groupby(["Symbol", "Date"], as_index=False)
        .agg(total_volume=("TotalVolume", "sum"))
    )
    daily = daily_trf.merge(daily_total, on=["Symbol", "Date"], how="inner")
    daily = daily[daily["total_volume"] > 0].copy()
    daily["dp_pct"] = (daily["dp_volume"] / daily["total_volume"] * 100).round(4)

    week_ending_for_daily = _parse_week_ending(df, filepath)
    _upsert_daily_prints(db, daily, week_ending_for_daily)

    # ---- Weekly aggregation (for scanner) ----
    dp_by_ticker = (
        trf_df.groupby("Symbol", as_index=False)
        .agg(dp_volume=("TotalVolume", "sum"))
    )

    # Total volume across all markets per ticker
    total_by_ticker = (
        df.groupby("Symbol", as_index=False)
        .agg(total_volume=("TotalVolume", "sum"))
    )

    # Inner join: only tickers with both TRF and total rows
    merged = dp_by_ticker.merge(total_by_ticker, on="Symbol", how="inner")
    merged = merged[merged["total_volume"] > 0].copy()
    merged["dp_pct"] = (merged["dp_volume"] / merged["total_volume"] * 100).round(4)

    logger.info(
        "Aggregated %d tickers with TRF activity (of %d tickers in file)",
        len(merged),
        df["Symbol"].nunique(),
    )

    # Derive week_ending from file content
    week_ending = _parse_week_ending(df, filepath)
    logger.info("Derived week_ending=%s", week_ending)

    # Upsert
    rows_ingested = _upsert_prints(db, merged, week_ending)

    # Rolling averages
    calculate_4wk_averages(db, week_ending)

    db.commit()
    logger.info(
        "=== FINRA ingest complete: %d rows upserted for week_ending=%s ===",
        rows_ingested, week_ending,
    )

    return {
        "tickers_processed": len(merged),
        "week_ending":        week_ending,
        "rows_ingested":      rows_ingested,
    }


# ---------------------------------------------------------------------------
# Public: rolling averages
# ---------------------------------------------------------------------------

def calculate_4wk_averages(db: Session, week_ending: date) -> None:
    """
    Update ``dp_volume_4wk_avg`` and ``volume_spike_ratio`` for every ticker
    that has a row in ``dark_pool_prints`` for *week_ending*.

    Algorithm
    ---------
    For each ticker:
      - Fetch up to ``_ROLLING_WINDOW`` weeks of history **before** *week_ending*.
      - If fewer than ``_MIN_HISTORY_WEEKS`` prior rows exist, leave NULL.
      - Otherwise:
          dp_volume_4wk_avg  = mean(prior dp_volumes)
          volume_spike_ratio = current_dp_volume / dp_volume_4wk_avg

    Does **not** commit — the caller owns the transaction boundary.
    """
    rows_this_week = (
        db.query(DarkPoolPrint)
        .filter(DarkPoolPrint.week_ending == week_ending)
        .all()
    )

    if not rows_this_week:
        logger.warning(
            "calculate_4wk_averages: no rows for week_ending=%s, skipping",
            week_ending,
        )
        return

    tickers = [r.ticker for r in rows_this_week]
    current_by_ticker: dict[str, DarkPoolPrint] = {r.ticker: r for r in rows_this_week}

    prior_all = (
        db.query(DarkPoolPrint)
        .filter(
            DarkPoolPrint.ticker.in_(tickers),
            DarkPoolPrint.week_ending < week_ending,
        )
        .order_by(DarkPoolPrint.ticker, DarkPoolPrint.week_ending.desc())
        .all()
    )

    prior_by_ticker: dict[str, list[DarkPoolPrint]] = {}
    for row in prior_all:
        prior_by_ticker.setdefault(row.ticker, []).append(row)

    updated = skipped_no_history = skipped_zero_avg = 0

    for ticker in tickers:
        prior_rows = prior_by_ticker.get(ticker, [])[:_ROLLING_WINDOW]

        if len(prior_rows) < _MIN_HISTORY_WEEKS:
            skipped_no_history += 1
            continue

        prior_volumes = [r.dp_volume for r in prior_rows]
        avg = sum(prior_volumes) / len(prior_volumes)

        current = current_by_ticker[ticker]
        current.dp_volume_4wk_avg = round(avg, 2)

        if avg > 0:
            current.volume_spike_ratio = round(current.dp_volume / avg, 4)
            updated += 1
        else:
            current.volume_spike_ratio = None
            skipped_zero_avg += 1

    db.flush()

    logger.info(
        "calculate_4wk_averages week_ending=%s: "
        "updated=%d, skipped_no_history=%d, skipped_zero_avg=%d",
        week_ending, updated, skipped_no_history, skipped_zero_avg,
    )


# ---------------------------------------------------------------------------
# Public: backfill total_volume from price snapshots
# ---------------------------------------------------------------------------

def backfill_total_volume_from_prices(db: Session, week_ending: date) -> int:
    """
    Re-compute ``total_volume`` and ``dp_pct`` for daily and weekly dark pool
    prints using yfinance (PriceSnapshot) volume as the true all-market
    total volume denominator.

    The FINRA REGSHODAILY API only provides TRF/ORF facility data, so
    exchange-listed stocks end up with total_volume == dp_volume (100 %).
    This function corrects that by using yfinance daily volume — which
    includes NYSE, NASDAQ, BATS, TRF, etc. — as the denominator.

    Must be called AFTER ``fetch_bulk_prices()`` has written PriceSnapshot
    rows for the tickers in this week.  Does NOT commit — caller owns the
    transaction boundary.

    Returns the number of daily rows updated.
    """
    # 1. Load all daily prints for this week
    daily_rows = (
        db.query(DailyDarkPoolPrint)
        .filter(DailyDarkPoolPrint.week_ending == week_ending)
        .all()
    )
    if not daily_rows:
        logger.warning(
            "backfill_total_volume: no daily prints for week_ending=%s", week_ending
        )
        return 0

    tickers = list({r.ticker for r in daily_rows})
    dates   = list({r.print_date for r in daily_rows})

    # 2. Batch-fetch yfinance volumes for all (ticker, date) pairs in one query
    price_vol: dict[tuple, int] = {
        (r.ticker, r.snapshot_date): int(r.volume)
        for r in db.query(
            PriceSnapshot.ticker,
            PriceSnapshot.snapshot_date,
            PriceSnapshot.volume,
        )
        .filter(
            PriceSnapshot.ticker.in_(tickers),
            PriceSnapshot.snapshot_date.in_(dates),
            PriceSnapshot.volume.isnot(None),
            PriceSnapshot.volume > 0,
        )
        .all()
    }

    # 2b. Second yfinance pull — for tickers not covered by price_snapshots,
    #     fetch daily volumes directly so exchange-listed stocks (dp_pct = 100 %
    #     from TRF-only FINRA data) get a real total_volume denominator.
    #
    #     Only attempt this for tickers with meaningful weekly dp_volume (≥ 1M
    #     shares) — everything below that threshold is filtered out by the scanner
    #     anyway, so correcting their dp_pct provides no value and fetching all
    #     ~11k FINRA tickers from yfinance would take 30+ minutes.
    _MIN_BACKFILL_VOLUME = 1_000_000
    high_vol_tickers = {
        r.ticker
        for r in db.query(DarkPoolPrint.ticker)
        .filter(
            DarkPoolPrint.week_ending == week_ending,
            DarkPoolPrint.dp_volume   >= _MIN_BACKFILL_VOLUME,
        )
        .all()
    }
    covered_tickers  = {ticker for (ticker, _) in price_vol}
    missing_tickers  = [t for t in tickers if t not in covered_tickers and t in high_vol_tickers]
    entries_before   = len(price_vol)

    if missing_tickers:
        logger.info(
            "backfill_total_volume: %d tickers missing from price_snapshots — "
            "fetching volumes directly from yfinance",
            len(missing_tickers),
        )
        min_date = min(dates)
        max_date = max(dates)
        start_str = min_date.strftime("%Y-%m-%d")
        end_str   = (max_date + timedelta(days=1)).strftime("%Y-%m-%d")

        _YF_BATCH = 50

        for i in range(0, len(missing_tickers), _YF_BATCH):
            batch = missing_tickers[i : i + _YF_BATCH]
            try:
                raw = yf.download(
                    tickers     = batch,
                    start       = start_str,
                    end         = end_str,
                    progress    = False,
                    threads     = False,
                    auto_adjust = True,
                    group_by    = "ticker",
                )
            except Exception as exc:
                logger.warning(
                    "backfill_total_volume: yfinance batch %d failed: %s",
                    i // _YF_BATCH + 1, exc,
                )
                time.sleep(0.5)
                continue

            if raw is None or raw.empty:
                time.sleep(0.5)
                continue

            # Normalise DatetimeIndex to plain date
            idx = pd.to_datetime(raw.index)
            if idx.tz is not None:
                idx = idx.tz_localize(None)
            raw.index = idx.normalize().date

            # Single-ticker download may lack MultiIndex in some yfinance versions
            if len(batch) == 1 and not isinstance(raw.columns, pd.MultiIndex):
                raw = pd.concat({batch[0]: raw}, axis=1)

            for ticker in batch:
                try:
                    t_df = raw[ticker].copy()
                except KeyError:
                    continue

                t_df.columns = [str(c).lower() for c in t_df.columns]
                if "volume" not in t_df.columns:
                    continue

                for idx_date, row in t_df.iterrows():
                    vol = row.get("volume")
                    if vol is None or pd.isna(vol) or vol <= 0:
                        continue
                    price_vol[(ticker, idx_date)] = int(vol)

            if i + _YF_BATCH < len(missing_tickers):
                time.sleep(0.5)

        logger.info(
            "backfill_total_volume: yfinance fallback added %d (ticker, date) volume entries",
            len(price_vol) - entries_before,
        )

    # 3. Update each daily row individually
    updated_daily = 0
    for row in daily_rows:
        yf_vol = price_vol.get((row.ticker, row.print_date))
        if yf_vol is None:
            continue
        # Guard: dp_pct must stay <= 100 % (data-quality edge cases)
        total = max(yf_vol, row.dp_volume)
        row.total_volume = total
        row.dp_pct       = round(row.dp_volume / total * 100, 4)
        updated_daily   += 1

    db.flush()

    # 4. Re-aggregate weekly totals by summing yfinance volumes across the week
    yf_weekly_vol: dict[str, int] = {
        ticker: sum(price_vol.get((ticker, d), 0) for d in dates)
        for ticker in tickers
    }

    weekly_rows = (
        db.query(DarkPoolPrint)
        .filter(DarkPoolPrint.week_ending == week_ending)
        .all()
    )
    updated_weekly = 0
    for row in weekly_rows:
        yf_vol = yf_weekly_vol.get(row.ticker, 0)
        if yf_vol == 0:
            continue
        total = max(yf_vol, row.dp_volume)
        row.total_volume = total
        row.dp_pct       = round(row.dp_volume / total * 100, 4)
        updated_weekly  += 1

    db.flush()

    logger.info(
        "backfill_total_volume week_ending=%s: "
        "%d daily rows updated, %d weekly rows updated",
        week_ending, updated_daily, updated_weekly,
    )
    return updated_daily


# ---------------------------------------------------------------------------
# Public: ingested weeks query
# ---------------------------------------------------------------------------

def get_ingested_weeks(db: Session) -> list[dict]:
    """
    Return a list of all week_ending dates already in the database,
    ordered most-recent first, with a tickers_processed count per week.

    Returns
    -------
    list[dict]
        Each dict: ``{week_ending: date, tickers_processed: int}``
    """
    rows = (
        db.query(
            DarkPoolPrint.week_ending,
            func.count(DarkPoolPrint.ticker).label("tickers_processed"),
        )
        .group_by(DarkPoolPrint.week_ending)
        .order_by(DarkPoolPrint.week_ending.desc())
        .all()
    )
    return [
        {"week_ending": r.week_ending, "tickers_processed": r.tickers_processed}
        for r in rows
    ]

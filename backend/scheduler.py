"""
APScheduler configuration for Dark Pool Tracker.

Jobs
----
weekly_pipeline      Monday 06:00 ET  — price refresh for top tickers (by dp_volume)
                                        → signal scan → Discord alert
                                        (no FINRA download — use POST /api/ingest/upload
                                        or POST /api/ingest/fetch to ingest new data)

daily_price_refresh  Mon–Fri 16:30 ET — price refresh for watchlist tickers only
                                        (keeps P&L current without hammering yfinance)

daily_ingest         Mon–Fri 17:00 ET — download latest FINRA data → refresh prices
(4pm CST)                               for top-500 + watchlist → re-run scanner
                                        Dark pool data updates Fridays; Mon–Thu the
                                        value is fresh prices + re-scored signals.

Public API
----------
run_full_pipeline()    — execute the Monday re-score pipeline synchronously
setup_scheduler()      — register jobs and start the BackgroundScheduler
shutdown_scheduler()   — gracefully stop the scheduler
"""

from __future__ import annotations

import logging

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from database import SessionLocal

logger = logging.getLogger(__name__)

# All job schedules are anchored to US Eastern Time.
_ET = pytz.timezone("America/New_York")

# Maximum number of tickers to fetch prices for in the weekly run.
_WEEKLY_PRICE_LIMIT = 500

# ---------------------------------------------------------------------------
# Pipeline implementations
# ---------------------------------------------------------------------------

def run_full_pipeline() -> list[dict]:
    """
    Execute the Monday re-score pipeline synchronously.

    This pipeline refreshes prices and re-runs the scanner on existing data.
    It does NOT download a new FINRA file — use POST /api/ingest/upload or
    POST /api/ingest/fetch for that.

    Steps
    -----
    1. Fetch 30 days of OHLCV for the top ``_WEEKLY_PRICE_LIMIT`` tickers
       from the latest ingested week (ordered by dp_volume).
    2. Run the signal scanner; persist Signal rows.
    3. Fire the Discord webhook for high-conviction signals.

    Returns
    -------
    list[dict]
        Signal dicts as returned by ``run_weekly_scan()``.
    """
    from sqlalchemy import func

    from ingest.price import fetch_bulk_prices
    from models import DarkPoolPrint
    from signals.scanner import run_weekly_scan, send_discord_alert

    db = SessionLocal()
    try:
        # Price fetch for top tickers by dark pool volume this week
        latest_week = db.query(func.max(DarkPoolPrint.week_ending)).scalar()
        tickers: list[str] = []
        if latest_week:
            tickers = [
                r.ticker
                for r in (
                    db.query(DarkPoolPrint.ticker)
                    .filter(DarkPoolPrint.week_ending == latest_week)
                    .order_by(DarkPoolPrint.dp_volume.desc())
                    .limit(_WEEKLY_PRICE_LIMIT)
                    .all()
                )
            ]
        if tickers:
            fetch_bulk_prices(tickers, db, lookback_days=30)
            db.commit()
            logger.info("Price fetch complete — %d tickers updated", len(tickers))
        else:
            logger.info(
                "run_full_pipeline: no tickers in database — skipping price fetch. "
                "Upload a FINRA file via POST /api/ingest/upload first."
            )

        # Score every ticker from the latest week
        signals = run_weekly_scan(db)

        # Discord alert for high-conviction signals
        if signals:
            send_discord_alert(signals, db)

        return signals

    finally:
        db.close()


def _run_daily_price_refresh() -> None:
    """
    Refresh price snapshots for all watchlist tickers.

    Fetches the last 5 trading days so the frontend P&L column stays
    up-to-date without over-fetching history.
    """
    from ingest.price import fetch_bulk_prices
    from models import WatchlistEntry

    db = SessionLocal()
    try:
        tickers = [r.ticker for r in db.query(WatchlistEntry.ticker).all()]
        if not tickers:
            logger.info("Daily price refresh: watchlist is empty — nothing to do")
            return

        fetched = fetch_bulk_prices(tickers, db, lookback_days=5)
        db.commit()
        logger.info(
            "Daily price refresh complete — %d ticker(s) updated", fetched
        )
    except Exception:
        logger.exception("Daily price refresh failed")
    finally:
        db.close()


def _run_daily_ingest() -> None:
    """
    Daily 4pm CST (17:00 ET) pipeline.

    Steps
    -----
    1. Download the latest FINRA REGSHODAILY data via API.
       - Fridays: new week's data becomes available.
       - Mon–Thu: re-fetches the same completed week (no-op for dark pool
         data, but keeps the file cache current).
    2. Refresh 30-day OHLCV for the top 500 tickers by dp_volume + all
       watchlist tickers so signals show current prices every afternoon.
    3. Re-run the signal scanner so bias, targets, and stops reflect
       today's closing prices.
    """
    from sqlalchemy import func

    from ingest.finra import ingest_from_file
    from ingest.finra_download import download_latest_finra_file
    from ingest.price import fetch_bulk_prices
    from models import DarkPoolPrint, WatchlistEntry
    from signals.scanner import run_weekly_scan

    db = SessionLocal()
    try:
        # ---- 1. FINRA download ----
        logger.info("Daily ingest: downloading latest FINRA data")
        filepath = download_latest_finra_file()
        result = ingest_from_file(str(filepath), db)
        logger.info(
            "Daily ingest: parsed %d tickers for week_ending=%s",
            result["tickers_processed"], result["week_ending"],
        )

        # ---- 2. Price refresh ----
        latest_week = db.query(func.max(DarkPoolPrint.week_ending)).scalar()
        top_tickers: list[str] = []
        if latest_week:
            top_tickers = [
                r.ticker
                for r in (
                    db.query(DarkPoolPrint.ticker)
                    .filter(DarkPoolPrint.week_ending == latest_week)
                    .order_by(DarkPoolPrint.dp_volume.desc())
                    .limit(_WEEKLY_PRICE_LIMIT)
                    .all()
                )
            ]
        watchlist_tickers = [r.ticker for r in db.query(WatchlistEntry.ticker).all()]
        all_tickers = list(set(top_tickers + watchlist_tickers))

        if all_tickers:
            fetch_bulk_prices(all_tickers, db, lookback_days=30)
            from ingest.finra import backfill_total_volume_from_prices
            backfill_total_volume_from_prices(db, result["week_ending"])
            db.commit()
            logger.info("Daily ingest: prices refreshed for %d tickers", len(all_tickers))

        # ---- 3. Re-score signals ----
        signals = run_weekly_scan(db)
        logger.info("Daily ingest: scanner produced %d signal(s)", len(signals))

    except Exception:
        logger.exception("Daily ingest job failed")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# APScheduler job wrappers
# ---------------------------------------------------------------------------

def _weekly_pipeline_job() -> None:
    """APScheduler entry point for the Monday 06:00 ET job."""
    logger.info("=== Weekly pipeline job started ===")
    try:
        signals = run_full_pipeline()
        logger.info(
            "=== Weekly pipeline complete — %d signal(s) generated ===",
            len(signals),
        )
    except Exception:
        logger.exception("Weekly pipeline job failed")


def _daily_price_refresh_job() -> None:
    """APScheduler entry point for the Mon–Fri 16:30 ET job."""
    logger.info("Daily price refresh job started")
    _run_daily_price_refresh()


def _daily_ingest_job() -> None:
    """APScheduler entry point for the Mon–Fri 17:00 ET (4pm CST) job."""
    logger.info("=== Daily ingest job started ===")
    _run_daily_ingest()
    logger.info("=== Daily ingest job complete ===")


def _run_sentiment_refresh() -> None:
    """
    Fetch new tweets from monitored accounts and re-score sentiment.

    Runs every 4 hours on weekdays (06:00, 10:00, 14:00, 18:00 ET) so
    the recommendations tab reflects intra-day sentiment shifts.
    """
    from ingest.twitter import fetch_and_store_tweets
    from signals.sentiment import run_sentiment_scan

    db = SessionLocal()
    try:
        tweet_result = fetch_and_store_tweets(db)
        results      = run_sentiment_scan(db)
        logger.info(
            "Sentiment refresh: %d new tweets, %d tickers scored",
            tweet_result["tweets_new"], len(results),
        )
    except Exception:
        logger.exception("Sentiment refresh job failed")
    finally:
        db.close()


def _sentiment_refresh_job() -> None:
    """APScheduler entry point for the every-4-hour sentiment job."""
    logger.info("Sentiment refresh job started")
    _run_sentiment_refresh()
    logger.info("Sentiment refresh job complete")


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

_scheduler = BackgroundScheduler(timezone=_ET)


def setup_scheduler() -> BackgroundScheduler:
    """
    Register both cron jobs and start the BackgroundScheduler.

    Call once from the FastAPI lifespan startup hook.

    Job details
    -----------
    weekly_pipeline
        Every Monday at 06:00 ET.
        ``misfire_grace_time=3600``: runs immediately on startup if the server
        was down at fire time and restarts within an hour.

    daily_price_refresh
        Every weekday (Mon–Fri) at 16:30 ET (after US market close).
        ``misfire_grace_time=1800``: 30-minute catch-up window.
    """
    _scheduler.add_job(
        _weekly_pipeline_job,
        trigger=CronTrigger(day_of_week="mon", hour=6, minute=0, timezone=_ET),
        id="weekly_pipeline",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    _scheduler.add_job(
        _daily_price_refresh_job,
        trigger=CronTrigger(
            day_of_week="mon-fri", hour=16, minute=30, timezone=_ET
        ),
        id="daily_price_refresh",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=1800,
    )
    _scheduler.add_job(
        _daily_ingest_job,
        trigger=CronTrigger(
            day_of_week="mon-fri", hour=17, minute=0, timezone=_ET
        ),
        id="daily_ingest",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=1800,
    )
    _scheduler.add_job(
        _sentiment_refresh_job,
        trigger=CronTrigger(
            day_of_week="mon-fri", hour="6,10,14,18", minute=0, timezone=_ET
        ),
        id="sentiment_refresh",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=1800,
    )

    _scheduler.start()
    logger.info(
        "Scheduler started — "
        "weekly pipeline: Monday 06:00 ET | "
        "daily price refresh: Mon-Fri 16:30 ET | "
        "daily ingest: Mon-Fri 17:00 ET | "
        "sentiment refresh: Mon-Fri 06/10/14/18:00 ET"
    )
    return _scheduler


def shutdown_scheduler() -> None:
    """Gracefully stop the scheduler. Safe to call if it was never started."""
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler shut down")

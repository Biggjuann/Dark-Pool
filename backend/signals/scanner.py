"""
Weekly dark pool signal scanner.

Thesis
------
Large dark pool prints precede price moves by 5–10 days.  We score each
ticker that appeared in the current week's FINRA data and flag setups where:
  1. A significant portion of volume went through dark pools (dp_pct).
  2. That volume is unusually high relative to the ticker's recent baseline
     (volume_spike_ratio).
  3. The price has not already broken out (price still "coiling").

Scoring rubric  (max = 100 pts)
--------------------------------
  dp_pct > 40 %                  →  +20 pts
  dp_pct > 55 %    (bonus)       →  +15 pts   (cumulative: 35)
  spike_ratio > 1.5x             →  +20 pts
  spike_ratio > 2.5x (bonus)     →  +15 pts   (cumulative: 35)
  is_price_flat == True          →  +30 pts
                                         ------
                                  max =  100 pts

Levels
------
  HIGH   ≥ 75
  MEDIUM ≥ 50  (saved to DB)
  LOW    < 50  (not saved)

Public API
----------
  score_ticker(ticker, week_ending, db)   → dict | None
  run_weekly_scan(db, week_ending=None)   → list[dict]
  get_top_signals(db, limit=25)           → list[dict]
  send_discord_alert(signals, db=None)    → bool
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, date, datetime, timedelta
from typing import Optional

import httpx
from sqlalchemy import func
from sqlalchemy.orm import Session

from models import DarkPoolPrint, PriceSnapshot, Signal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunable constants
# ---------------------------------------------------------------------------

# Scoring thresholds
_DP_PCT_BASE_THRESHOLD:   float = 40.0   # > this → +25
_DP_PCT_BONUS_THRESHOLD:  float = 55.0   # > this → additional +15
_SPIKE_BASE_THRESHOLD:    float = 1.5    # > this → +25
_SPIKE_BONUS_THRESHOLD:   float = 2.5    # > this → additional +15
_COIL_MIN_PCT:            float = 2.0    # price range >= this % → coiling (not flat/bond-ETF)
_COIL_MAX_PCT:            float = 8.0    # price range < this % → not already broken out

# Minimum dark pool volume to be considered institutional-scale (shares/week)
_MIN_DP_VOLUME: int = 1_000_000

# Point values — kept as named constants so test assertions are readable
_PTS_DP_PCT_BASE:   int = 25
_PTS_DP_PCT_BONUS:  int = 15
_PTS_SPIKE_BASE:    int = 25
_PTS_SPIKE_BONUS:   int = 15
_PTS_PRICE_COIL:    int = 20

# Only signals at or above this threshold are written to the DB
MIN_SCORE_THRESHOLD: int = 30

# How many calendar days of price history to pull for the flatness check
_PRICE_LOOKBACK_DAYS: int = 14

# Discord
_DISCORD_WEBHOOK_URL: str = os.getenv("DISCORD_WEBHOOK_URL", "")
_DISCORD_ALERT_LIMIT: int = 10    # max signals per Discord post
_DISCORD_COLOR: int = 0x5865F2    # Discord Blurple

SIGNAL_TYPE: str = "dp_spike"


# ---------------------------------------------------------------------------
# Pure helpers (no DB, fully unit-testable)
# ---------------------------------------------------------------------------

def _is_coiling_from_closes(
    closes: list[float],
    min_pct: float = _COIL_MIN_PCT,
    max_pct: float = _COIL_MAX_PCT,
) -> bool:
    """
    Return True if the high–low range of *closes* is between *min_pct* and
    *max_pct* percent — i.e. the stock is compressing/coiling, not dead-flat
    (bond ETF) and not already broken out.

    Requires at least 3 data points; returns False for thin data.
    """
    if len(closes) < 3:
        return False
    lo, hi = min(closes), max(closes)
    if lo == 0:
        return False
    range_pct = (hi - lo) / lo * 100
    return min_pct <= range_pct < max_pct


def _score_to_level(score: float) -> str:
    """Map a numeric score to a human-readable level string."""
    if score >= 75:
        return "high"
    if score >= MIN_SCORE_THRESHOLD:
        return "medium"
    return "low"


def _score_dp_print(dp: DarkPoolPrint, closes: list[float]) -> dict:
    """
    Compute the signal score for a single DarkPoolPrint row.

    Parameters
    ----------
    dp:
        ORM row from ``dark_pool_prints``.
    closes:
        List of recent close prices for ``dp.ticker`` (sorted ascending by
        date), used to determine whether price is still coiling.

    Returns
    -------
    dict with keys:
        ticker, week_ending, score, level, signal_type,
        dp_pct, dp_volume, total_volume, volume_spike_ratio,
        is_flat, price_close, breakdown
    """
    pts_dp_pct_base  = 0
    pts_dp_pct_bonus = 0
    pts_spike_base   = 0
    pts_spike_bonus  = 0
    pts_price_coil   = 0

    # ---- Criterion 1 & 2: dark pool percentage ----
    if dp.dp_pct > _DP_PCT_BASE_THRESHOLD:
        pts_dp_pct_base = _PTS_DP_PCT_BASE
    if dp.dp_pct > _DP_PCT_BONUS_THRESHOLD:
        pts_dp_pct_bonus = _PTS_DP_PCT_BONUS

    # ---- Criterion 3 & 4: volume spike ratio ----
    # NULL means insufficient 4-week history; the criteria score 0.
    spike = dp.volume_spike_ratio
    if spike is not None:
        if spike > _SPIKE_BASE_THRESHOLD:
            pts_spike_base = _PTS_SPIKE_BASE
        if spike > _SPIKE_BONUS_THRESHOLD:
            pts_spike_bonus = _PTS_SPIKE_BONUS

    # ---- Criterion 5: price coiling (2–8% range = compressing, not flat/broken) ----
    is_coiling = _is_coiling_from_closes(closes)
    if is_coiling:
        pts_price_coil = _PTS_PRICE_COIL

    score = pts_dp_pct_base + pts_dp_pct_bonus + pts_spike_base + pts_spike_bonus + pts_price_coil

    return {
        "ticker":             dp.ticker,
        "week_ending":        dp.week_ending,
        "score":              float(score),
        "level":              _score_to_level(score),
        "signal_type":        SIGNAL_TYPE,
        "dp_pct":             dp.dp_pct,
        "dp_volume":          dp.dp_volume,
        "total_volume":       dp.total_volume,
        "volume_spike_ratio": spike,
        "is_coiling":         is_coiling,
        "price_close":        closes[-1] if closes else None,
        "breakdown": {
            "dp_pct_base":  pts_dp_pct_base,
            "dp_pct_bonus": pts_dp_pct_bonus,
            "spike_base":   pts_spike_base,
            "spike_bonus":  pts_spike_bonus,
            "price_coil":   pts_price_coil,
        },
    }


# ---------------------------------------------------------------------------
# Internal: DB helpers
# ---------------------------------------------------------------------------

def _load_closes(db: Session, tickers: list[str], lookback_days: int) -> dict[str, list[float]]:
    """
    Bulk-fetch recent close prices for *tickers* in one query.

    Returns ``{ticker: [close, ...]}`` sorted ascending by snapshot_date.
    Tickers with no price data map to an empty list.
    """
    cutoff = date.today() - timedelta(days=lookback_days)
    rows = (
        db.query(PriceSnapshot.ticker, PriceSnapshot.snapshot_date, PriceSnapshot.close)
        .filter(
            PriceSnapshot.ticker.in_(tickers),
            PriceSnapshot.snapshot_date >= cutoff,
            PriceSnapshot.close.isnot(None),
        )
        .order_by(PriceSnapshot.ticker, PriceSnapshot.snapshot_date.asc())
        .all()
    )
    closes: dict[str, list[float]] = {t: [] for t in tickers}
    for ticker, _, close in rows:
        closes[ticker].append(float(close))
    return closes


def _upsert_signal(db: Session, scored: dict) -> None:
    """
    Insert or update a Signal row from a scored dict.

    ``alerted`` is preserved on update so a rerun doesn't clear the flag.
    ``triggered_at`` is only set on initial insert.
    """
    existing = (
        db.query(Signal)
        .filter_by(
            ticker      = scored["ticker"],
            week_ending = scored["week_ending"],
            signal_type = scored["signal_type"],
        )
        .first()
    )
    if existing:
        existing.score = scored["score"]
    else:
        db.add(Signal(
            ticker      = scored["ticker"],
            week_ending = scored["week_ending"],
            signal_type = scored["signal_type"],
            score       = scored["score"],
            alerted     = False,
        ))
    db.flush()


# ---------------------------------------------------------------------------
# Public: score a single ticker
# ---------------------------------------------------------------------------

def score_ticker(
    ticker: str,
    week_ending: date,
    db: Session,
) -> dict | None:
    """
    Score *ticker* for *week_ending* and return a breakdown dict.

    Parameters
    ----------
    ticker:
        Uppercase ticker symbol.
    week_ending:
        The week to score (must match a row in ``dark_pool_prints``).
    db:
        Active SQLAlchemy session.

    Returns
    -------
    dict | None
        Score breakdown dict (see ``_score_dp_print`` for shape), or ``None``
        if no dark pool print exists for this (ticker, week_ending).
    """
    dp = (
        db.query(DarkPoolPrint)
        .filter_by(ticker=ticker.upper(), week_ending=week_ending)
        .first()
    )
    if dp is None:
        logger.debug("score_ticker: no dark pool print for %s / %s", ticker, week_ending)
        return None

    closes_map = _load_closes(db, [ticker.upper()], _PRICE_LOOKBACK_DAYS)
    closes = closes_map.get(ticker.upper(), [])
    return _score_dp_print(dp, closes)


# ---------------------------------------------------------------------------
# Public: run the full weekly scan
# ---------------------------------------------------------------------------

def run_weekly_scan(
    db: Session,
    week_ending: Optional[date] = None,
) -> list[dict]:
    """
    Score every ticker in the most recent week's dark pool prints and persist
    those scoring >= ``MIN_SCORE_THRESHOLD`` to the ``signals`` table.

    Parameters
    ----------
    db:
        Active SQLAlchemy session.  This function calls ``db.commit()``.
    week_ending:
        Target week.  When ``None``, the most recent ``week_ending`` in
        ``dark_pool_prints`` is used.

    Returns
    -------
    list[dict]
        Scored dicts for every signal that met the threshold, sorted
        by score descending.
    """
    # ---- Resolve week ----
    if week_ending is None:
        week_ending = db.query(func.max(DarkPoolPrint.week_ending)).scalar()
        if week_ending is None:
            logger.warning("run_weekly_scan: dark_pool_prints table is empty — nothing to scan")
            return []

    logger.info("=== run_weekly_scan: week_ending=%s ===", week_ending)

    # ---- Load all prints for this week ----
    prints: list[DarkPoolPrint] = (
        db.query(DarkPoolPrint)
        .filter(DarkPoolPrint.week_ending == week_ending)
        .all()
    )
    if not prints:
        logger.warning("run_weekly_scan: no dark pool prints for %s", week_ending)
        return []

    logger.info("Scoring %d tickers for week_ending=%s", len(prints), week_ending)

    # ---- Bulk-load all price closes in a single query ----
    tickers = [dp.ticker for dp in prints]
    closes_by_ticker = _load_closes(db, tickers, _PRICE_LOOKBACK_DAYS)

    # ---- Score each ticker ----
    scored_all: list[dict] = []
    below_threshold = 0
    skipped_volume  = 0
    skipped_quality = 0

    for dp in prints:
        # Gate 1: skip sub-institutional volume
        if dp.dp_volume < _MIN_DP_VOLUME:
            skipped_volume += 1
            continue

        # Gate 2: skip tickers where dp_pct wasn't backfilled from real price data
        # (total_volume == dp_volume means we only have TRF data, no exchange volume)
        if dp.dp_pct >= 99.9:
            skipped_quality += 1
            continue

        closes = closes_by_ticker.get(dp.ticker, [])
        result = _score_dp_print(dp, closes)

        if result["score"] < MIN_SCORE_THRESHOLD:
            below_threshold += 1
            continue

        _upsert_signal(db, result)
        scored_all.append(result)

    db.commit()

    scored_all.sort(key=lambda x: x["score"], reverse=True)

    logger.info(
        "run_weekly_scan complete: %d signals saved, %d below threshold (score < %d), "
        "%d skipped (low volume), %d skipped (bad dp_pct data)",
        len(scored_all), below_threshold, MIN_SCORE_THRESHOLD,
        skipped_volume, skipped_quality,
    )
    return scored_all


# ---------------------------------------------------------------------------
# Public: query top signals for the API / frontend
# ---------------------------------------------------------------------------

def get_top_signals(
    db: Session,
    limit: int = 25,
) -> list[dict]:
    """
    Return the top *limit* signals sorted by score descending, enriched with
    the latest available price close and dark pool print metrics.

    Parameters
    ----------
    db:
        Active SQLAlchemy session.
    limit:
        Maximum number of signals to return (default 25).

    Returns
    -------
    list[dict]
        Each dict contains:
            id, ticker, week_ending, score, level, signal_type, alerted,
            triggered_at, dp_pct, dp_volume, volume_spike_ratio,
            price_close, price_date
    """
    # Subquery: most recent price snapshot date per ticker
    price_subq = (
        db.query(
            PriceSnapshot.ticker,
            func.max(PriceSnapshot.snapshot_date).label("latest_date"),
        )
        .group_by(PriceSnapshot.ticker)
        .subquery("latest_prices")
    )

    rows = (
        db.query(
            Signal,
            DarkPoolPrint.dp_pct,
            DarkPoolPrint.dp_volume,
            DarkPoolPrint.volume_spike_ratio,
            PriceSnapshot.close,
            PriceSnapshot.snapshot_date,
        )
        .outerjoin(
            DarkPoolPrint,
            (DarkPoolPrint.ticker      == Signal.ticker) &
            (DarkPoolPrint.week_ending == Signal.week_ending),
        )
        .outerjoin(price_subq, Signal.ticker == price_subq.c.ticker)
        .outerjoin(
            PriceSnapshot,
            (PriceSnapshot.ticker        == price_subq.c.ticker) &
            (PriceSnapshot.snapshot_date == price_subq.c.latest_date),
        )
        .order_by(Signal.score.desc())
        .limit(limit)
        .all()
    )

    result = []
    for signal, dp_pct, dp_volume, spike_ratio, close, price_date in rows:
        result.append({
            "id":                 signal.id,
            "ticker":             signal.ticker,
            "week_ending":        signal.week_ending,
            "score":              signal.score,
            "level":              _score_to_level(signal.score),
            "signal_type":        signal.signal_type,
            "alerted":            signal.alerted,
            "triggered_at":       signal.triggered_at,
            "dp_pct":             dp_pct,
            "dp_volume":          dp_volume,
            "volume_spike_ratio": spike_ratio,
            "price_close":        float(close) if close is not None else None,
            "price_date":         price_date,
        })

    return result


# ---------------------------------------------------------------------------
# Public: Discord alert
# ---------------------------------------------------------------------------

def send_discord_alert(
    signals: list[dict],
    db: Optional[Session] = None,
) -> bool:
    """
    Post the top ``_DISCORD_ALERT_LIMIT`` signals to the Discord webhook
    configured in the ``DISCORD_WEBHOOK_URL`` environment variable.

    Parameters
    ----------
    signals:
        List of signal dicts (output of ``run_weekly_scan`` or ``get_top_signals``).
        Sorted by score descending before truncation.
    db:
        Optional session.  When provided, ``Signal.alerted`` is set to ``True``
        for every signal that is included in the post.

    Returns
    -------
    bool
        ``True`` if the webhook POST succeeded, ``False`` otherwise.
    """
    webhook_url = _DISCORD_WEBHOOK_URL
    if not webhook_url:
        logger.warning(
            "send_discord_alert: DISCORD_WEBHOOK_URL is not set — skipping alert"
        )
        return False

    if not signals:
        logger.info("send_discord_alert: no signals to post")
        return False

    top = sorted(signals, key=lambda x: x["score"], reverse=True)[:_DISCORD_ALERT_LIMIT]

    # ---- Build embed fields ----
    _level_icon = {"high": "🔴", "medium": "🟡", "low": "⚪"}
    fields = []
    for rank, sig in enumerate(top, start=1):
        level     = sig.get("level") or _score_to_level(sig.get("score", 0))
        icon      = _level_icon.get(level, "⚪")
        spike_str = f"{sig['volume_spike_ratio']:.2f}x" if sig.get("volume_spike_ratio") else "N/A"
        price_str = f"${sig['price_close']:.2f}" if sig.get("price_close") else "N/A"
        dp_pct    = sig.get("dp_pct") or 0.0
        week_str  = str(sig.get("week_ending", ""))

        fields.append({
            "name":   f"{rank}. {sig['ticker']}  {icon} {level.upper()}  —  Score: {int(sig['score'])}",
            "value":  (
                f"DP%: **{dp_pct:.1f}%** | "
                f"Spike: **{spike_str}** | "
                f"Price: **{price_str}** | "
                f"Week: {week_str}"
            ),
            "inline": False,
        })

    # ---- Determine colour from top signal level ----
    top_level = top[0].get("level") or _score_to_level(top[0].get("score", 0))
    embed_color = {
        "high":   0xED4245,   # Discord red
        "medium": 0xFEE75C,   # Discord yellow
    }.get(top_level, _DISCORD_COLOR)

    week_label = str(top[0].get("week_ending", "")) if top else ""
    payload = {
        "username": "Dark Pool Tracker",
        "embeds": [{
            "title":       "🎯 Weekly Dark Pool Signals",
            "description": (
                f"Top institutional accumulation setups — week ending **{week_label}**\n"
                f"Signals shown: {len(top)} of {len(signals)} that scored ≥ {MIN_SCORE_THRESHOLD}"
            ),
            "color":  embed_color,
            "fields": fields,
            "footer": {
                "text": "Dark Pool Tracker  •  FINRA ATS data  •  Not financial advice",
            },
            "timestamp": datetime.now(UTC).isoformat(),
        }],
    }

    # ---- POST to Discord ----
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.post(webhook_url, json=payload)
            resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        logger.error(
            "send_discord_alert: HTTP %d posting to Discord: %s",
            exc.response.status_code, exc,
        )
        return False
    except httpx.RequestError as exc:
        logger.error("send_discord_alert: network error posting to Discord: %s", exc)
        return False

    logger.info("send_discord_alert: posted %d signals to Discord", len(top))

    # ---- Mark alerted in DB ----
    if db is not None:
        alerted_tickers = {s["ticker"] for s in top}
        week_endings    = {s["week_ending"] for s in top}
        (
            db.query(Signal)
            .filter(
                Signal.ticker.in_(alerted_tickers),
                Signal.week_ending.in_(week_endings),
            )
            .update({"alerted": True}, synchronize_session="fetch")
        )
        db.commit()
        logger.debug("Marked %d signal(s) as alerted", len(alerted_tickers))

    return True

"""
Swing-trade recommendation engine.

Combines dark pool data (60%) with Twitter sentiment (40%).

Candidate pool
--------------
Tickers are included if they appear in EITHER:
  - the Signal table (dark pool scanner flagged them), OR
  - the TickerSentiment table (your Twitter accounts mentioned them)

...as long as they also have a DarkPoolPrint this week with dp_pct >= 25%.

This means large-caps like NVDA, PLTR, META that your traders tweet about
will appear even if they don't "spike" enough to generate a Signal row.

Scoring
-------
    dp_score     = Signal.score if available, else scored live from DarkPoolPrint
    combined     = 0.60 * dp_score + 0.40 * sentiment_score
    align bonus  = +8 if BOTH dp_score >= 70 AND sentiment_score >= 70
    no-sent cap  = dp-only tickers capped at 70

Public API
----------
    get_recommendations(db, min_score=40, limit=25) → list[dict]
"""

from __future__ import annotations

import logging
from datetime import date

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import DarkPoolPrint, PriceSnapshot, Signal, TickerMeta, TickerSentiment
from signals.scanner import _load_closes, _score_dp_print

logger = logging.getLogger(__name__)

_DP_WEIGHT       = 0.60
_SENT_WEIGHT     = 0.40
_ALIGN_THRESHOLD = 70.0
_ALIGN_BONUS     = 8.0
_NO_SENT_CAP     = 70.0
_MIN_DP_PCT_GATE = 25.0   # include large-caps with steady dark pool activity


def _level(score: float) -> str:
    if score >= 75:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def _combine(dp_score: float, sentiment_score: float | None) -> float:
    if sentiment_score is None:
        return min(_NO_SENT_CAP, dp_score)
    combined = _DP_WEIGHT * dp_score + _SENT_WEIGHT * sentiment_score
    if dp_score >= _ALIGN_THRESHOLD and sentiment_score >= _ALIGN_THRESHOLD:
        combined = min(100.0, combined + _ALIGN_BONUS)
    return round(combined, 1)


def get_recommendations(
    db: Session,
    min_score: float = 40.0,
    limit: int = 25,
) -> list[dict]:
    """
    Return ranked swing-trade recommendations.

    Includes tickers flagged by the dark pool scanner AND tickers mentioned
    by monitored Twitter accounts that have dark pool data this week.
    """
    latest_week: date | None = db.query(func.max(DarkPoolPrint.week_ending)).scalar()
    if not latest_week:
        return []

    # ── 1. Dark pool prints for this week (dp_pct >= gate) ──
    dp_map: dict[str, DarkPoolPrint] = {
        row.ticker: row
        for row in (
            db.query(DarkPoolPrint)
            .filter(
                DarkPoolPrint.week_ending == latest_week,
                DarkPoolPrint.dp_pct      >= _MIN_DP_PCT_GATE,
            )
            .all()
        )
    }

    # ── 2. Pre-computed signal scores (scanner output) ──
    signal_map: dict[str, float] = {
        row.ticker: row.score
        for row in db.query(Signal).filter_by(week_ending=latest_week).all()
    }

    # ── 3. Sentiment scores (most recent per ticker) ──
    sent_map: dict[str, TickerSentiment] = {}
    for row in (
        db.query(TickerSentiment)
        .order_by(TickerSentiment.date.desc())
        .all()
    ):
        if row.ticker not in sent_map:
            sent_map[row.ticker] = row

    # ── 4. Candidate tickers: Signal rows ∪ Sentiment rows (both need dp data) ──
    candidates = (set(signal_map) | set(sent_map)) & set(dp_map)

    if not candidates:
        return []

    # ── 5. Bulk-load price closes for dp scoring (sentiment tickers w/o Signal) ──
    needs_scoring = [t for t in candidates if t not in signal_map]
    closes_map = _load_closes(db, list(candidates), 14) if needs_scoring else {}

    # ── 6. Company metadata + latest prices ──
    tickers_list = list(candidates)

    meta_map: dict[str, TickerMeta] = {
        m.ticker: m
        for m in db.query(TickerMeta).filter(TickerMeta.ticker.in_(tickers_list)).all()
    }

    price_subq = (
        db.query(
            PriceSnapshot.ticker,
            func.max(PriceSnapshot.snapshot_date).label("max_date"),
        )
        .filter(PriceSnapshot.ticker.in_(tickers_list))
        .group_by(PriceSnapshot.ticker)
        .subquery()
    )
    price_map: dict[str, float] = {
        p.ticker: float(p.close)
        for p in (
            db.query(PriceSnapshot)
            .join(
                price_subq,
                (PriceSnapshot.ticker        == price_subq.c.ticker) &
                (PriceSnapshot.snapshot_date == price_subq.c.max_date),
            )
            .all()
        )
        if p.close is not None
    }

    # ── 7. Score and filter ──
    results = []
    for ticker in candidates:
        dp = dp_map[ticker]

        # dp_score: use pre-computed Signal score if available, else score live
        if ticker in signal_map:
            dp_score = signal_map[ticker]
        else:
            closes  = closes_map.get(ticker, [])
            scored  = _score_dp_print(dp, closes)
            dp_score = scored["score"]

        sent  = sent_map.get(ticker)
        s_score = sent.sentiment_score if sent else None

        combined = _combine(dp_score, s_score)
        if combined < min_score:
            continue

        meta = meta_map.get(ticker)
        results.append({
            "ticker":             ticker,
            "combined_score":     combined,
            "level":              _level(combined),
            "dp_score":           dp_score,
            "dp_pct":             dp.dp_pct,
            "volume_spike_ratio": dp.volume_spike_ratio,
            "sentiment_score":    s_score,
            "tweet_count":        sent.tweet_count if sent else 0,
            "bullish_count":      sent.bullish_count if sent else 0,
            "bearish_count":      sent.bearish_count if sent else 0,
            "has_sentiment":      sent is not None,
            "price_close":        price_map.get(ticker),
            "week_ending":        str(latest_week),
            "name":               meta.name if meta else None,
            "sector":             meta.sector if meta else None,
        })

    # Sentiment-confirmed picks first (both signals present), then dp-only.
    # Within each group, sort by combined_score descending.
    results.sort(key=lambda x: (not x["has_sentiment"], -x["combined_score"]))
    return results[:limit]

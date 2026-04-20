"""
Keyword-based Twitter sentiment analyser.

Scoring
-------
Per tweet:
    bull_pts = count of bullish keyword matches in lowercased tokens
    bear_pts = count of bearish keyword matches in lowercased tokens
    tweet_sentiment = (bull_pts - bear_pts) / (bull_pts + bear_pts + 1)
    # -1 = pure bearish, 0 = neutral, +1 = pure bullish

Per ticker (aggregated over lookback window, recency-weighted):
    avg_sentiment   = weighted mean of tweet_sentiments
    sentiment_score = round(50 + 50 * avg_sentiment, 1)   # 0–100

Recency weight: exponential decay with 3-day half-life so today's tweets
count ~8x more than a week-old tweet.

Public API
----------
    score_tweet(text)                → float  (-1 to +1)
    run_sentiment_scan(db)           → list[dict]
    get_ticker_sentiment(ticker, db) → dict | None
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta

from sqlalchemy.orm import Session

from models import TickerSentiment, Tweet

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Keyword tables
# ---------------------------------------------------------------------------

_BULLISH: frozenset[str] = frozenset({
    # Direct buy / long signals
    "long", "longed", "longing", "buy", "bought", "buying",
    "calls", "call", "leaps", "longs",
    # Entry / accumulation
    "loaded", "loading", "entry", "entering", "entered", "starter",
    "add", "adding", "added", "accumulate", "accumulating", "position",
    # Bullish technical setups
    "breakout", "breaking", "broke",
    "bounce", "bouncing", "bounced",
    "squeeze", "coiling", "coil",
    "support", "reversal", "momentum",
    # Enthusiasm / price action
    "ripping", "rip", "runner", "running",
    "surge", "surging",
    # Sentiment words
    "bullish", "bull", "love", "setup", "watching",
    # Emoji (raw characters treated as tokens)
    "🚀", "🟢", "💚", "✅", "🔥", "📈", "💰",
})

_BEARISH: frozenset[str] = frozenset({
    # Direct short / sell signals
    "short", "shorted", "shorting", "sell", "sold", "selling",
    "puts", "put",
    # Exit signals
    "exit", "exited", "exiting", "closed",
    # Bearish technical signals
    "breakdown", "dump", "dumping", "dumped",
    "dropping", "drop", "falling", "fell",
    "fail", "failed", "rejection", "rejected",
    "resistance", "fade", "fading",
    # Sentiment words
    "bearish", "bear", "avoid", "caution", "careful",
    "overextended", "extended",
    # Emoji
    "🔴", "📉", "🩸", "💀",
})

_SENTIMENT_LOOKBACK_DAYS = 7
_HALFLIFE_DAYS           = 3.0   # recency half-life


# ---------------------------------------------------------------------------
# Pure helpers (no DB, unit-testable)
# ---------------------------------------------------------------------------

def score_tweet(text: str) -> float:
    """
    Return a sentiment score in [-1, +1] for a single tweet.

    -1 = strongly bearish  |  0 = neutral  |  +1 = strongly bullish
    """
    tokens = set(text.lower().split())
    bull   = len(tokens & _BULLISH)
    bear   = len(tokens & _BEARISH)
    return (bull - bear) / (bull + bear + 1)


def _decay_weight(tweet_time: datetime, now: datetime) -> float:
    """Exponential decay: newer tweets weighted higher. Half-life = 3 days."""
    age_days = max(0.0, (now - tweet_time).total_seconds() / 86_400)
    return math.exp(-math.log(2) * age_days / _HALFLIFE_DAYS)


# ---------------------------------------------------------------------------
# DB-level functions
# ---------------------------------------------------------------------------

def run_sentiment_scan(
    db: Session,
    lookback_days: int = _SENTIMENT_LOOKBACK_DAYS,
) -> list[dict]:
    """
    Score every ticker mentioned in recent tweets and upsert ticker_sentiment.

    Parameters
    ----------
    db:
        Active SQLAlchemy session.  Calls db.commit().
    lookback_days:
        How many days of tweets to include.

    Returns
    -------
    list[dict]
        One dict per ticker: ticker, date, sentiment_score,
        tweet_count, bullish_count, bearish_count.
    """
    cutoff = datetime.utcnow() - timedelta(days=lookback_days)
    tweets = (
        db.query(Tweet)
        .filter(Tweet.created_at >= cutoff)
        .order_by(Tweet.created_at.asc())
        .all()
    )

    if not tweets:
        logger.info("run_sentiment_scan: no tweets in last %d days", lookback_days)
        return []

    now   = datetime.utcnow()
    today = date.today()

    # Accumulate per-ticker stats
    ticker_stats: dict[str, dict] = {}

    for tw in tweets:
        if not tw.tickers:
            continue

        raw_score = score_tweet(tw.text)
        weight    = _decay_weight(tw.created_at, now)

        for ticker in tw.tickers.split(","):
            ticker = ticker.strip().upper()
            if not ticker or len(ticker) > 5:
                continue

            if ticker not in ticker_stats:
                ticker_stats[ticker] = {
                    "weighted_sum":  0.0,
                    "weight_total":  0.0,
                    "tweet_count":   0,
                    "bullish_count": 0,
                    "bearish_count": 0,
                }

            s = ticker_stats[ticker]
            s["weighted_sum"]  += raw_score * weight
            s["weight_total"]  += weight
            s["tweet_count"]   += 1
            if raw_score > 0.05:
                s["bullish_count"] += 1
            elif raw_score < -0.05:
                s["bearish_count"] += 1

    results: list[dict] = []

    for ticker, s in ticker_stats.items():
        if s["weight_total"] == 0:
            continue

        avg_sentiment   = s["weighted_sum"] / s["weight_total"]
        sentiment_score = max(0.0, min(100.0, round(50.0 + 50.0 * avg_sentiment, 1)))

        # Upsert today's row
        row = (
            db.query(TickerSentiment)
            .filter_by(ticker=ticker, date=today)
            .first()
        )
        if row:
            row.sentiment_score = sentiment_score
            row.tweet_count     = s["tweet_count"]
            row.bullish_count   = s["bullish_count"]
            row.bearish_count   = s["bearish_count"]
            row.last_updated    = now
        else:
            db.add(TickerSentiment(
                ticker          = ticker,
                date            = today,
                sentiment_score = sentiment_score,
                tweet_count     = s["tweet_count"],
                bullish_count   = s["bullish_count"],
                bearish_count   = s["bearish_count"],
                last_updated    = now,
            ))

        results.append({
            "ticker":          ticker,
            "date":            today,
            "sentiment_score": sentiment_score,
            "tweet_count":     s["tweet_count"],
            "bullish_count":   s["bullish_count"],
            "bearish_count":   s["bearish_count"],
        })

    db.commit()
    logger.info(
        "Sentiment scan: %d tickers scored from %d tweets",
        len(results), len(tweets),
    )
    return results


def get_ticker_sentiment(ticker: str, db: Session) -> dict | None:
    """Return the most recent sentiment record for *ticker*, or None."""
    row = (
        db.query(TickerSentiment)
        .filter_by(ticker=ticker.upper())
        .order_by(TickerSentiment.date.desc())
        .first()
    )
    if not row:
        return None
    return {
        "ticker":          row.ticker,
        "date":            row.date,
        "sentiment_score": row.sentiment_score,
        "tweet_count":     row.tweet_count,
        "bullish_count":   row.bullish_count,
        "bearish_count":   row.bearish_count,
        "last_updated":    row.last_updated,
    }

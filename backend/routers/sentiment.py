"""
REST endpoints for Twitter account management and sentiment data.

Routes
------
GET    /api/sentiment/accounts           — list all monitored handles
POST   /api/sentiment/accounts           — add a handle
DELETE /api/sentiment/accounts/{handle} — deactivate a handle (soft-delete)
POST   /api/sentiment/refresh            — fetch tweets + re-score sentiment
GET    /api/sentiment/status             — last refresh time + tweet totals
GET    /api/sentiment/tickers            — per-ticker sentiment scores (latest)
"""

from __future__ import annotations

import logging
from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from database import get_db
from ingest.twitter import fetch_and_store_tweets
from models import TickerSentiment, Tweet, TwitterAccount
from signals.sentiment import run_sentiment_scan

router = APIRouter(prefix="/api/sentiment", tags=["sentiment"])
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class AddAccountRequest(BaseModel):
    handle: str   # "@astocks92" or "astocks92"


class AccountOut(BaseModel):
    id:        int
    handle:    str
    is_active: bool
    added_at:  date

    model_config = {"from_attributes": True}


class SentimentOut(BaseModel):
    ticker:          str
    date:            date
    sentiment_score: float
    tweet_count:     int
    bullish_count:   int
    bearish_count:   int

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Account management
# ---------------------------------------------------------------------------

@router.get("/accounts", response_model=list[AccountOut])
def list_accounts(db: Session = Depends(get_db)):
    """Return all Twitter accounts ordered alphabetically."""
    return (
        db.query(TwitterAccount)
        .order_by(TwitterAccount.handle)
        .all()
    )


@router.post("/accounts", response_model=AccountOut, status_code=201)
def add_account(body: AddAccountRequest, db: Session = Depends(get_db)):
    """
    Add a Twitter handle to the monitoring list.

    If the handle already exists (possibly inactive), reactivates it.
    The leading '@' is stripped automatically.
    """
    handle = body.handle.lstrip("@").strip().lower()
    if not handle:
        raise HTTPException(status_code=422, detail="handle must not be empty")

    existing = db.query(TwitterAccount).filter_by(handle=handle).first()
    if existing:
        existing.is_active = True
        db.commit()
        db.refresh(existing)
        return existing

    account = TwitterAccount(handle=handle)
    db.add(account)
    db.commit()
    db.refresh(account)
    return account


@router.delete("/accounts/{handle}", status_code=204)
def remove_account(handle: str, db: Session = Depends(get_db)):
    """Deactivate a Twitter handle (soft-delete; tweets are retained)."""
    handle = handle.lstrip("@").strip().lower()
    account = db.query(TwitterAccount).filter_by(handle=handle).first()
    if not account:
        raise HTTPException(status_code=404, detail=f"Account '{handle}' not found")
    account.is_active = False
    db.commit()


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------

@router.post("/refresh")
def refresh_sentiment(db: Session = Depends(get_db)):
    """
    Trigger a full sentiment pipeline:
    1. Fetch new tweets from all active accounts.
    2. Re-score sentiment for every mentioned ticker.
    """
    logger.info("Manual sentiment refresh triggered via API")

    tweet_result      = fetch_and_store_tweets(db)
    sentiment_results = run_sentiment_scan(db)

    return {
        "tweets_fetched":    tweet_result["tweets_fetched"],
        "tweets_new":        tweet_result["tweets_new"],
        "tickers_scored":    len(sentiment_results),
        "tickers_mentioned": tweet_result["tickers_mentioned"][:20],
    }


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@router.get("/status")
def sentiment_status(db: Session = Depends(get_db)):
    """Return summary stats: last tweet time, total tweets, tickers tracked."""
    total_tweets   = db.query(func.count(Tweet.id)).scalar() or 0
    latest_tweet   = db.query(func.max(Tweet.created_at)).scalar()
    tickers_tracked = db.query(func.count(func.distinct(TickerSentiment.ticker))).scalar() or 0
    active_accounts = db.query(func.count(TwitterAccount.id)).filter_by(is_active=True).scalar() or 0

    return {
        "total_tweets":      total_tweets,
        "latest_tweet_at":   latest_tweet,
        "tickers_tracked":   tickers_tracked,
        "active_accounts":   active_accounts,
    }


# ---------------------------------------------------------------------------
# Ticker sentiment data
# ---------------------------------------------------------------------------

@router.get("/tickers", response_model=list[SentimentOut])
def list_ticker_sentiments(
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """
    Return the most recent sentiment score for every tracked ticker,
    sorted by sentiment_score descending.
    """
    latest_subq = (
        db.query(
            TickerSentiment.ticker,
            func.max(TickerSentiment.date).label("latest_date"),
        )
        .group_by(TickerSentiment.ticker)
        .subquery()
    )

    rows = (
        db.query(TickerSentiment)
        .join(
            latest_subq,
            (TickerSentiment.ticker == latest_subq.c.ticker) &
            (TickerSentiment.date   == latest_subq.c.latest_date),
        )
        .order_by(TickerSentiment.sentiment_score.desc())
        .limit(limit)
        .all()
    )
    return rows

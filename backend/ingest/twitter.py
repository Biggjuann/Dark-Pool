"""
Twitter API v2 ingestion module.

Uses App-Only Bearer Token authentication (consumer key + consumer secret)
to fetch recent tweets via the search/recent endpoint.

Only tweets containing cashtag mentions ($AAPL, $TSLA, etc.) are stored.
New tweets are deduplicated by tweet_id.

Public API
----------
    fetch_and_store_tweets(db, lookback_days=7) → dict
    seed_default_accounts(db)                   → None
"""

from __future__ import annotations

import base64
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from models import Tweet, TwitterAccount

logger = logging.getLogger(__name__)

_CONSUMER_KEY    = os.getenv("TWITTER_CONSUMER_KEY", "")
_CONSUMER_SECRET = os.getenv("TWITTER_CONSUMER_SECRET", "")
_BEARER_TOKEN    = os.getenv("TWITTER_BEARER_TOKEN", "")

_API_BASE     = "https://api.twitter.com/2"
_MAX_RESULTS  = 100      # per page (Twitter max for search/recent)
_LOOKBACK_DAYS = 7       # Basic tier allows up to 7 days back

# 15 initial accounts from the user's trading list
DEFAULT_ACCOUNTS = [
    "astocks92", "glitch_trades", "thejokertradess", "milkrcg",
    "blondebroker1", "optionscalps", "yam_trades", "tony_mansour",
    "iv_trader", "pharmd_ks", "rileyphunter", "thewiseone888",
    "dmt_doctor", "mattydaytrades_", "gammaedges",
]


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _get_bearer_token() -> str:
    """
    Return the Twitter bearer token.

    Uses TWITTER_BEARER_TOKEN directly if set (preferred).
    Falls back to generating one from consumer key + secret via OAuth 2.0.
    """
    if _BEARER_TOKEN:
        return _BEARER_TOKEN

    if not _CONSUMER_KEY or not _CONSUMER_SECRET:
        raise RuntimeError(
            "Set TWITTER_BEARER_TOKEN (or TWITTER_CONSUMER_KEY + TWITTER_CONSUMER_SECRET) in .env"
        )

    credentials = base64.b64encode(
        f"{_CONSUMER_KEY}:{_CONSUMER_SECRET}".encode()
    ).decode()

    with httpx.Client(timeout=15) as client:
        resp = client.post(
            "https://api.twitter.com/oauth2/token",
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            },
            content=b"grant_type=client_credentials",
        )
        resp.raise_for_status()
        return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

_QUERY_CHAR_LIMIT = 512   # Twitter API v2 max query length

def _build_query_batches(handles: list[str]) -> list[str]:
    """
    Split handles into batches that each produce a query under 512 chars.

    Returns a list of query strings, one per batch.
    """
    suffix   = " has:cashtags"
    queries  = []
    batch: list[str] = []
    batch_len = 0

    for h in handles:
        clause = f"from:{h}"
        # "(clause) has:cashtags" or "(prev OR clause) has:cashtags"
        needed = len("(") + batch_len + (len(" OR ") if batch else 0) + len(clause) + len(") ") + len(suffix)
        if batch and needed > _QUERY_CHAR_LIMIT:
            queries.append("(" + " OR ".join(f"from:{x}" for x in batch) + ")" + suffix)
            batch = [h]
            batch_len = len(clause)
        else:
            batch_len += (len(" OR ") if batch else 0) + len(clause)
            batch.append(h)

    if batch:
        queries.append("(" + " OR ".join(f"from:{x}" for x in batch) + ")" + suffix)

    return queries


def _extract_tickers(text: str, entities: dict | None) -> list[str]:
    """
    Extract ticker symbols from a tweet.

    Prefers the Twitter-provided entities.cashtags (accurate, no false positives),
    falls back to regex for tweets where entities is unavailable.
    """
    if entities and "cashtags" in entities:
        return [ct["tag"].upper() for ct in entities["cashtags"]]
    # Regex fallback: matches $AAPL, $TSLA (1–5 uppercase letters after $)
    return [m.upper() for m in re.findall(r"\$([A-Za-z]{1,5})\b", text)]


def _parse_tweet_time(created_at_str: str) -> datetime:
    """Parse Twitter's RFC 3339 timestamp to a naive UTC datetime."""
    try:
        # Twitter returns e.g. "2026-03-31T12:00:00.000Z"
        dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except (ValueError, AttributeError):
        return datetime.utcnow()


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

def _fetch_page(
    bearer_token: str,
    query: str,
    start_time: str,
    next_token: str | None = None,
) -> dict:
    """Fetch one page of search results from Twitter API v2."""
    params: dict = {
        "query":        query,
        "max_results":  _MAX_RESULTS,
        "tweet.fields": "created_at,text,author_id,entities",
        "expansions":   "author_id",
        "user.fields":  "username",
        "start_time":   start_time,
    }
    if next_token:
        params["next_token"] = next_token

    with httpx.Client(timeout=30) as client:
        resp = client.get(
            f"{_API_BASE}/tweets/search/recent",
            headers={"Authorization": f"Bearer {bearer_token}"},
            params=params,
        )
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Public: main ingestion function
# ---------------------------------------------------------------------------

def fetch_and_store_tweets(db: Session, lookback_days: int = _LOOKBACK_DAYS) -> dict:
    """
    Fetch recent cashtag tweets from all active accounts and persist new ones.

    Parameters
    ----------
    db:
        Active SQLAlchemy session.
    lookback_days:
        Days back to search (max 7 for Basic tier).

    Returns
    -------
    dict: tweets_fetched, tweets_new, tickers_mentioned
    """
    accounts = db.query(TwitterAccount).filter_by(is_active=True).all()
    if not accounts:
        logger.info("fetch_and_store_tweets: no active accounts — skipping")
        return {"tweets_fetched": 0, "tweets_new": 0, "tickers_mentioned": []}

    handles = [a.handle for a in accounts]
    queries = _build_query_batches(handles)
    logger.info(
        "Fetching tweets for %d accounts across %d query batch(es)",
        len(handles), len(queries),
    )

    try:
        bearer_token = _get_bearer_token()
    except Exception as exc:
        logger.error("Failed to obtain Twitter bearer token: %s", exc)
        return {"tweets_fetched": 0, "tweets_new": 0, "tickers_mentioned": []}

    start_time = (
        datetime.utcnow() - timedelta(days=lookback_days)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    tweets_fetched  = 0
    tweets_new      = 0
    tickers_seen: set[str] = set()
    # Track IDs added in this run to catch within-run duplicates that Twitter
    # occasionally returns across paginated results.
    added_this_run: set[str] = set()

    for query in queries:
        next_token: str | None = None
        while True:
            try:
                data = _fetch_page(bearer_token, query, start_time, next_token)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "Twitter API error %d: %s",
                    exc.response.status_code, exc.response.text[:200],
                )
                break
            except httpx.RequestError as exc:
                logger.error("Twitter network error: %s", exc)
                break

            raw_tweets = data.get("data") or []
            if not raw_tweets:
                break

            user_map: dict[str, str] = {
                u["id"]: u["username"].lower()
                for u in data.get("includes", {}).get("users", [])
            }

            for tw in raw_tweets:
                tweets_fetched += 1
                tweet_id = tw["id"]

                if tweet_id in added_this_run:
                    continue
                if db.query(Tweet).filter_by(tweet_id=tweet_id).first():
                    continue

                tickers = _extract_tickers(tw["text"], tw.get("entities"))
                if not tickers:
                    continue

                tickers_seen.update(tickers)
                author_handle = user_map.get(tw.get("author_id", ""), "unknown")

                db.add(Tweet(
                    tweet_id      = tweet_id,
                    author_handle = author_handle,
                    text          = tw["text"][:1024],
                    tickers       = ",".join(tickers),
                    created_at    = _parse_tweet_time(tw.get("created_at", "")),
                ))
                added_this_run.add(tweet_id)
                tweets_new += 1

            try:
                db.commit()
            except Exception as exc:
                db.rollback()
                logger.warning("Tweet commit failed (likely duplicate), rolling back page: %s", exc)

            next_token = data.get("meta", {}).get("next_token")
            if not next_token:
                break

    logger.info(
        "Tweet ingestion: %d fetched, %d new, %d unique tickers",
        tweets_fetched, tweets_new, len(tickers_seen),
    )
    return {
        "tweets_fetched":    tweets_fetched,
        "tweets_new":        tweets_new,
        "tickers_mentioned": sorted(tickers_seen),
    }


# ---------------------------------------------------------------------------
# Public: seed default accounts
# ---------------------------------------------------------------------------

def seed_default_accounts(db: Session) -> None:
    """
    Insert DEFAULT_ACCOUNTS into twitter_accounts if the table is empty.

    Called once at startup so the app is ready to fetch on first run.
    """
    count = db.query(TwitterAccount).count()
    if count > 0:
        return

    for handle in DEFAULT_ACCOUNTS:
        db.add(TwitterAccount(handle=handle, is_active=True))
    db.commit()
    logger.info("Seeded %d default Twitter accounts", len(DEFAULT_ACCOUNTS))

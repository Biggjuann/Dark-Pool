"""
Tickers router — dark pool signal data for the frontend.

Routes
------
GET /api/tickers/signals        — top signals for a given week
GET /api/tickers/search         — ticker autocomplete
GET /api/tickers/{ticker}/history — 12-week DP history for charting
GET /api/tickers/{ticker}/price   — 30-day OHLCV with signal annotations
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from database import get_db
from models import DailyDarkPoolPrint, DarkPoolPrint, PriceSnapshot, Signal, TickerMeta
from signals.scanner import _score_to_level

router = APIRouter(prefix="/api/tickers", tags=["tickers"])


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class PrintOut(BaseModel):
    """One daily dark pool print for the screener."""
    ticker:       str
    print_date:   date
    week_ending:  date
    dp_volume:    int
    total_volume: int
    dp_pct:       float
    dp_dollars:   float | None   # dp_volume × price_close
    price_close:  float | None
    name:         str   | None
    sector:       str   | None
    industry:     str   | None
    bias:         str   | None


class SignalOut(BaseModel):
    ticker:             str
    week_ending:        date
    score:              float
    level:              str           # "high" | "medium" | "low"
    signal_type:        str
    dp_pct:             float | None
    dp_volume:          int   | None
    total_volume:       int   | None
    volume_spike_ratio: float | None
    price_close:        float | None
    alerted:            bool
    # Sector / metadata
    name:               str   | None = None
    sector:             str   | None = None
    industry:           str   | None = None
    # Trade setup fields
    print_price:        float | None = None   # price at time of DP print (near week_ending)
    price_vs_print_pct: float | None = None   # (current - print) / print * 100
    bias:               str   | None = None   # "long" | "short" | "neutral"
    target_long:        float | None = None   # +8% from print price
    target_short:       float | None = None   # -8% from print price
    stop_long:          float | None = None   # -4% from print price (long invalidation)
    stop_short:         float | None = None   # +4% from print price (short invalidation)


class DpHistoryPoint(BaseModel):
    """One data point per week for the dark pool history chart."""
    week_ending:        date
    dp_volume:          int
    dp_pct:             float
    dp_volume_4wk_avg:  float | None
    volume_spike_ratio: float | None
    close:              float | None   # price close on or near week_ending


class PricePoint(BaseModel):
    """One data point per trading day for the price chart."""
    date:       date
    open:       float | None
    high:       float | None
    low:        float | None
    close:      float | None
    volume:     int   | None
    has_signal: bool   # True when a scanner signal was generated on/near this date


class SearchResult(BaseModel):
    ticker:             str
    latest_week_ending: date  | None
    latest_dp_pct:      float | None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _latest_price_subquery(db: Session):
    """Return a subquery that finds the most-recent snapshot_date per ticker."""
    return (
        db.query(
            PriceSnapshot.ticker,
            func.max(PriceSnapshot.snapshot_date).label("latest_date"),
        )
        .group_by(PriceSnapshot.ticker)
        .subquery("lp")
    )


def _find_closest_close(
    closes_by_date: dict[date, float],
    target: date,
    tolerance: int = 5,
) -> float | None:
    """
    Find the close price closest to *target* within *tolerance* calendar days.

    Checks target date first, then alternates forward/back (±1, ±2 …) so
    the first match wins regardless of direction.
    """
    candidates = [target] + [
        target + timedelta(days=offset * sign)
        for offset in range(1, tolerance + 1)
        for sign in (1, -1)
    ]
    for d in candidates:
        if d in closes_by_date:
            return closes_by_date[d]
    return None


# ---------------------------------------------------------------------------
# Trade-setup helpers
# ---------------------------------------------------------------------------

def _get_print_prices(
    db: Session,
    ticker_weeks: list[tuple[str, date]],
    tolerance: int = 5,
) -> dict[str, float | None]:
    """
    Batch-fetch the price close on/near each (ticker, week_ending) pair.

    Returns {ticker: close} using the snapshot date closest to week_ending
    within *tolerance* calendar days.
    """
    if not ticker_weeks:
        return {}

    conditions = [
        and_(
            PriceSnapshot.ticker == t,
            PriceSnapshot.snapshot_date >= (w - timedelta(days=tolerance)),
            PriceSnapshot.snapshot_date <= (w + timedelta(days=2)),
            PriceSnapshot.close.isnot(None),
        )
        for t, w in ticker_weeks
    ]
    rows = (
        db.query(PriceSnapshot.ticker, PriceSnapshot.snapshot_date, PriceSnapshot.close)
        .filter(or_(*conditions))
        .all()
    )

    week_map = {t: w for t, w in ticker_weeks}
    best: dict[str, tuple[int, float]] = {}
    for ticker, snap_date, close in rows:
        if ticker not in week_map:
            continue
        diff = abs((snap_date - week_map[ticker]).days)
        if ticker not in best or diff < best[ticker][0]:
            best[ticker] = (diff, float(close))

    return {t: best[t][1] if t in best else None for t, _ in ticker_weeks}


def _trade_setup(current: float | None, print_p: float | None) -> dict:
    """
    Compute bias, percentage move, targets, and stops from the current and
    print-date prices.

    Bias logic
    ----------
    long    — current >= print * 1.02  (price confirmed above accumulation zone)
    short   — current <= print * 0.97  (price below print = potential distribution)
    neutral — price coiling within ±2/3 % of print level (wait for confirmation)

    Targets use ±8 % from print price; stops use ±4 %.
    """
    if current is None or print_p is None or print_p == 0:
        return {
            "price_vs_print_pct": None,
            "bias":               None,
            "target_long":        None,
            "target_short":       None,
            "stop_long":          None,
            "stop_short":         None,
        }

    pct = (current - print_p) / print_p * 100

    if current >= print_p * 1.02:
        bias = "long"
    elif current <= print_p * 0.97:
        bias = "short"
    else:
        bias = "neutral"

    return {
        "price_vs_print_pct": round(pct, 2),
        "bias":               bias,
        "target_long":        round(print_p * 1.08, 2),
        "target_short":       round(print_p * 0.92, 2),
        "stop_long":          round(print_p * 0.96, 2),
        "stop_short":         round(print_p * 1.04, 2),
    }


# ---------------------------------------------------------------------------
# GET /api/tickers/signals
# ---------------------------------------------------------------------------

@router.get("/signals", response_model=list[SignalOut])
def list_signals(
    week:      Optional[date]  = Query(None,  description="Week ending (YYYY-MM-DD). Defaults to most recent."),
    min_score: float           = Query(50.0,  ge=0, le=100, description="Minimum signal score"),
    limit:     int             = Query(25,    ge=1, le=500),
    db:        Session         = Depends(get_db),
):
    """
    Return top dark pool signals for *week*, ordered by score descending.

    If *week* is omitted, the most recent week in the signals table is used.
    Returns an empty list — not 404 — when no data has been ingested yet.
    """
    if week is None:
        week = db.query(func.max(Signal.week_ending)).scalar()
        if week is None:
            return []

    price_subq = _latest_price_subquery(db)

    rows = (
        db.query(Signal, DarkPoolPrint, PriceSnapshot.close,
                 TickerMeta.name, TickerMeta.sector, TickerMeta.industry)
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
        .outerjoin(TickerMeta, TickerMeta.ticker == Signal.ticker)
        .filter(Signal.week_ending == week, Signal.score >= min_score)
        .order_by(Signal.score.desc())
        .limit(limit)
        .all()
    )

    # Batch-fetch print-date prices for trade setup computation
    ticker_weeks  = [(sig.ticker, sig.week_ending) for sig, _, _, _, _, _ in rows]
    print_prices  = _get_print_prices(db, ticker_weeks)

    result = []
    for sig, dp, close, name, sector, industry in rows:
        current_p = float(close) if close is not None else None
        print_p   = print_prices.get(sig.ticker)
        setup     = _trade_setup(current_p, print_p)
        result.append(SignalOut(
            ticker             = sig.ticker,
            week_ending        = sig.week_ending,
            score              = sig.score,
            level              = _score_to_level(sig.score),
            signal_type        = sig.signal_type,
            dp_pct             = dp.dp_pct             if dp else None,
            dp_volume          = dp.dp_volume           if dp else None,
            total_volume       = dp.total_volume        if dp else None,
            volume_spike_ratio = dp.volume_spike_ratio  if dp else None,
            price_close        = current_p,
            alerted            = sig.alerted,
            name               = name,
            sector             = sector,
            industry           = industry,
            print_price        = print_p,
            **setup,
        ))
    return result


# ---------------------------------------------------------------------------
# GET /api/tickers/search
# ---------------------------------------------------------------------------

@router.get("/search", response_model=list[SearchResult])
def search_tickers(
    q:  str     = Query(..., min_length=1, max_length=10, description="Ticker prefix"),
    db: Session = Depends(get_db),
):
    """
    Autocomplete: return up to 10 tickers whose symbol starts with *q*.

    Searches across all tickers ever seen in ``dark_pool_prints``.
    Returns the most-recent week_ending and dp_pct alongside each match.
    """
    prefix = q.upper().strip()

    # Subquery: latest week per matching ticker
    latest_subq = (
        db.query(
            DarkPoolPrint.ticker,
            func.max(DarkPoolPrint.week_ending).label("max_week"),
        )
        .filter(DarkPoolPrint.ticker.ilike(f"{prefix}%"))
        .group_by(DarkPoolPrint.ticker)
        .subquery("ls")
    )

    rows = (
        db.query(DarkPoolPrint.ticker, DarkPoolPrint.week_ending, DarkPoolPrint.dp_pct)
        .join(
            latest_subq,
            (DarkPoolPrint.ticker      == latest_subq.c.ticker) &
            (DarkPoolPrint.week_ending == latest_subq.c.max_week),
        )
        .order_by(DarkPoolPrint.ticker.asc())
        .limit(10)
        .all()
    )

    return [
        SearchResult(ticker=r.ticker, latest_week_ending=r.week_ending, latest_dp_pct=r.dp_pct)
        for r in rows
    ]


# ---------------------------------------------------------------------------
# GET /api/tickers/prints  — daily screener
# ---------------------------------------------------------------------------

@router.get("/prints", response_model=list[PrintOut])
def get_recent_prints(
    days:       int            = Query(7,    ge=1, le=30,  description="How many calendar days back"),
    sector:     Optional[str]  = Query(None,              description="Filter by sector (case-insensitive contains)"),
    min_dp_pct: float          = Query(0.0,  ge=0, le=100),
    min_volume: int            = Query(0,    ge=0),
    limit:      int            = Query(200,  ge=1, le=1000),
    db:         Session        = Depends(get_db),
):
    """
    Return individual daily dark pool prints for the screener view.

    Each row represents one ticker on one trading day, enriched with the
    closing price, sector, dollar value, and trade bias.
    """
    cutoff = date.today() - timedelta(days=days)

    # Latest price per ticker for dollar value + bias computation
    price_subq = _latest_price_subquery(db)

    q = (
        db.query(
            DailyDarkPoolPrint,
            PriceSnapshot.close,
            TickerMeta.name,
            TickerMeta.sector,
            TickerMeta.industry,
        )
        .outerjoin(price_subq, DailyDarkPoolPrint.ticker == price_subq.c.ticker)
        .outerjoin(
            PriceSnapshot,
            (PriceSnapshot.ticker        == price_subq.c.ticker) &
            (PriceSnapshot.snapshot_date == price_subq.c.latest_date),
        )
        .outerjoin(TickerMeta, TickerMeta.ticker == DailyDarkPoolPrint.ticker)
        .filter(
            DailyDarkPoolPrint.print_date   >= cutoff,
            DailyDarkPoolPrint.dp_pct       >= min_dp_pct,
            DailyDarkPoolPrint.dp_volume    >= min_volume,
        )
    )

    if sector:
        q = q.filter(TickerMeta.sector.ilike(f"%{sector}%"))

    rows = (
        q.order_by(DailyDarkPoolPrint.print_date.desc(), DailyDarkPoolPrint.dp_volume.desc())
        .limit(limit)
        .all()
    )

    result = []
    for dp, close, name, sec, industry in rows:
        current_p  = float(close) if close is not None else None
        dp_dollars = round(dp.dp_volume * current_p, 0) if current_p else None
        setup      = _trade_setup(current_p, current_p)  # use current as rough proxy; print_price filled via history
        result.append(PrintOut(
            ticker       = dp.ticker,
            print_date   = dp.print_date,
            week_ending  = dp.week_ending,
            dp_volume    = dp.dp_volume,
            total_volume = dp.total_volume,
            dp_pct       = dp.dp_pct,
            dp_dollars   = dp_dollars,
            price_close  = current_p,
            name         = name,
            sector       = sec,
            industry     = industry,
            bias         = setup["bias"],
        ))
    return result


# ---------------------------------------------------------------------------
# GET /api/tickers/{ticker}/history
# ---------------------------------------------------------------------------

@router.get("/{ticker}/history", response_model=list[DpHistoryPoint])
def get_ticker_history(
    ticker: str,
    weeks:  int     = Query(12, ge=1, le=52),
    db:     Session = Depends(get_db),
):
    """
    Return *weeks* weeks of dark pool history for *ticker*.

    Each point includes dp_volume, dp_pct, 4-week average, spike ratio, and
    the price close on the nearest trading day to the week_ending date.
    Raises 404 if the ticker has never appeared in a dark pool print.
    """
    ticker = ticker.upper()
    cutoff = date.today() - timedelta(weeks=weeks)

    dp_rows = (
        db.query(DarkPoolPrint)
        .filter(
            DarkPoolPrint.ticker       == ticker,
            DarkPoolPrint.week_ending  >= cutoff,
        )
        .order_by(DarkPoolPrint.week_ending.asc())
        .all()
    )

    if not dp_rows:
        raise HTTPException(
            status_code=404,
            detail=f"No dark pool history found for {ticker}",
        )

    # Fetch all price rows for this ticker over the same window (+7 day buffer
    # so we can find a close price for the final week_ending).
    price_rows = (
        db.query(PriceSnapshot.snapshot_date, PriceSnapshot.close)
        .filter(
            PriceSnapshot.ticker        == ticker,
            PriceSnapshot.snapshot_date >= cutoff - timedelta(days=7),
            PriceSnapshot.close.isnot(None),
        )
        .all()
    )
    closes_by_date: dict[date, float] = {r.snapshot_date: float(r.close) for r in price_rows}

    return [
        DpHistoryPoint(
            week_ending        = dp.week_ending,
            dp_volume          = dp.dp_volume,
            dp_pct             = dp.dp_pct,
            dp_volume_4wk_avg  = dp.dp_volume_4wk_avg,
            volume_spike_ratio = dp.volume_spike_ratio,
            close              = _find_closest_close(closes_by_date, dp.week_ending),
        )
        for dp in dp_rows
    ]


# ---------------------------------------------------------------------------
# GET /api/tickers/{ticker}/price
# ---------------------------------------------------------------------------

@router.get("/{ticker}/price", response_model=list[PricePoint])
def get_ticker_price(
    ticker: str,
    days:   int     = Query(30, ge=1, le=365),
    db:     Session = Depends(get_db),
):
    """
    Return *days* days of daily OHLCV for *ticker*, with ``has_signal=True``
    on dates that fall within ±3 days of a scanner signal's week_ending.

    This lets the frontend overlay signal markers on the price chart.
    Raises 404 if no price data exists for the ticker.
    """
    ticker = ticker.upper()
    cutoff = date.today() - timedelta(days=days)

    price_rows = (
        db.query(PriceSnapshot)
        .filter(
            PriceSnapshot.ticker        == ticker,
            PriceSnapshot.snapshot_date >= cutoff,
        )
        .order_by(PriceSnapshot.snapshot_date.asc())
        .all()
    )

    if not price_rows:
        raise HTTPException(
            status_code=404,
            detail=f"No price data found for {ticker}",
        )

    # Collect all signal week_ending dates for this ticker in the window
    signal_week_endings: set[date] = {
        row.week_ending
        for row in db.query(Signal.week_ending)
        .filter(Signal.ticker == ticker, Signal.week_ending >= cutoff)
        .all()
    }

    # A price date "has a signal" if it falls within ±3 days of any signal week
    _SIGNAL_PROXIMITY_DAYS = 3

    def _near_signal(d: date) -> bool:
        return any(
            abs((d - wk).days) <= _SIGNAL_PROXIMITY_DAYS
            for wk in signal_week_endings
        )

    return [
        PricePoint(
            date       = p.snapshot_date,
            open       = p.open,
            high       = p.high,
            low        = p.low,
            close      = p.close,
            volume     = p.volume,
            has_signal = _near_signal(p.snapshot_date),
        )
        for p in price_rows
    ]

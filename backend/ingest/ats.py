"""FINRA ATS weekly data ingest.

Download from: https://otctransparency.finra.org/otctransparency/ → ATS Download
Upload via POST /api/ingest/ats/upload with week_ending=YYYY-MM-DD.
Run POST /api/ingest/ats/recompute once after all weeks are uploaded.
"""
from __future__ import annotations

import io
import logging
import re
import shutil
from datetime import date, datetime
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ingest/ats", tags=["ingest-ats"])

UPLOAD_DIR = Path("data/uploads/ats")

TICKER_COL = "issueSymbolIdentifier"
VOLUME_COL = "totalWeeklyShareQuantity"
TRADES_COL = "totalWeeklyTradeCount"
HEADER_MARK = "tierDescription"
REQUIRED = {TICKER_COL, VOLUME_COL, TRADES_COL}

_DATE_RE = re.compile(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})")


def _infer_week_ending(explicit: str | None, filename: str) -> date:
    if explicit:
        return datetime.strptime(explicit, "%Y-%m-%d").date()
    m = _DATE_RE.search(filename)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    raise HTTPException(400, "Pass week_ending=YYYY-MM-DD.")


def _extract_pipe_text(filepath: Path) -> str:
    raw = filepath.read_text(errors="replace")
    lines = raw.splitlines()
    start = next((i for i, ln in enumerate(lines) if ln.lstrip().startswith(HEADER_MARK)), -1)
    if start < 0:
        raise ValueError(f"ATS header row not found (expected line starting with '{HEADER_MARK}').")
    end = len(lines)
    for i in range(start + 1, len(lines)):
        s = lines[i].lstrip()
        if s.startswith("<") or s.startswith("}") or s.startswith("]"):
            end = i
            break
    return "\n".join(lines[start:end])


def _parse_ats(filepath: Path, week_ending: date):
    import pandas as pd
    text = _extract_pipe_text(filepath)
    df = pd.read_csv(io.StringIO(text), sep="|", dtype=str, engine="python")
    missing = REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}. Found: {list(df.columns)}")
    df = df[[TICKER_COL, VOLUME_COL, TRADES_COL]].copy()
    df.columns = ["ticker", "dp_volume", "dp_trade_count"]
    df = df[df["ticker"].astype(str).str.match(r"^[A-Z]{1,6}$", na=False)]
    df["dp_volume"] = pd.to_numeric(df["dp_volume"], errors="coerce").fillna(0).astype("int64")
    df["dp_trade_count"] = pd.to_numeric(df["dp_trade_count"], errors="coerce").fillna(0).astype("int64")
    df = df[df["dp_volume"] > 0]
    agg = df.groupby("ticker", as_index=False).agg(
        dp_volume=("dp_volume", "sum"),
        dp_trade_count=("dp_trade_count", "sum"),
    )
    agg["week_ending"] = week_ending
    return agg


def _upsert(db, rows) -> int:
    from models import DarkPoolPrint
    n = 0
    for _, r in rows.iterrows():
        dp_vol = int(r["dp_volume"])
        existing = db.query(DarkPoolPrint).filter_by(
            ticker=r["ticker"], week_ending=r["week_ending"]
        ).first()
        if existing:
            existing.dp_volume = dp_vol
            existing.dp_trade_count = int(r["dp_trade_count"])
            existing.total_volume = dp_vol
            existing.dp_pct = 100.0
        else:
            db.add(DarkPoolPrint(
                ticker=r["ticker"],
                week_ending=r["week_ending"],
                dp_volume=dp_vol,
                dp_trade_count=int(r["dp_trade_count"]),
                total_volume=dp_vol,
                dp_pct=100.0,
            ))
        n += 1
    db.commit()
    return n


def _recompute_rolling_for_tickers(db, tickers: list[str]) -> int:
    """Fill dp_volume_4wk_avg and volume_spike_ratio for every row of each
    ticker, based on the trailing 4 weeks of DB state."""
    from models import DarkPoolPrint
    updated = 0
    for ticker in tickers:
        rows = (
            db.query(DarkPoolPrint)
            .filter(DarkPoolPrint.ticker == ticker)
            .order_by(DarkPoolPrint.week_ending)
            .all()
        )
        for i, row in enumerate(rows):
            if i >= 4:
                prior = rows[i - 4 : i]
                avg = sum(p.dp_volume for p in prior) / 4.0
                row.dp_volume_4wk_avg = avg
                row.volume_spike_ratio = row.dp_volume / avg if avg > 0 else None
            else:
                row.dp_volume_4wk_avg = None
                row.volume_spike_ratio = None
            updated += 1
    db.commit()
    return updated


@router.post("/upload")
async def upload_ats(
    file: UploadFile = File(...),
    week_ending: str | None = Form(None),
):
    from database import get_db
    week_end = _infer_week_ending(week_ending, file.filename or "")

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{file.filename}"
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        agg = _parse_ats(dest, week_end)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if agg.empty:
        return {"ok": True, "rows": 0, "week_ending": week_end.isoformat()}

    db_gen = get_db()
    db = next(db_gen)
    try:
        n = _upsert(db, agg)
        tickers = agg["ticker"].unique().tolist()
        _recompute_rolling_for_tickers(db, tickers)
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass

    return {
        "ok": True,
        "rows": n,
        "week_ending": week_end.isoformat(),
        "tickers": int(agg["ticker"].nunique()),
    }


@router.post("/recompute")
async def recompute_all():
    """One-shot: recompute rolling averages for every ticker in the DB.
    Call this once after all historical weeks have been uploaded."""
    from database import get_db
    from models import DarkPoolPrint

    db_gen = get_db()
    db = next(db_gen)
    try:
        tickers = [t[0] for t in db.query(DarkPoolPrint.ticker).distinct().all()]
        updated = _recompute_rolling_for_tickers(db, tickers)
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass
    return {"ok": True, "tickers": len(tickers), "rows_updated": updated}

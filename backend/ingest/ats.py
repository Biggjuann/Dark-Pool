"""FINRA ATS (Alternative Trading System) weekly data ingest.

True dark-pool data: aggregates weekly share quantity and trade count per
ticker across all ATSs reported to FINRA.

Download CSVs from:
  https://otctransparency.finra.org/otctransparency/  →  ATS Download

The file is pipe-delimited with columns:
  tierDescription | issueSymbolIdentifier | issueName |
  marketParticipantName | MPID | totalWeeklyShareQuantity |
  totalWeeklyTradeCount | lastUpdateDate

The download file does NOT include the week ending date — you select it on
the FINRA site when downloading. This endpoint accepts week_ending as a
form field; if omitted it tries to parse YYYYMMDD from the filename.
"""
from __future__ import annotations

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
REQUIRED = {TICKER_COL, VOLUME_COL, TRADES_COL}

_DATE_RE = re.compile(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})")


def _infer_week_ending(explicit: str | None, filename: str) -> date:
    if explicit:
        return datetime.strptime(explicit, "%Y-%m-%d").date()
    m = _DATE_RE.search(filename)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    raise HTTPException(
        400,
        "Could not determine week_ending. Pass ?week_ending=YYYY-MM-DD "
        "or ensure the filename contains a YYYYMMDD date.",
    )


def _parse_ats(filepath: Path, week_ending: date):
    import pandas as pd

    df = pd.read_csv(filepath, sep="|", dtype=str)
    missing = REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"ATS file missing required columns: {sorted(missing)}")

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
    except Exception as e:
        raise HTTPException(400, f"parse failed: {e}")

    if agg.empty:
        return {"ok": True, "rows": 0, "week_ending": week_end.isoformat(), "note": "no rows after validation"}

    db_gen = get_db()
    db = next(db_gen)
    try:
        n = _upsert(db, agg)
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

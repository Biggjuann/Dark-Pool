"""FINRA ATS (Alternative Trading System) weekly data ingest.

True dark-pool data: aggregates weekly share quantity and trade count per
ticker across all ATSs reported to FINRA. Writes into the same
DarkPoolPrint schema used by the existing Reg SHO ingest so the scanner
works unchanged.

Download CSVs from:
  https://www.finra.org/finra-data/browse-catalog/
    otc-equity-trading-information/ats-issue-data

total_volume is set equal to dp_volume and dp_pct to 100.0: ATS data is
entirely off-exchange, so those derived fields become trivial. The scanner
still ranks primarily on dp_volume absolutes and volume_spike_ratio.
"""
from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from database import get_db
from models import DarkPoolPrint

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ingest/ats", tags=["ingest-ats"])

UPLOAD_DIR = Path("data/uploads/ats")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

TICKER_COL = "issueSymbolIdentifier"
WEEK_END_COL = "weekEndDate"
VOLUME_COL = "totalWeeklyShareQuantity"
TRADES_COL = "totalWeeklyTradeCount"
REQUIRED = {TICKER_COL, WEEK_END_COL, VOLUME_COL, TRADES_COL}


def _parse_ats(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    missing = REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"ATS file missing required columns: {sorted(missing)}")

    df = df[[TICKER_COL, WEEK_END_COL, VOLUME_COL, TRADES_COL]].copy()
    df.columns = ["ticker", "week_ending", "dp_volume", "dp_trade_count"]

    df = df[df["ticker"].astype(str).str.match(r"^[A-Z]{1,6}$", na=False)]
    df = df.dropna(subset=["ticker", "week_ending", "dp_volume"])

    df["week_ending"] = pd.to_datetime(df["week_ending"]).dt.date
    df["dp_volume"] = df["dp_volume"].astype("int64")
    df["dp_trade_count"] = df["dp_trade_count"].fillna(0).astype("int64")

    agg = df.groupby(["ticker", "week_ending"], as_index=False).agg(
        dp_volume=("dp_volume", "sum"),
        dp_trade_count=("dp_trade_count", "sum"),
    )
    return agg[agg["dp_volume"] > 0]


def _upsert(db: Session, rows: pd.DataFrame) -> int:
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
async def upload_ats(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "ATS file must be .csv")
    dest = UPLOAD_DIR / f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{file.filename}"
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    try:
        agg = _parse_ats(dest)
    except (ValueError, pd.errors.ParserError) as e:
        raise HTTPException(400, str(e))
    if agg.empty:
        return {"ok": True, "rows": 0, "note": "no rows after validation"}
    return {
        "ok": True,
        "rows": _upsert(db, agg),
        "weeks": int(agg["week_ending"].nunique()),
    }

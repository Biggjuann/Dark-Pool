"""
Ingest router — manual FINRA file upload and Playwright auto-download workflow.

Routes
------
POST /api/ingest/upload
    Accepts a multipart .txt file upload (max 50 MB).
    Saves the file to data/uploads/, then runs the full pipeline
    (parse → prices → scan → alert) in a background thread.
    Returns {job_id} immediately.

POST /api/ingest/fetch
    Triggers the Playwright-based FINRA download on the local machine,
    then runs the same pipeline.  Requires ``playwright`` to be installed
    and ``python -m playwright install chromium`` to have been run.
    Returns {job_id} immediately.

GET /api/ingest/status/{job_id}
    Returns pipeline status: {stage, progress, done, error, ...summary fields}.
    Stages: downloading → parsing → prices → scanning → alerting → complete

GET /api/ingest/history
    Returns a list of week_ending dates already in the database, newest first,
    with a tickers_processed count per week.
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import date
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File
from sqlalchemy.orm import Session

from database import SessionLocal
from models import DarkPoolPrint, PriceSnapshot

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ingest", tags=["ingest"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_UPLOADS_DIR = Path("data/uploads")
_MAX_FILE_BYTES = 50 * 1024 * 1024   # 50 MB

# ---------------------------------------------------------------------------
# In-memory job state
# ---------------------------------------------------------------------------

_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()


def _set_stage(job_id: str, stage: str, progress: int = 0) -> None:
    with _jobs_lock:
        _jobs[job_id]["stage"]    = stage
        _jobs[job_id]["progress"] = progress


def _fail_job(job_id: str, error: str) -> None:
    with _jobs_lock:
        _jobs[job_id].update(stage="failed", done=True, error=error)


def _complete_job(job_id: str, **extra) -> None:
    with _jobs_lock:
        _jobs[job_id].update(stage="complete", progress=100, done=True, error=None, **extra)


# ---------------------------------------------------------------------------
# Pipeline runner (shared by upload and fetch flows)
# ---------------------------------------------------------------------------

def _run_pipeline(job_id: str, filepath: Path) -> None:
    """
    Execute the full ingest pipeline for a local FINRA file.

    Stages: parsing → prices → scanning → alerting → complete
    Updates _jobs[job_id] at each stage transition.
    """
    from ingest.finra import ingest_from_file
    from ingest.price import fetch_bulk_prices
    from signals.scanner import run_weekly_scan, send_discord_alert
    from sqlalchemy import func

    db: Session = SessionLocal()
    try:
        # ---- Stage: parsing ----
        _set_stage(job_id, "parsing", 0)
        result = ingest_from_file(str(filepath), db)
        week_ending       = result["week_ending"]
        tickers_processed = result["tickers_processed"]
        _set_stage(job_id, "parsing", 100)
        logger.info(
            "Job %s: parsed %d tickers for week_ending=%s",
            job_id, tickers_processed, week_ending,
        )

        # ---- Stage: prices ----
        _set_stage(job_id, "prices", 0)
        tickers = [
            r.ticker
            for r in (
                db.query(DarkPoolPrint.ticker)
                .filter(DarkPoolPrint.week_ending == week_ending)
                .order_by(DarkPoolPrint.dp_volume.desc())
                .limit(500)
                .all()
            )
        ]
        if tickers:
            fetch_bulk_prices(tickers, db, lookback_days=30)
            from ingest.finra import backfill_total_volume_from_prices
            backfill_total_volume_from_prices(db, week_ending)
            db.commit()
            logger.info("Job %s: price fetch (pass 1) complete — %d tickers", job_id, len(tickers))

        # Pass 2: fetch prices for remaining scanner-eligible tickers that still
        # lack recent history (backfill corrected their dp_pct but didn't write
        # OHLCV — scanner needs closes for coiling, UI needs them for price/move%).
        from datetime import timedelta
        _price_cutoff    = date.today() - timedelta(days=14)
        _MIN_DP_VOL      = 1_000_000
        eligible_tickers = [
            r.ticker
            for r in (
                db.query(DarkPoolPrint.ticker)
                .filter(
                    DarkPoolPrint.week_ending   == week_ending,
                    DarkPoolPrint.dp_volume     >= _MIN_DP_VOL,
                    DarkPoolPrint.dp_pct        <  99.9,
                )
                .all()
            )
        ]
        have_prices = {
            r.ticker
            for r in db.query(PriceSnapshot.ticker)
            .filter(
                PriceSnapshot.ticker.in_(eligible_tickers),
                PriceSnapshot.snapshot_date >= _price_cutoff,
            )
            .distinct()
            .all()
        }
        uncovered = [t for t in eligible_tickers if t not in have_prices]
        if uncovered:
            fetch_bulk_prices(uncovered, db, lookback_days=30)
            db.commit()
            logger.info(
                "Job %s: price fetch (pass 2) complete — %d tickers", job_id, len(uncovered)
            )
        _set_stage(job_id, "prices", 100)

        # ---- Stage: metadata ----
        _set_stage(job_id, "metadata", 0)
        try:
            from ingest.price import fetch_ticker_meta
            # Fetch meta for top 200 tickers by DP volume (sector/industry/name)
            meta_tickers = [
                r.ticker
                for r in (
                    db.query(DarkPoolPrint.ticker)
                    .filter(DarkPoolPrint.week_ending == week_ending)
                    .order_by(DarkPoolPrint.dp_volume.desc())
                    .limit(200)
                    .all()
                )
            ]
            if meta_tickers:
                fetch_ticker_meta(meta_tickers, db)
                db.commit()
                logger.info("Job %s: meta fetch complete — %d tickers", job_id, len(meta_tickers))
        except Exception as exc:
            logger.warning("Job %s: meta fetch failed (non-fatal): %s", job_id, exc)
        _set_stage(job_id, "metadata", 100)

        # ---- Stage: scanning ----
        _set_stage(job_id, "scanning", 0)
        signals = run_weekly_scan(db)
        _set_stage(job_id, "scanning", 100)
        logger.info("Job %s: scanner found %d signal(s)", job_id, len(signals))

        # ---- Stage: alerting ----
        _set_stage(job_id, "alerting", 0)
        if signals:
            send_discord_alert(signals, db)
        _set_stage(job_id, "alerting", 100)

        # ---- Done ----
        top_signals_count = sum(1 for s in signals if s.get("score", 0) >= 75)
        _complete_job(
            job_id,
            week_ending       = str(week_ending),
            tickers_processed = tickers_processed,
            top_signals_count = top_signals_count,
        )
        logger.info(
            "Job %s complete — %d tickers, %d high-conviction signals",
            job_id, tickers_processed, top_signals_count,
        )

    except Exception as exc:
        logger.exception("Pipeline job %s failed", job_id)
        _fail_job(job_id, str(exc))
    finally:
        db.close()


# ---------------------------------------------------------------------------
# POST /api/ingest/upload
# ---------------------------------------------------------------------------

@router.post("/upload")
async def upload_finra_file(file: UploadFile = File(...)):
    """
    Accept a FINRA weekly .txt file upload and run the full ingest pipeline.

    - Validates .txt extension and 50 MB size cap.
    - Saves the file to ``data/uploads/``.
    - Starts the pipeline (parse → prices → scan → alert) in a background thread.
    - Returns ``{job_id}`` immediately for polling via ``GET /api/ingest/status/{job_id}``.
    """
    if not file.filename or not file.filename.lower().endswith(".txt"):
        raise HTTPException(status_code=400, detail="Only .txt files are accepted.")

    content = await file.read()
    if len(content) > _MAX_FILE_BYTES:
        raise HTTPException(status_code=413, detail="File exceeds the 50 MB limit.")
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    _UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    dest = _UPLOADS_DIR / file.filename
    dest.write_bytes(content)
    logger.info("Uploaded file saved: %s (%d bytes)", dest, len(content))

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {"stage": "queued", "progress": 0, "done": False, "error": None}

    threading.Thread(
        target=_run_pipeline,
        args=(job_id, dest),
        daemon=True,
        name=f"ingest-{job_id[:8]}",
    ).start()

    return {"job_id": job_id, "status": "started", "filename": file.filename}


# ---------------------------------------------------------------------------
# POST /api/ingest/fetch
# ---------------------------------------------------------------------------

@router.post("/fetch")
def fetch_from_finra():
    """
    Download the latest FINRA weekly file from regsho.finra.org, then run the pipeline.

    Uses plain HTTP (httpx) — no browser or Playwright required.
    Returns ``{job_id}`` immediately.  The download stage appears as
    ``stage == "downloading"`` in the status response.
    """
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {"stage": "downloading", "progress": 0, "done": False, "error": None}

    def _run() -> None:
        try:
            from ingest.finra_download import get_latest_finra_file
            logger.info("Job %s: downloading from regsho.finra.org", job_id)
            filepath = get_latest_finra_file()
            logger.info("Job %s: download complete — %s", job_id, filepath)
            _run_pipeline(job_id, filepath)
        except Exception as exc:
            logger.exception("Fetch pipeline job %s failed during download", job_id)
            _fail_job(job_id, str(exc))

    threading.Thread(
        target=_run,
        daemon=True,
        name=f"fetch-{job_id[:8]}",
    ).start()

    return {"job_id": job_id, "status": "started"}


# ---------------------------------------------------------------------------
# GET /api/ingest/status/{job_id}
# ---------------------------------------------------------------------------

@router.get("/status/{job_id}")
def get_job_status(job_id: str):
    """
    Return the current pipeline status for *job_id*.

    Response fields
    ---------------
    stage:             Current stage name (downloading | parsing | prices |
                       scanning | alerting | complete | failed)
    progress:          0–100 within the current stage
    done:              True once the pipeline finishes (success or failure)
    error:             Error message if stage == "failed", else null
    week_ending:       (complete only) ISO date string
    tickers_processed: (complete only) count of tickers ingested
    top_signals_count: (complete only) signals scoring >= 75
    """
    with _jobs_lock:
        job = _jobs.get(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found.")

    return dict(job)


# ---------------------------------------------------------------------------
# GET /api/ingest/history
# ---------------------------------------------------------------------------

@router.get("/history")
def get_ingest_history():
    """
    Return all week_ending dates present in the database, newest first.

    Each entry includes:
      week_ending:       ISO date string
      tickers_processed: number of tickers ingested for that week
    """
    from ingest.finra import get_ingested_weeks

    db: Session = SessionLocal()
    try:
        weeks = get_ingested_weeks(db)
        return [
            {"week_ending": str(w["week_ending"]), "tickers_processed": w["tickers_processed"]}
            for w in weeks
        ]
    finally:
        db.close()

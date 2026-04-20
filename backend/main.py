"""
Dark Pool Tracker — FastAPI application entry point.

Startup behaviour
-----------------
1. ``python-dotenv`` loads ``.env`` before any project module is imported
   so DATABASE_URL and DISCORD_WEBHOOK_URL are available at import time.
2. ``db_init()`` creates all tables on first run (idempotent).
3. The APScheduler ``BackgroundScheduler`` is started; it fires:
     - Every Monday 06:00 ET  — price refresh for top tickers + scan + Discord alert
     - Every weekday 16:30 ET — price refresh for watchlist tickers

FINRA data is ingested manually via the file-upload workflow:
  POST /api/ingest/upload   — upload a .txt file from the FINRA download page
  POST /api/ingest/fetch    — auto-download via Playwright (local machine only)

CORS
----
Allows requests from both the Vite dev server (5173) and the default
React/Next dev port (3000).  Override via the ``CORS_ORIGINS`` env var
(comma-separated, e.g. ``http://myapp.example.com``).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Load .env FIRST — before any project module is imported.
# database.py reads DATABASE_URL at module level, so load_dotenv() must run
# before that import is processed.
# ---------------------------------------------------------------------------
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Standard-library and third-party imports
# ---------------------------------------------------------------------------

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

logging.basicConfig(level=logging.INFO, format="%(levelname)s:     %(name)s — %(message)s")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Project imports  (safe now that .env is loaded)
# ---------------------------------------------------------------------------

from database import db_init
from routers import tickers, watchlist
from routers import ingest as ingest_router
from routers import sentiment as sentiment_router
from routers import recommendations as recommendations_router
from scheduler import setup_scheduler, shutdown_scheduler

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    # ---- Startup ----
    db_init()
    setup_scheduler()

    # Seed default Twitter accounts if table is empty
    from database import SessionLocal
    from ingest.twitter import seed_default_accounts
    _db = SessionLocal()
    try:
        seed_default_accounts(_db)
    finally:
        _db.close()

    yield

    # ---- Shutdown ----
    shutdown_scheduler()


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

_raw_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:5173")
_allowed_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app = FastAPI(
    title       = "Dark Pool Tracker",
    version     = "0.2.0",
    description = (
        "Weekly institutional accumulation signal tracker using FINRA ATS data. "
        "Upload the weekly FINRA file via POST /api/ingest/upload to populate data."
    ),
    lifespan    = lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = _allowed_origins,
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

app.include_router(tickers.router)
app.include_router(watchlist.router)
app.include_router(ingest_router.router)
app.include_router(sentiment_router.router)
app.include_router(recommendations_router.router)


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

@app.get("/api/health", tags=["meta"])
def health_check():
    """
    Liveness probe.

    Returns ``{"status": "ok"}`` as long as the process is running.
    For a deeper readiness check (DB reachable, data present) use the
    signals endpoint and verify it returns rows.
    """
    return {"status": "ok"}

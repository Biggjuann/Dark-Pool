"""
Integration tests for the Dark Pool Tracker API.

Strategy
--------
All endpoints are exercised against a real SQLite database.  We use
``StaticPool`` so that the request-handler thread, ``_db_is_empty()`` in the
lifespan, and any scheduler background threads all share the **same**
in-memory connection — avoiding the per-thread isolation that the default
``SingletonThreadPool`` would impose.

The patching sequence is:
  1. Create the StaticPool engine + session factory.
  2. Overwrite ``database.engine`` and ``database.SessionLocal`` so that
     ``get_db()`` (defined inside database.py) picks up the new factory.
  3. Overwrite ``main.SessionLocal`` and ``scheduler.SessionLocal`` because
     those modules did ``from database import SessionLocal`` at import time
     and therefore hold their own reference.
  4. Import ``main.app`` *after* all patches so the FastAPI routes close over
     the correct session factory.

Seed data inserted before starting TestClient ensures ``_db_is_empty()``
returns False, suppressing the auto-ingest thread in the lifespan.
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Step 1 — patch the database layer BEFORE importing app modules
# ---------------------------------------------------------------------------
import database as _db
import models
import scheduler as _sched

_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
_factory = sessionmaker(autocommit=False, autoflush=False, bind=_engine)

# Overwrite module attributes so every SessionLocal() call uses the test DB
_db.engine        = _engine
_db.SessionLocal  = _factory
_sched.SessionLocal = _factory      # scheduler imported it at module level

# ---------------------------------------------------------------------------
# Step 2 — import the app after patching
# ---------------------------------------------------------------------------
import main as _main               # noqa: E402  must follow patches above
_main.SessionLocal = _factory      # main imported it at module level

from main import app               # noqa: E402

# ---------------------------------------------------------------------------
# Constants used by seed data
# ---------------------------------------------------------------------------
TODAY = date.today()
WEEK  = TODAY - timedelta(days=TODAY.weekday())   # Monday of current week


# ---------------------------------------------------------------------------
# Module-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def _schema():
    """Create all tables once; drop them after the entire module finishes."""
    models.Base.metadata.create_all(_engine)
    yield
    models.Base.metadata.drop_all(_engine)


@pytest.fixture(scope="module")
def _seed():
    """
    Populate the integration DB with enough data to exercise every endpoint:
      - 2 × DarkPoolPrint  (AAPL high-signal, AMZN low-signal)
      - 1 × older AAPL print for 4-week history
      - 30 × PriceSnapshot for AAPL (one per day, last 30 days)
      - 1 × Signal for AAPL
    """
    db = _factory()
    try:
        # --- dark pool prints ---
        db.add(models.DarkPoolPrint(
            ticker="AAPL", week_ending=WEEK,
            dp_volume=5_000_000, dp_trade_count=0,
            total_volume=10_000_000, dp_pct=50.0,
            dp_volume_4wk_avg=3_000_000.0, volume_spike_ratio=1.67,
        ))
        db.add(models.DarkPoolPrint(
            ticker="AMZN", week_ending=WEEK,
            dp_volume=3_000_000, dp_trade_count=0,
            total_volume=8_000_000, dp_pct=37.5,
            dp_volume_4wk_avg=2_500_000.0, volume_spike_ratio=1.2,
        ))
        db.add(models.DarkPoolPrint(
            ticker="AAPL", week_ending=WEEK - timedelta(weeks=1),
            dp_volume=4_500_000, dp_trade_count=0,
            total_volume=9_000_000, dp_pct=50.0,
            dp_volume_4wk_avg=None, volume_spike_ratio=None,
        ))

        # --- price snapshots (30 days for AAPL) ---
        for i in range(30):
            d = TODAY - timedelta(days=29 - i)
            db.add(models.PriceSnapshot(
                ticker="AAPL", snapshot_date=d,
                open=170.0 + i * 0.05,
                high=172.0 + i * 0.05,
                low=169.0 + i * 0.05,
                close=171.0 + i * 0.05,
                volume=50_000_000,
            ))

        # --- signal ---
        db.add(models.Signal(
            ticker="AAPL", week_ending=WEEK,
            signal_type="dp_spike", score=70.0, alerted=False,
        ))

        db.commit()
    finally:
        db.close()


@pytest.fixture(scope="module")
def client(_seed):
    """
    TestClient backed by the seeded integration DB.

    Because the DB is not empty, the lifespan's ``_db_is_empty()`` check
    returns False and the auto-ingest background thread is never spawned.
    """
    with TestClient(app) as c:
        yield c


# ===========================================================================
# Health
# ===========================================================================

class TestHealth:

    def test_returns_ok(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


# ===========================================================================
# GET /api/tickers/signals
# ===========================================================================

class TestSignals:

    def test_returns_list_with_seeded_data(self, client):
        r = client.get("/api/tickers/signals")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_seeded_signal_fields(self, client):
        sig = client.get("/api/tickers/signals").json()[0]
        for field in (
            "ticker", "week_ending", "score", "level", "signal_type",
            "dp_pct", "dp_volume", "total_volume", "volume_spike_ratio",
            "price_close", "alerted",
        ):
            assert field in sig, f"Missing field: {field}"

    def test_aapl_returned(self, client):
        tickers = [s["ticker"] for s in client.get("/api/tickers/signals").json()]
        assert "AAPL" in tickers

    def test_level_values_valid(self, client):
        for sig in client.get("/api/tickers/signals").json():
            assert sig["level"] in ("high", "medium", "low")

    def test_min_score_filter(self, client):
        # seeded score is 70 — a min_score of 99 should exclude it
        r = client.get("/api/tickers/signals?min_score=99")
        assert r.status_code == 200
        assert r.json() == []

    def test_limit_param(self, client):
        r = client.get("/api/tickers/signals?limit=1")
        assert r.status_code == 200
        assert len(r.json()) <= 1

    def test_unknown_week_returns_empty(self, client):
        r = client.get("/api/tickers/signals?week=1990-01-07")
        assert r.status_code == 200
        assert r.json() == []

    def test_price_close_populated_from_snapshot(self, client):
        # We seeded 30 price rows for AAPL so price_close should be non-null
        aapl = next(
            s for s in client.get("/api/tickers/signals").json()
            if s["ticker"] == "AAPL"
        )
        assert aapl["price_close"] is not None


# ===========================================================================
# GET /api/tickers/search
# ===========================================================================

class TestSearch:

    def test_exact_match(self, client):
        r = client.get("/api/tickers/search?q=AAPL")
        assert r.status_code == 200
        tickers = [x["ticker"] for x in r.json()]
        assert "AAPL" in tickers

    def test_prefix_match(self, client):
        r = client.get("/api/tickers/search?q=A")
        assert r.status_code == 200
        results = r.json()
        assert len(results) >= 2
        assert all(x["ticker"].startswith("A") for x in results)

    def test_result_fields(self, client):
        result = client.get("/api/tickers/search?q=AAPL").json()[0]
        assert "ticker" in result
        assert "latest_week_ending" in result
        assert "latest_dp_pct" in result

    def test_no_match_returns_empty(self, client):
        r = client.get("/api/tickers/search?q=ZZZNOMATCH")
        assert r.status_code == 200
        assert r.json() == []

    def test_missing_q_returns_422(self, client):
        r = client.get("/api/tickers/search")
        assert r.status_code == 422


# ===========================================================================
# GET /api/tickers/{ticker}/history
# ===========================================================================

class TestTickerHistory:

    def test_returns_list(self, client):
        r = client.get("/api/tickers/AAPL/history")
        assert r.status_code == 200
        assert isinstance(r.json(), list)
        assert len(r.json()) >= 1

    def test_data_point_fields(self, client):
        point = client.get("/api/tickers/AAPL/history").json()[0]
        for field in (
            "week_ending", "dp_volume", "dp_pct",
            "dp_volume_4wk_avg", "volume_spike_ratio", "close",
        ):
            assert field in point, f"Missing field: {field}"

    def test_close_price_resolved(self, client):
        # Price snapshots were seeded → at least one point should have close
        points = client.get("/api/tickers/AAPL/history").json()
        assert any(p["close"] is not None for p in points)

    def test_case_insensitive(self, client):
        r = client.get("/api/tickers/aapl/history")
        assert r.status_code == 200
        assert len(r.json()) >= 1

    def test_unknown_ticker_404(self, client):
        r = client.get("/api/tickers/ZZZNOPE/history")
        assert r.status_code == 404

    def test_weeks_param(self, client):
        r = client.get("/api/tickers/AAPL/history?weeks=1")
        assert r.status_code == 200
        # With weeks=1 we should still get the seeded current-week print
        assert len(r.json()) >= 1


# ===========================================================================
# GET /api/tickers/{ticker}/price
# ===========================================================================

class TestTickerPrice:

    def test_returns_list(self, client):
        r = client.get("/api/tickers/AAPL/price")
        assert r.status_code == 200
        assert isinstance(r.json(), list)
        assert len(r.json()) >= 1

    def test_data_point_fields(self, client):
        point = client.get("/api/tickers/AAPL/price").json()[0]
        for field in ("date", "open", "high", "low", "close", "volume", "has_signal"):
            assert field in point, f"Missing field: {field}"

    def test_ohlcv_values_sensible(self, client):
        point = client.get("/api/tickers/AAPL/price").json()[-1]  # most recent
        assert point["close"] > 0
        assert point["high"] >= point["low"]

    def test_has_signal_set_near_week(self, client):
        # The seeded Signal has week_ending=WEEK; price rows around that date
        # should be flagged (±3 days window).
        data = client.get("/api/tickers/AAPL/price").json()
        signal_days = [p for p in data if p["has_signal"]]
        assert len(signal_days) >= 1

    def test_unknown_ticker_404(self, client):
        r = client.get("/api/tickers/ZZZNOPE/price")
        assert r.status_code == 404

    def test_days_param_limits_results(self, client):
        # cutoff = today - days  (inclusive on both ends), so days=7 returns
        # at most 8 rows.  The key assertion is that it returns fewer rows than
        # the default 30-day window.
        r30 = client.get("/api/tickers/AAPL/price?days=30")
        r7  = client.get("/api/tickers/AAPL/price?days=7")
        assert r7.status_code == 200
        assert 0 < len(r7.json()) < len(r30.json())


# ===========================================================================
# Watchlist CRUD  (full round-trip in a single ordered test)
# ===========================================================================

class TestWatchlistCRUD:

    def test_list_initially_empty(self, client):
        r = client.get("/api/watchlist/")
        assert r.status_code == 200
        assert r.json() == []

    def test_add_ticker(self, client):
        r = client.post("/api/watchlist/", json={"ticker": "TSLA"})
        assert r.status_code == 200
        entry = r.json()
        assert entry["ticker"] == "TSLA"
        assert entry["status"] == "watching"
        assert isinstance(entry["id"], int)
        TestWatchlistCRUD._eid = entry["id"]

    def test_list_shows_entry(self, client):
        r = client.get("/api/watchlist/")
        assert r.status_code == 200
        tickers = [e["ticker"] for e in r.json()]
        assert "TSLA" in tickers

    def test_add_is_idempotent(self, client):
        # Re-adding the same ticker updates entry_price, does not duplicate
        r = client.post("/api/watchlist/", json={"ticker": "TSLA", "entry_price": 250.00})
        assert r.status_code == 200
        assert r.json()["entry_price"] == 250.00
        assert len(client.get("/api/watchlist/").json()) == 1

    def test_patch_status(self, client):
        r = client.patch(f"/api/watchlist/{self._eid}", json={"status": "entered"})
        assert r.status_code == 200
        assert r.json()["status"] == "entered"

    def test_patch_notes(self, client):
        r = client.patch(
            f"/api/watchlist/{self._eid}",
            json={"notes": "Strong print — watching for breakout"},
        )
        assert r.status_code == 200
        assert r.json()["notes"] == "Strong print — watching for breakout"

    def test_patch_rejects_invalid_status(self, client):
        r = client.patch(f"/api/watchlist/{self._eid}", json={"status": "yolo"})
        assert r.status_code == 422

    def test_patch_unknown_id_returns_404(self, client):
        r = client.patch("/api/watchlist/999999", json={"status": "closed"})
        assert r.status_code == 404

    def test_add_second_ticker_with_notes(self, client):
        r = client.post(
            "/api/watchlist/",
            json={"ticker": "NVDA", "entry_price": 880.0, "notes": "AI play"},
        )
        assert r.status_code == 200
        assert r.json()["notes"] == "AI play"
        TestWatchlistCRUD._eid2 = r.json()["id"]

    def test_list_returns_both(self, client):
        tickers = [e["ticker"] for e in client.get("/api/watchlist/").json()]
        assert "TSLA" in tickers
        assert "NVDA" in tickers

    def test_delete_first(self, client):
        r = client.delete(f"/api/watchlist/{self._eid}")
        assert r.status_code == 204

    def test_gone_after_delete(self, client):
        tickers = [e["ticker"] for e in client.get("/api/watchlist/").json()]
        assert "TSLA" not in tickers
        assert "NVDA" in tickers

    def test_delete_unknown_id_returns_404(self, client):
        r = client.delete(f"/api/watchlist/{self._eid}")
        assert r.status_code == 404

    def test_cleanup(self, client):
        r = client.delete(f"/api/watchlist/{self._eid2}")
        assert r.status_code == 204
        assert client.get("/api/watchlist/").json() == []


# ===========================================================================
# POST /api/ingest  (manual scan trigger)
# ===========================================================================

class TestManualIngest:

    def test_upload_returns_job_id_immediately(self, client):
        """POST /api/ingest/upload must return a job_id immediately."""
        with patch("routers.ingest._run_pipeline"):
            r = client.post(
                "/api/ingest/upload",
                files={"file": ("CNMSweekly20240108.txt", b"Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n", "text/plain")},
            )
        assert r.status_code == 200
        body = r.json()
        assert "job_id" in body
        assert body["status"] == "started"

    def test_upload_rejects_non_txt_file(self, client):
        """Only .txt files should be accepted."""
        r = client.post(
            "/api/ingest/upload",
            files={"file": ("data.csv", b"col1,col2\n", "text/csv")},
        )
        assert r.status_code == 400

    def test_fetch_returns_job_id_immediately(self, client):
        """POST /api/ingest/fetch must return a job_id without blocking."""
        with patch("routers.ingest._run_pipeline"), \
             patch("ingest.finra_download.get_latest_finra_file", return_value="/tmp/fake.txt"):
            r = client.post("/api/ingest/fetch")
        assert r.status_code == 200
        assert "job_id" in r.json()

    def test_status_404_for_unknown_job(self, client):
        """Polling an unknown job_id must return 404."""
        r = client.get("/api/ingest/status/nonexistent-job-id")
        assert r.status_code == 404

    def test_history_returns_list(self, client):
        """GET /api/ingest/history must return a list (possibly empty)."""
        r = client.get("/api/ingest/history")
        assert r.status_code == 200
        assert isinstance(r.json(), list)


# ===========================================================================
# Scheduler configuration
# ===========================================================================

class TestSchedulerConfig:

    def test_scheduler_is_running(self, client):
        """setup_scheduler() was called during FastAPI lifespan startup."""
        assert _sched._scheduler.running

    def test_weekly_pipeline_job_exists(self, client):
        job = _sched._scheduler.get_job("weekly_pipeline")
        assert job is not None
        assert job.next_run_time is not None

    def test_daily_price_refresh_job_exists(self, client):
        job = _sched._scheduler.get_job("daily_price_refresh")
        assert job is not None
        assert job.next_run_time is not None

    def test_weekly_job_fires_on_monday(self, client):
        import pytz
        job = _sched._scheduler.get_job("weekly_pipeline")
        next_run = job.next_run_time.astimezone(pytz.timezone("America/New_York"))
        assert next_run.weekday() == 0, (
            f"Expected Monday (0), got weekday {next_run.weekday()}"
        )

    def test_weekly_job_fires_at_0600_et(self, client):
        import pytz
        job = _sched._scheduler.get_job("weekly_pipeline")
        next_run = job.next_run_time.astimezone(pytz.timezone("America/New_York"))
        assert next_run.hour == 6
        assert next_run.minute == 0

    def test_daily_job_fires_on_weekday(self, client):
        import pytz
        job = _sched._scheduler.get_job("daily_price_refresh")
        next_run = job.next_run_time.astimezone(pytz.timezone("America/New_York"))
        assert next_run.weekday() < 5, (
            f"Expected Mon–Fri (0–4), got weekday {next_run.weekday()}"
        )

    def test_daily_job_fires_at_1630_et(self, client):
        import pytz
        job = _sched._scheduler.get_job("daily_price_refresh")
        next_run = job.next_run_time.astimezone(pytz.timezone("America/New_York"))
        assert next_run.hour == 16
        assert next_run.minute == 30

    def test_both_jobs_have_max_instances_one(self, client):
        """Prevent duplicate pipeline runs if a job is still executing."""
        for job_id in ("weekly_pipeline", "daily_price_refresh"):
            job = _sched._scheduler.get_job(job_id)
            assert job.max_instances == 1

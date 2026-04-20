# Dark Pool Trade Tracker

A weekly research tool that surfaces institutional accumulation setups using free FINRA OTC data.

**Thesis:** Large dark pool prints precede price moves by 5–10 days. The tool flags tickers with unusually high off-exchange volume that haven't yet broken out — a setup that historically precedes institutional-driven moves.

---

## Stack

| Layer     | Technology                                       |
|-----------|--------------------------------------------------|
| Backend   | Python 3.11+, FastAPI, SQLite, SQLAlchemy 2.0    |
| Scheduler | APScheduler 3.x — two cron jobs (see below)      |
| Alerts    | Discord webhook (optional)                       |
| Frontend  | React 18, Vite, TailwindCSS, Recharts            |

---

## Project Structure

```
darkpool-tracker/
├── backend/
│   ├── main.py             FastAPI app, CORS, scheduler wiring
│   ├── database.py         SQLAlchemy engine, session factory, db_init()
│   ├── models.py           DarkPoolPrint, PriceSnapshot, WatchlistEntry, Signal
│   ├── scheduler.py        APScheduler — two cron jobs
│   ├── requirements.txt
│   ├── .env.example        Template for environment variables
│   ├── alembic/            Schema migrations
│   ├── ingest/
│   │   ├── finra.py        FINRA weekly file download + upsert
│   │   └── price.py        yfinance OHLCV fetch + upsert
│   ├── signals/
│   │   └── scanner.py      Scoring engine + Discord webhook
│   ├── routers/
│   │   ├── tickers.py      GET /api/tickers/signals, /search, /{ticker}/history, /{ticker}/price
│   │   └── watchlist.py    GET/POST/PATCH/DELETE /api/watchlist/
│   └── tests/
│       ├── test_finra.py   38 tests
│       └── test_scanner.py 64 tests
└── frontend/
    ├── src/
    │   ├── App.jsx          Two-tab shell (Signals / Watchlist)
    │   ├── api/client.js    Typed fetch wrappers for all API endpoints
    │   └── components/
    │       ├── Dashboard.jsx    Header + Run Scan button + TickerTable
    │       ├── TickerTable.jsx  Sortable, filterable signal table
    │       ├── TickerDetail.jsx Slide-out panel with candlestick + DP volume charts
    │       ├── Watchlist.jsx    Full-page watchlist management table
    │       └── AlertBadge.jsx   HIGH / MED / LOW score badge
    └── package.json
```

---

## Setup

### 1. Backend

```bash
cd backend

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # macOS/Linux
.venv\Scripts\activate             # Windows

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env — set DISCORD_WEBHOOK_URL if you want alerts
```

### 2. Frontend

```bash
cd frontend
npm install
```

---

## Running

### Backend

```bash
cd backend
uvicorn main:app --reload
```

- API: `http://localhost:8000`
- Interactive docs: `http://localhost:8000/docs`

On first start the server detects an empty database and kicks off an initial ingest in the background. The UI will show data within a minute or two (depending on network speed and FINRA file size).

### Frontend

```bash
cd frontend
npm run dev
```

UI: `http://localhost:5173`

---

## Scheduled Jobs

Two APScheduler cron jobs run automatically while the backend is running:

| Job | Schedule | What it does |
|-----|----------|--------------|
| `weekly_pipeline` | **Monday 06:00 ET** | Downloads latest FINRA file → upserts dark pool prints → computes 4-week rolling averages → fetches 30-day OHLCV for top 500 tickers → runs signal scanner → fires Discord alert |
| `daily_price_refresh` | **Mon–Fri 16:30 ET** | Fetches 5 days of OHLCV for watchlist tickers only (keeps P&L current after market close) |

If the server was down at a scheduled fire time and restarts within the grace window (1 hour for the weekly job, 30 minutes for the daily job), the job runs immediately on startup.

---

## Loading FINRA Data

FINRA's CDN blocks automated downloads from non-residential IPs, so data is loaded manually.

### Option A — UI (recommended)

Open `http://localhost:5173` and use the **file drop zone** on the Signals tab:

- **Drop or browse** — drag a `CNMSweekly*.txt` file onto the zone
- **Download from FINRA →** button — auto-downloads via Playwright (requires setup below)

### Option B — curl

```bash
# Upload a file you downloaded manually
curl -X POST http://localhost:8000/api/ingest/upload \
  -F "file=@CNMSweekly20260314.txt"
# {"job_id": "...", "status": "started", "filename": "CNMSweekly20260314.txt"}

# Trigger Playwright auto-download (local machine only)
curl -X POST http://localhost:8000/api/ingest/fetch
# {"job_id": "...", "status": "started"}

# Poll progress
curl http://localhost:8000/api/ingest/status/{job_id}
# {"stage": "prices", "progress": 100, "done": false, "error": null}

# List ingested weeks
curl http://localhost:8000/api/ingest/history
```

The pipeline runs in a background thread (parse → prices → scan → alert) and returns immediately. Stages: `parsing → prices → scanning → alerting → complete`.

### Playwright setup (for auto-download button)

```bash
pip install playwright
python -m playwright install chromium
```

Download the FINRA file:
```
https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/weekly-short-sale-volume-data
```

---

## Environment Variables

Copy `backend/.env.example` to `backend/.env` and edit as needed.

| Variable              | Default                                    | Description                                    |
|-----------------------|--------------------------------------------|------------------------------------------------|
| `DATABASE_URL`        | `sqlite:///./darkpool.db`                  | SQLAlchemy connection string                   |
| `DISCORD_WEBHOOK_URL` | _(empty)_                                  | Discord incoming webhook; leave blank to disable |
| `CORS_ORIGINS`        | `http://localhost:3000,http://localhost:5173` | Comma-separated allowed frontend origins      |
| `SQL_ECHO`            | `0`                                        | Set to `1` to log all SQL to stdout            |
| `VITE_API_URL`        | `http://localhost:8000`                    | Backend base URL (set in `frontend/.env`)      |

---

## Data Source — FINRA TRF

The signal data comes from FINRA's free weekly **Consolidated NMS (CNMS) Short Sale Volume** files.

**Why TRF rows?**
Each row in the FINRA file is broken out by market (`NYSE`, `NASDAQ`, `TRF`, etc.). Rows where `Market == TRF` (Trade Reporting Facility) represent trades reported off-exchange — the best public proxy for dark pool activity. All other rows represent lit-exchange volume.

**What gets computed per ticker per week:**

| Field | Formula |
|-------|---------|
| `dp_volume` | Sum of `TotalVolume` across all TRF rows |
| `total_volume` | Sum of `TotalVolume` across all markets |
| `dp_pct` | `dp_volume / total_volume × 100` |
| `dp_volume_4wk_avg` | Rolling 4-week mean of `dp_volume` (requires 2+ prior weeks) |
| `volume_spike_ratio` | `dp_volume / dp_volume_4wk_avg` |

**Signal scoring (0–100):**

| Condition | Points |
|-----------|--------|
| `dp_pct > 40%` | +20 |
| `dp_pct > 55%` (bonus) | +15 |
| `spike_ratio > 1.5×` | +20 |
| `spike_ratio > 2.5×` (bonus) | +15 |
| Price flat (< 3% move over 10 days) | +30 |

Levels: **HIGH** ≥ 75 · **MED** ≥ 50 · signals below 50 are not persisted.

---

## Running Tests

```bash
cd backend
pytest
```

102 tests — covers FINRA ingest parsing, 4-week rolling average calculation, signal scoring logic, and Discord alert formatting.

---

## Discord Setup (optional)

1. Open your Discord server → **Server Settings** → **Integrations** → **Webhooks** → **New Webhook**
2. Choose a channel, copy the webhook URL
3. Add it to `backend/.env`:
   ```
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
   ```

Alerts fire automatically after each Monday scan for signals that score ≥ 75 (HIGH). Each alert includes the ticker, score, DP%, spike ratio, and price close.

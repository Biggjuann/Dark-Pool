"""
FINRA REGSHODAILY API downloader.

Fetches weekly short-sale volume data from FINRA's Query API
(developer.finra.org) using the OTCMARKET/REGSHODAILY dataset, then
writes a CNMS-compatible pipe-delimited .txt file for the standard
ingest pipeline.

Authentication
--------------
OAuth 2.0 client credentials flow.  Set two environment variables:

    FINRA_CLIENT_ID      API client ID from developer.finra.org
    FINRA_CLIENT_SECRET  API client secret

The backend loads these automatically from backend/.env via python-dotenv.
For CLI use this script loads .env from the current directory.

Data source
-----------
REGSHODAILY provides daily per-ticker short-sale volume for every
FINRA-registered trade reporting facility:

    NCTRF  NYSE Chicago TRF
    NQTRF  NASDAQ TRF
    NYTRF  NYSE TRF
    ORF    OTC Reporting Facility (ADF)

TRF rows (NCTRF + NQTRF + NYTRF) are aggregated as dp_volume.
ORF rows are written as Market=NASD so the ingest pipeline can include
them in the total_volume denominator for OTC/pink-sheet stocks.

Note: exchange-listed stocks (e.g. AAPL) have no ORF row, so their
dp_pct will be 100 %.  The spike-ratio signal is unaffected.

Public API
----------
    download_latest_finra_file() -> Path   fetch & save CNMS-format file
    get_latest_finra_file()      -> Path   alias used by routers/ingest.py

CLI
---
    python -m ingest.finra_download
"""

from __future__ import annotations

import base64
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

_TOKEN_URL   = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token"
_API_BASE    = "https://api.finra.org"
_UPLOADS_DIR = Path("data/uploads")

_TRF_CODES = frozenset({"NCTRF", "NQTRF", "NYTRF"})
_ORF_CODES = frozenset({"ORF"})
_PAGE_SIZE  = 5000

_FIELDS = [
    "securitiesInformationProcessorSymbolIdentifier",
    "totalParQuantity",
    "shortParQuantity",
    "shortExemptParQuantity",
    "reportingFacilityCode",
    "tradeReportDate",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_env() -> None:
    """Load .env from the current directory (CLI use only)."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


def _get_token() -> str:
    """Exchange client credentials for a Bearer token."""
    client_id     = os.environ["FINRA_CLIENT_ID"]
    client_secret = os.environ["FINRA_CLIENT_SECRET"]
    creds = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    with httpx.Client(timeout=30) as client:
        resp = client.post(
            f"{_TOKEN_URL}?grant_type=client_credentials",
            headers={"Authorization": f"Basic {creds}"},
        )
    resp.raise_for_status()

    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(
            f"No access_token in FINRA auth response: {resp.text[:200]}"
        )
    return token


def _latest_friday() -> date:
    """Return the most recent Friday (end of the latest complete trading week)."""
    d = date.today()
    days_since_friday = (d.weekday() - 4) % 7   # 0 if today is Friday
    return d - timedelta(days=days_since_friday)


def _fetch_pages(token: str, start_date: str, end_date: str) -> list[dict]:
    """
    Paginate through OTCMARKET/REGSHODAILY for the given date range.

    Uses POST with a JSON body — the GET dateRangeFilters query param is
    silently ignored by the FINRA API; only the POST body filter works.

    Returns all records as a flat list of dicts.
    """
    all_records: list[dict] = []
    offset = 0
    total: int | None = None

    with httpx.Client(timeout=90) as client:
        while True:
            body = {
                "limit":  _PAGE_SIZE,
                "offset": offset,
                "dateRangeFilters": [
                    {
                        "fieldName": "tradeReportDate",
                        "startDate": start_date,
                        "endDate":   end_date,
                    }
                ],
                "fields": _FIELDS,
            }
            resp = client.post(
                f"{_API_BASE}/data/group/OTCMARKET/name/REGSHODAILY",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept":        "application/json",
                    "Content-Type":  "application/json",
                },
                json=body,
            )
            resp.raise_for_status()

            if total is None:
                total = int(resp.headers.get("record-total", 0))

            page = resp.json()

            if not page:
                break

            all_records.extend(page)
            logger.info(
                "REGSHODAILY offset=%d: %d records (total so far: %d / %s)",
                offset, len(page), len(all_records),
                total if total else "?",
            )

            if len(page) < _PAGE_SIZE:
                break   # last (partial) page

            offset += _PAGE_SIZE

    return all_records


# ---------------------------------------------------------------------------
# Public: download
# ---------------------------------------------------------------------------

def download_latest_finra_file() -> Path:
    """
    Fetch the latest week's REGSHODAILY data from FINRA's API and save it
    as a CNMS-compatible pipe-delimited .txt file.

    The file is written to ``data/uploads/CNMSweekly{YYYYMMDD}.txt`` where
    the date is the Friday (week-ending) of the most recent complete week.

    Returns
    -------
    pathlib.Path
        Absolute path to the saved file.

    Raises
    ------
    KeyError
        If FINRA_CLIENT_ID or FINRA_CLIENT_SECRET is not set.
    RuntimeError
        If auth fails or the API returns no data for the current week.
    """
    token = _get_token()

    week_end   = _latest_friday()
    week_start = week_end - timedelta(days=4)   # Monday
    start_str  = week_start.isoformat()
    end_str    = week_end.isoformat()

    logger.info("Fetching REGSHODAILY %s → %s", start_str, end_str)
    records = _fetch_pages(token, start_str, end_str)

    if not records:
        raise RuntimeError(
            f"No REGSHODAILY records found for week {start_str} to {end_str}."
        )
    logger.info("Fetched %d daily records for the week", len(records))

    # ---- Build DataFrame ----
    df = pd.DataFrame(records).rename(columns={
        "securitiesInformationProcessorSymbolIdentifier": "Symbol",
        "totalParQuantity":       "TotalVolume",
        "shortParQuantity":       "ShortVolume",
        "shortExemptParQuantity": "ShortExemptVolume",
        "reportingFacilityCode":  "FacilityCode",
        "tradeReportDate":        "Date",
    })

    # Derive week_ending from the latest date actually present in the data
    week_ending = pd.to_datetime(df["Date"]).max().date()
    date_str    = week_ending.strftime("%Y%m%d")

    # ---- Aggregate daily volumes per ticker per day ----
    trf_df = df[df["FacilityCode"].isin(_TRF_CODES)]
    orf_df = df[df["FacilityCode"].isin(_ORF_CODES)]

    trf_agg = (
        trf_df.groupby(["Symbol", "Date"], as_index=False)
        .agg(
            TotalVolume       = ("TotalVolume",       "sum"),
            ShortVolume       = ("ShortVolume",       "sum"),
            ShortExemptVolume = ("ShortExemptVolume", "sum"),
        )
    )

    if trf_agg.empty:
        raise RuntimeError(
            "No TRF records found in REGSHODAILY data for "
            f"{start_str} to {end_str}."
        )

    orf_agg = (
        orf_df.groupby(["Symbol", "Date"], as_index=False)
        .agg(orf_volume=("TotalVolume", "sum"))
        if not orf_df.empty
        else pd.DataFrame(columns=["Symbol", "Date", "orf_volume"])
    )

    merged = trf_agg.merge(orf_agg, on=["Symbol", "Date"], how="left")
    merged["orf_volume"] = merged["orf_volume"].fillna(0).astype(int)

    # ---- Write CNMS-format pipe-delimited file (one row per ticker per day) ----
    lines = ["Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market"]

    for _, row in merged.iterrows():
        sym           = str(row["Symbol"])
        # API date is "YYYY-MM-DD"; CNMS format wants "YYYYMMDD"
        day_str       = str(row["Date"]).replace("-", "")
        trf_vol       = int(row["TotalVolume"])
        short_v       = int(row["ShortVolume"])
        exempt_v      = int(row["ShortExemptVolume"])
        orf_vol       = int(row["orf_volume"])

        # TRF row — dark pool volume for this ticker on this day
        lines.append(f"{day_str}|{sym}|{short_v}|{exempt_v}|{trf_vol}|TRF")

        # ORF row — OTC/pink-sheet total_volume denominator
        if orf_vol > 0:
            lines.append(f"{day_str}|{sym}|0|0|{orf_vol}|NASD")

    _UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"CNMSweekly{date_str}.txt"
    dest     = _UPLOADS_DIR / filename
    dest.write_text("\n".join(lines), encoding="utf-8")

    ticker_count = merged["Symbol"].nunique()
    logger.info(
        "Saved %s: %d tickers, %d daily rows, %d file rows",
        dest, ticker_count, len(merged), len(lines) - 1,
    )
    return dest.resolve()


# Alias expected by routers/ingest.py
get_latest_finra_file = download_latest_finra_file


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    _load_env()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    print("Fetching latest FINRA weekly data via API…")
    try:
        filepath = download_latest_finra_file()
    except KeyError as exc:
        print(
            f"Missing credential: {exc}\n"
            "Set FINRA_CLIENT_ID and FINRA_CLIENT_SECRET in backend/.env",
            file=sys.stderr,
        )
        sys.exit(1)
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Downloaded: {filepath}")
    print("Triggering ingest pipeline…")

    try:
        with open(filepath, "rb") as fh:
            resp = httpx.post(
                "http://localhost:8000/api/ingest/upload",
                files={"file": (filepath.name, fh, "text/plain")},
                timeout=30,
            )
        data = resp.json()
    except Exception as exc:
        print(f"Failed to call /api/ingest/upload: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Pipeline started — job_id: {data.get('job_id', '?')}")

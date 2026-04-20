"""
Unit tests for backend/ingest/finra.py (file-based ingest workflow).

Coverage
--------
- parse_finra_file()        — TRF filter, symbol cleaning, column validation
- ingest_from_file()        — dp_pct math, upsert, idempotency, week_ending derivation
- get_ingested_weeks()      — DB query, ordering
- calculate_4wk_averages()  — rolling average math, edge cases
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from ingest.finra import (
    _MIN_HISTORY_WEEKS,
    _ROLLING_WINDOW,
    calculate_4wk_averages,
    get_ingested_weeks,
    ingest_from_file,
    parse_finra_file,
)
from models import DarkPoolPrint


# ---------------------------------------------------------------------------
# Shared sample data
# ---------------------------------------------------------------------------

# Pipe-delimited content matching the real FINRA CNMS weekly file format.
# Includes:
#   - AAPL on NYSE and TRF
#   - NVDA on NASDAQ and TRF
#   - MSFT on NYSE only (no TRF → excluded from dark pool aggregation)
#   - TOTAL row that must be filtered out by symbol validation
SAMPLE_PIPE_CONTENT = (
    "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
    "20240108|AAPL|1000000|50000|5000000|NYSE\n"
    "20240108|AAPL|2000000|80000|3000000|TRF\n"
    "20240108|NVDA|500000|20000|2000000|NASDAQ\n"
    "20240108|NVDA|800000|30000|1500000|TRF\n"
    "20240108|MSFT|300000|10000|1000000|NYSE\n"
    "20240108|TOTAL|999999|0|999999|ALL\n"   # summary row — must be dropped
)

SAMPLE_WEEK_ENDING = date(2024, 1, 8)


def _write_sample(tmp_path: Path, content: str = SAMPLE_PIPE_CONTENT) -> Path:
    """Write *content* to a temp .txt file and return its path."""
    f = tmp_path / "CNMSweekly20240108.txt"
    f.write_text(content, encoding="utf-8")
    return f


# ===========================================================================
# parse_finra_file
# ===========================================================================

class TestParseFinraFile:
    def test_returns_only_trf_rows(self, tmp_path):
        fp = _write_sample(tmp_path)
        df = parse_finra_file(str(fp))
        assert (df["Market"].str.strip().str.upper() == "TRF").all()

    def test_correct_tickers_in_trf(self, tmp_path):
        fp = _write_sample(tmp_path)
        df = parse_finra_file(str(fp))
        assert set(df["Symbol"]) == {"AAPL", "NVDA"}

    def test_msft_excluded_no_trf_row(self, tmp_path):
        fp = _write_sample(tmp_path)
        df = parse_finra_file(str(fp))
        assert "MSFT" not in df["Symbol"].values

    def test_total_summary_row_excluded(self, tmp_path):
        fp = _write_sample(tmp_path)
        df = parse_finra_file(str(fp))
        assert "TOTAL" not in df["Symbol"].values

    def test_returns_expected_columns(self, tmp_path):
        fp = _write_sample(tmp_path)
        df = parse_finra_file(str(fp))
        for col in ("Date", "Symbol", "ShortVolume", "TotalVolume", "Market"):
            assert col in df.columns

    def test_filters_numeric_symbol(self, tmp_path):
        content = (
            "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20240108|AAPL|1000000|50000|5000000|TRF\n"
            "20240108|AAP1|100000|5000|500000|TRF\n"   # digit — should be dropped
        )
        fp = _write_sample(tmp_path, content)
        df = parse_finra_file(str(fp))
        assert "AAPL" in df["Symbol"].values
        assert "AAP1" not in df["Symbol"].values

    def test_drops_zero_total_volume_rows(self, tmp_path):
        content = (
            "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20240108|AAPL|1000000|50000|5000000|TRF\n"
            "20240108|ZERO|0|0|0|TRF\n"
        )
        fp = _write_sample(tmp_path, content)
        df = parse_finra_file(str(fp))
        assert "ZERO" not in df["Symbol"].values

    def test_raises_on_missing_pipe_delimiter(self, tmp_path):
        fp = tmp_path / "bad.txt"
        fp.write_text("<html>not a finra file</html>")
        with pytest.raises(ValueError, match="pipe-delimited"):
            parse_finra_file(str(fp))

    def test_raises_on_missing_columns(self, tmp_path):
        fp = tmp_path / "bad.txt"
        fp.write_text("Ticker|Volume|TradeDate\nAAPL|100000|20240108\n")
        with pytest.raises(ValueError, match="missing expected columns"):
            parse_finra_file(str(fp))

    def test_raises_on_empty_file(self, tmp_path):
        fp = tmp_path / "empty.txt"
        fp.write_text("")
        with pytest.raises(ValueError):
            parse_finra_file(str(fp))

    def test_raises_on_unreadable_path(self):
        with pytest.raises(ValueError, match="Cannot read"):
            parse_finra_file("/nonexistent/path/file.txt")

    def test_empty_trf_returns_empty_dataframe(self, tmp_path):
        # File with no TRF rows at all
        content = (
            "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20240108|AAPL|1000000|50000|5000000|NYSE\n"
            "20240108|MSFT|300000|10000|1000000|NASDAQ\n"
        )
        fp = _write_sample(tmp_path, content)
        df = parse_finra_file(str(fp))
        assert df.empty


# ===========================================================================
# ingest_from_file  (DB integration)
# ===========================================================================

class TestIngestFromFile:
    def test_inserts_expected_rows(self, tmp_path, db_session):
        """AAPL and NVDA have TRF rows; MSFT does not — only 2 rows expected."""
        fp = _write_sample(tmp_path)
        result = ingest_from_file(str(fp), db_session)

        assert result["tickers_processed"] == 2
        rows = db_session.query(DarkPoolPrint).all()
        assert len(rows) == 2
        assert {r.ticker for r in rows} == {"AAPL", "NVDA"}

    def test_dp_pct_calculated_correctly(self, tmp_path, db_session):
        """
        AAPL: TRF TotalVolume=3M, all-market TotalVolume=8M → dp_pct=37.5
        NVDA: TRF TotalVolume=1.5M, all-market=3.5M → dp_pct≈42.857
        """
        fp = _write_sample(tmp_path)
        ingest_from_file(str(fp), db_session)

        aapl = db_session.query(DarkPoolPrint).filter_by(ticker="AAPL").first()
        nvda = db_session.query(DarkPoolPrint).filter_by(ticker="NVDA").first()

        assert round(aapl.dp_pct, 4) == round(3_000_000 / 8_000_000 * 100, 4)
        assert round(nvda.dp_pct, 4) == round(1_500_000 / 3_500_000 * 100, 4)

    def test_dp_volume_stored_correctly(self, tmp_path, db_session):
        fp = _write_sample(tmp_path)
        ingest_from_file(str(fp), db_session)

        aapl = db_session.query(DarkPoolPrint).filter_by(ticker="AAPL").first()
        assert aapl.dp_volume    == 3_000_000
        assert aapl.total_volume == 8_000_000

    def test_week_ending_derived_from_date_column(self, tmp_path, db_session):
        fp = _write_sample(tmp_path)
        result = ingest_from_file(str(fp), db_session)

        assert result["week_ending"] == SAMPLE_WEEK_ENDING
        rows = db_session.query(DarkPoolPrint).all()
        assert all(r.week_ending == SAMPLE_WEEK_ENDING for r in rows)

    def test_returns_correct_result_dict(self, tmp_path, db_session):
        fp = _write_sample(tmp_path)
        result = ingest_from_file(str(fp), db_session)

        assert "tickers_processed" in result
        assert "week_ending" in result
        assert "rows_ingested" in result
        assert result["tickers_processed"] == 2
        assert result["rows_ingested"] == 2

    def test_idempotent_on_rerun(self, tmp_path, db_session):
        """Running ingest twice for the same week must not duplicate rows."""
        fp = _write_sample(tmp_path)
        ingest_from_file(str(fp), db_session)
        ingest_from_file(str(fp), db_session)

        assert db_session.query(DarkPoolPrint).count() == 2

    def test_rerun_updates_values(self, tmp_path, db_session):
        """A second ingest with different volumes should overwrite, not duplicate."""
        fp = _write_sample(tmp_path)
        ingest_from_file(str(fp), db_session)

        # New file: AAPL TRF TotalVolume doubled (3M → 6M), NVDA unchanged
        updated = (
            "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20240108|AAPL|1000000|50000|5000000|NYSE\n"
            "20240108|AAPL|4000000|80000|6000000|TRF\n"  # doubled
            "20240108|NVDA|500000|20000|2000000|NASDAQ\n"
            "20240108|NVDA|800000|30000|1500000|TRF\n"
        )
        fp2 = tmp_path / "updated.txt"
        fp2.write_text(updated)
        ingest_from_file(str(fp2), db_session)

        assert db_session.query(DarkPoolPrint).count() == 2
        aapl = db_session.query(DarkPoolPrint).filter_by(ticker="AAPL").first()
        assert aapl.dp_volume == 6_000_000

    def test_raises_when_no_trf_rows(self, tmp_path, db_session):
        content = (
            "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20240108|AAPL|1000000|50000|5000000|NYSE\n"
            "20240108|MSFT|300000|10000|1000000|NASDAQ\n"
        )
        fp = _write_sample(tmp_path, content)
        with pytest.raises(ValueError, match="No TRF rows"):
            ingest_from_file(str(fp), db_session)

    def test_dp_trade_count_defaults_to_zero(self, tmp_path, db_session):
        """The CNMS weekly file has no trade count field; must be stored as 0."""
        fp = _write_sample(tmp_path)
        ingest_from_file(str(fp), db_session)

        for row in db_session.query(DarkPoolPrint).all():
            assert row.dp_trade_count == 0

    def test_multiple_weeks_stored_separately(self, tmp_path, db_session):
        """Ingesting two different week files should yield separate rows."""
        w1 = (
            "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20240108|AAPL|1000000|50000|5000000|NYSE\n"
            "20240108|AAPL|2000000|80000|3000000|TRF\n"
        )
        w2 = (
            "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20240115|AAPL|1500000|50000|6000000|NYSE\n"
            "20240115|AAPL|2500000|80000|4000000|TRF\n"
        )
        f1 = tmp_path / "week1.txt"
        f2 = tmp_path / "week2.txt"
        f1.write_text(w1)
        f2.write_text(w2)

        ingest_from_file(str(f1), db_session)
        ingest_from_file(str(f2), db_session)

        assert db_session.query(DarkPoolPrint).count() == 2


# ===========================================================================
# get_ingested_weeks  (DB integration)
# ===========================================================================

class TestGetIngestedWeeks:
    def _seed(self, db_session, ticker: str, week: date, dp_vol: int) -> None:
        db_session.add(DarkPoolPrint(
            ticker=ticker, week_ending=week,
            dp_volume=dp_vol, dp_trade_count=0,
            total_volume=dp_vol * 2, dp_pct=50.0,
        ))
        db_session.flush()

    def test_returns_empty_list_when_no_data(self, db_session):
        assert get_ingested_weeks(db_session) == []

    def test_returns_weeks_newest_first(self, db_session):
        w1 = date(2024, 1, 5)
        w2 = date(2024, 1, 12)
        w3 = date(2024, 1, 19)
        for w in [w1, w2, w3]:
            self._seed(db_session, "AAPL", w, 1_000_000)

        weeks = get_ingested_weeks(db_session)
        dates = [r["week_ending"] for r in weeks]
        assert dates == [w3, w2, w1]

    def test_tickers_processed_count_is_correct(self, db_session):
        w = date(2024, 1, 8)
        for ticker in ("AAPL", "NVDA", "MSFT"):
            self._seed(db_session, ticker, w, 1_000_000)

        weeks = get_ingested_weeks(db_session)
        assert len(weeks) == 1
        assert weeks[0]["tickers_processed"] == 3

    def test_each_week_counts_independently(self, db_session):
        w1 = date(2024, 1, 5)
        w2 = date(2024, 1, 12)
        for ticker in ("AAPL", "NVDA"):
            self._seed(db_session, ticker, w1, 1_000_000)
        self._seed(db_session, "AAPL", w2, 2_000_000)

        weeks = get_ingested_weeks(db_session)
        by_week = {r["week_ending"]: r["tickers_processed"] for r in weeks}
        assert by_week[w1] == 2
        assert by_week[w2] == 1


# ===========================================================================
# calculate_4wk_averages  (DB integration)
# ===========================================================================

def _seed_dp_prints(db_session, ticker: str, weeks: list[tuple[date, int]]) -> None:
    """Insert DarkPoolPrint rows for testing. (week_ending, dp_volume) pairs."""
    for week_ending, dp_vol in weeks:
        db_session.add(DarkPoolPrint(
            ticker         = ticker,
            week_ending    = week_ending,
            dp_volume      = dp_vol,
            dp_trade_count = 0,
            total_volume   = dp_vol * 2,
            dp_pct         = 50.0,
        ))
    db_session.flush()


class TestCalculate4wkAverages:

    W1 = date(2024, 1, 5)
    W2 = date(2024, 1, 12)
    W3 = date(2024, 1, 19)
    W4 = date(2024, 1, 26)
    W5 = date(2024, 2, 2)   # "current" week

    def test_computes_average_of_prior_four_weeks(self, db_session):
        volumes = [1_000_000, 1_200_000, 900_000, 1_100_000, 2_500_000]
        _seed_dp_prints(db_session, "NVDA", list(zip(
            [self.W1, self.W2, self.W3, self.W4, self.W5], volumes
        )))

        calculate_4wk_averages(db_session, self.W5)

        current = db_session.query(DarkPoolPrint).filter_by(
            ticker="NVDA", week_ending=self.W5
        ).first()

        expected_avg = (1_100_000 + 900_000 + 1_200_000 + 1_000_000) / 4
        assert current.dp_volume_4wk_avg == pytest.approx(expected_avg, rel=1e-4)

    def test_computes_spike_ratio_correctly(self, db_session):
        volumes = [1_000_000, 1_200_000, 900_000, 1_100_000, 2_500_000]
        _seed_dp_prints(db_session, "NVDA", list(zip(
            [self.W1, self.W2, self.W3, self.W4, self.W5], volumes
        )))

        calculate_4wk_averages(db_session, self.W5)

        current = db_session.query(DarkPoolPrint).filter_by(
            ticker="NVDA", week_ending=self.W5
        ).first()

        expected_avg   = (1_100_000 + 900_000 + 1_200_000 + 1_000_000) / 4
        expected_ratio = 2_500_000 / expected_avg
        assert current.volume_spike_ratio == pytest.approx(expected_ratio, rel=1e-4)

    def test_uses_only_rolling_window_most_recent_prior_weeks(self, db_session):
        """With 6 prior weeks, only the 4 most recent should count."""
        W0  = self.W1 - timedelta(weeks=1)
        Wm1 = W0  - timedelta(weeks=1)
        vols = [
            (Wm1, 9_999_999),
            (W0,  9_999_999),
            (self.W1, 1_000_000),
            (self.W2, 1_200_000),
            (self.W3, 900_000),
            (self.W4, 1_100_000),
            (self.W5, 2_500_000),
        ]
        _seed_dp_prints(db_session, "NVDA", vols)

        calculate_4wk_averages(db_session, self.W5)

        current = db_session.query(DarkPoolPrint).filter_by(
            ticker="NVDA", week_ending=self.W5
        ).first()

        expected_avg = (1_100_000 + 900_000 + 1_200_000 + 1_000_000) / 4
        assert current.dp_volume_4wk_avg == pytest.approx(expected_avg, rel=1e-4)

    def test_leaves_null_when_insufficient_history(self, db_session):
        """Only 1 prior week — below MIN_HISTORY_WEEKS — avg must stay NULL."""
        assert _MIN_HISTORY_WEEKS >= 2
        _seed_dp_prints(db_session, "NEWT", [
            (self.W4, 500_000),
            (self.W5, 1_500_000),
        ])

        calculate_4wk_averages(db_session, self.W5)

        current = db_session.query(DarkPoolPrint).filter_by(
            ticker="NEWT", week_ending=self.W5
        ).first()
        assert current.dp_volume_4wk_avg  is None
        assert current.volume_spike_ratio is None

    def test_null_spike_ratio_when_prior_average_is_zero(self, db_session):
        """If all prior volumes are 0, spike ratio must be NULL (not inf)."""
        _seed_dp_prints(db_session, "ZERO", [
            (self.W2, 0),
            (self.W3, 0),
            (self.W4, 0),
            (self.W5, 1_000_000),
        ])

        calculate_4wk_averages(db_session, self.W5)

        current = db_session.query(DarkPoolPrint).filter_by(
            ticker="ZERO", week_ending=self.W5
        ).first()
        assert current.dp_volume_4wk_avg  == pytest.approx(0.0)
        assert current.volume_spike_ratio is None

    def test_does_not_include_current_week_in_average(self, db_session):
        _seed_dp_prints(db_session, "SELF", [
            (self.W2, 1_000_000),
            (self.W3, 1_000_000),
            (self.W4, 1_000_000),
            (self.W5, 10_000_000),
        ])

        calculate_4wk_averages(db_session, self.W5)

        current = db_session.query(DarkPoolPrint).filter_by(
            ticker="SELF", week_ending=self.W5
        ).first()

        assert current.dp_volume_4wk_avg  == pytest.approx(1_000_000, rel=1e-4)
        assert current.volume_spike_ratio == pytest.approx(10.0, rel=1e-4)

    def test_handles_multiple_tickers_independently(self, db_session):
        _seed_dp_prints(db_session, "AAA", [
            (self.W2, 100_000), (self.W3, 100_000), (self.W4, 100_000),
            (self.W5, 300_000),
        ])
        _seed_dp_prints(db_session, "BBB", [
            (self.W2, 1_000_000), (self.W3, 1_000_000), (self.W4, 1_000_000),
            (self.W5, 3_000_000),
        ])

        calculate_4wk_averages(db_session, self.W5)

        aaa = db_session.query(DarkPoolPrint).filter_by(ticker="AAA", week_ending=self.W5).first()
        bbb = db_session.query(DarkPoolPrint).filter_by(ticker="BBB", week_ending=self.W5).first()

        assert aaa.dp_volume_4wk_avg  == pytest.approx(100_000)
        assert bbb.dp_volume_4wk_avg  == pytest.approx(1_000_000)
        assert aaa.volume_spike_ratio == pytest.approx(3.0, rel=1e-4)
        assert bbb.volume_spike_ratio == pytest.approx(3.0, rel=1e-4)

    def test_no_rows_for_week_does_not_raise(self, db_session):
        calculate_4wk_averages(db_session, self.W5)   # must not raise

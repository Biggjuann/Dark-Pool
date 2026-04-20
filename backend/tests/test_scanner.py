"""
Unit and integration tests for backend/signals/scanner.py.

Test classes
------------
TestIsFlatFromCloses   — pure function, no DB
TestScoreDpPrint       — pure function, no DB
TestScoreToLevel       — pure function, no DB
TestScoreTicker        — DB integration (single-ticker public API)
TestRunWeeklyScan      — DB integration (full scan pipeline)
TestGetTopSignals      — DB integration (query + join)
TestSendDiscordAlert   — mocked HTTP, optionally verifies DB update
"""

from __future__ import annotations

from datetime import date, timedelta, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from models import DarkPoolPrint, PriceSnapshot, Signal
from signals.scanner import (
    MIN_SCORE_THRESHOLD,
    SIGNAL_TYPE,
    _PTS_DP_PCT_BASE,
    _PTS_DP_PCT_BONUS,
    _PTS_PRICE_FLAT,
    _PTS_SPIKE_BASE,
    _PTS_SPIKE_BONUS,
    _is_flat_from_closes,
    _score_dp_print,
    _score_to_level,
    get_top_signals,
    run_weekly_scan,
    score_ticker,
    send_discord_alert,
)


# ---------------------------------------------------------------------------
# Shared dates
# ---------------------------------------------------------------------------

WEEK = date(2024, 1, 8)


# ---------------------------------------------------------------------------
# Fixtures / seed helpers
# ---------------------------------------------------------------------------

def _make_dp(
    ticker: str,
    week_ending: date = WEEK,
    dp_pct: float = 30.0,
    dp_volume: int = 1_000_000,
    total_volume: int = 3_000_000,
    volume_spike_ratio: float | None = None,
) -> DarkPoolPrint:
    return DarkPoolPrint(
        ticker             = ticker,
        week_ending        = week_ending,
        dp_volume          = dp_volume,
        dp_trade_count     = 0,
        total_volume       = total_volume,
        dp_pct             = dp_pct,
        dp_volume_4wk_avg  = (dp_volume / volume_spike_ratio) if volume_spike_ratio else None,
        volume_spike_ratio = volume_spike_ratio,
    )


def _add_dp(db_session, **kwargs) -> DarkPoolPrint:
    obj = _make_dp(**kwargs)
    db_session.add(obj)
    db_session.flush()
    return obj


def _add_prices(db_session, ticker: str, closes: list[float], base_date: date | None = None) -> None:
    """Add consecutive daily PriceSnapshot rows ending at base_date (defaults to today)."""
    if base_date is None:
        base_date = datetime.today().date()
    for i, close in enumerate(closes):
        db_session.add(PriceSnapshot(
            ticker        = ticker,
            snapshot_date = base_date - timedelta(days=len(closes) - 1 - i),
            open          = close * 0.99,
            high          = close * 1.01,
            low           = close * 0.98,
            close         = close,
            volume        = 1_000_000,
        ))
    db_session.flush()


# ===========================================================================
# _is_flat_from_closes
# ===========================================================================

class TestIsFlatFromCloses:
    def test_flat_within_threshold(self):
        closes = [100.0, 100.5, 101.0, 100.8, 100.2]
        assert _is_flat_from_closes(closes) is True

    def test_not_flat_exceeds_threshold(self):
        closes = [100.0, 103.5]   # 3.5% range > 3.0% default
        assert _is_flat_from_closes(closes) is False

    def test_exactly_at_threshold_is_not_flat(self):
        # 3.0% range is NOT flat (condition is strictly less-than)
        closes = [100.0, 103.0]
        assert _is_flat_from_closes(closes) is False

    def test_just_below_threshold_is_flat(self):
        closes = [100.0, 102.99]
        assert _is_flat_from_closes(closes) is True

    def test_single_value_returns_false(self):
        assert _is_flat_from_closes([100.0]) is False

    def test_empty_list_returns_false(self):
        assert _is_flat_from_closes([]) is False

    def test_zero_min_price_returns_false(self):
        assert _is_flat_from_closes([0.0, 1.0]) is False

    def test_custom_threshold(self):
        closes = [100.0, 104.0]  # 4% range
        assert _is_flat_from_closes(closes, threshold_pct=5.0) is True
        assert _is_flat_from_closes(closes, threshold_pct=3.0) is False

    def test_decreasing_prices_detected(self):
        closes = [105.0, 104.0, 103.0, 102.0]  # 2.8% range → flat
        assert _is_flat_from_closes(closes) is True

    def test_large_drop_is_not_flat(self):
        closes = [100.0, 90.0]   # 11% range
        assert _is_flat_from_closes(closes) is False


# ===========================================================================
# _score_to_level
# ===========================================================================

class TestScoreToLevel:
    def test_high_at_75(self):
        assert _score_to_level(75) == "high"

    def test_high_at_100(self):
        assert _score_to_level(100) == "high"

    def test_medium_at_50(self):
        assert _score_to_level(50) == "medium"

    def test_medium_at_74(self):
        assert _score_to_level(74) == "medium"

    def test_low_at_49(self):
        assert _score_to_level(49) == "low"

    def test_low_at_zero(self):
        assert _score_to_level(0) == "low"


# ===========================================================================
# _score_dp_print
# ===========================================================================

class TestScoreDpPrint:
    """All tests use _make_dp() and plain close lists — zero DB involvement."""

    def test_perfect_score_100(self):
        dp = _make_dp("NVDA", dp_pct=60.0, volume_spike_ratio=3.0)
        flat_closes = [100.0, 100.5, 101.0]
        result = _score_dp_print(dp, flat_closes)

        assert result["score"] == 100
        assert result["level"] == "high"
        assert result["breakdown"]["dp_pct_base"]  == _PTS_DP_PCT_BASE
        assert result["breakdown"]["dp_pct_bonus"] == _PTS_DP_PCT_BONUS
        assert result["breakdown"]["spike_base"]   == _PTS_SPIKE_BASE
        assert result["breakdown"]["spike_bonus"]  == _PTS_SPIKE_BONUS
        assert result["breakdown"]["price_flat"]   == _PTS_PRICE_FLAT

    def test_dp_pct_base_only(self):
        # dp_pct=45% (above 40%, below 55%), no spike, no flat
        dp = _make_dp("X", dp_pct=45.0, volume_spike_ratio=None)
        result = _score_dp_print(dp, [])

        assert result["score"] == _PTS_DP_PCT_BASE
        assert result["breakdown"]["dp_pct_base"]  == _PTS_DP_PCT_BASE
        assert result["breakdown"]["dp_pct_bonus"] == 0

    def test_dp_pct_base_and_bonus(self):
        dp = _make_dp("X", dp_pct=60.0, volume_spike_ratio=None)
        result = _score_dp_print(dp, [])

        assert result["score"] == _PTS_DP_PCT_BASE + _PTS_DP_PCT_BONUS

    def test_dp_pct_below_threshold_scores_zero(self):
        dp = _make_dp("X", dp_pct=39.9)
        result = _score_dp_print(dp, [])

        assert result["breakdown"]["dp_pct_base"]  == 0
        assert result["breakdown"]["dp_pct_bonus"] == 0

    def test_spike_base_only(self):
        dp = _make_dp("X", dp_pct=30.0, volume_spike_ratio=2.0)
        result = _score_dp_print(dp, [])

        assert result["breakdown"]["spike_base"]  == _PTS_SPIKE_BASE
        assert result["breakdown"]["spike_bonus"] == 0

    def test_spike_base_and_bonus(self):
        dp = _make_dp("X", dp_pct=30.0, volume_spike_ratio=3.0)
        result = _score_dp_print(dp, [])

        assert result["breakdown"]["spike_base"]  == _PTS_SPIKE_BASE
        assert result["breakdown"]["spike_bonus"] == _PTS_SPIKE_BONUS

    def test_spike_below_threshold_scores_zero(self):
        dp = _make_dp("X", dp_pct=30.0, volume_spike_ratio=1.4)
        result = _score_dp_print(dp, [])

        assert result["breakdown"]["spike_base"]  == 0
        assert result["breakdown"]["spike_bonus"] == 0

    def test_null_spike_ratio_scores_zero(self):
        dp = _make_dp("X", dp_pct=60.0, volume_spike_ratio=None)
        result = _score_dp_print(dp, [100.0, 100.5, 101.0])  # flat

        assert result["volume_spike_ratio"] is None
        assert result["breakdown"]["spike_base"]  == 0
        assert result["breakdown"]["spike_bonus"] == 0

    def test_price_flat_adds_points(self):
        dp = _make_dp("X", dp_pct=30.0, volume_spike_ratio=None)
        result = _score_dp_print(dp, [100.0, 100.5, 101.0])

        assert result["is_flat"] is True
        assert result["breakdown"]["price_flat"] == _PTS_PRICE_FLAT

    def test_price_not_flat_scores_zero(self):
        dp = _make_dp("X", dp_pct=30.0)
        result = _score_dp_print(dp, [100.0, 110.0])   # 10% range

        assert result["is_flat"] is False
        assert result["breakdown"]["price_flat"] == 0

    def test_no_price_data_scores_zero_for_flat(self):
        dp = _make_dp("X", dp_pct=30.0)
        result = _score_dp_print(dp, [])

        assert result["is_flat"] is False
        assert result["breakdown"]["price_flat"] == 0

    def test_price_close_is_last_element(self):
        dp = _make_dp("X")
        closes = [99.0, 100.0, 101.5]
        result = _score_dp_print(dp, closes)
        assert result["price_close"] == 101.5

    def test_price_close_none_when_no_closes(self):
        dp = _make_dp("X")
        result = _score_dp_print(dp, [])
        assert result["price_close"] is None

    def test_score_breakdown_sums_to_total(self):
        dp = _make_dp("X", dp_pct=60.0, volume_spike_ratio=3.0)
        result = _score_dp_print(dp, [100.0, 100.5])
        total = sum(result["breakdown"].values())
        assert total == result["score"]

    def test_returns_correct_ticker_and_week_ending(self):
        dp = _make_dp("TSLA", week_ending=date(2024, 2, 5))
        result = _score_dp_print(dp, [])
        assert result["ticker"]      == "TSLA"
        assert result["week_ending"] == date(2024, 2, 5)

    def test_medium_level_at_50_points(self):
        # dp_pct=45% (+20) + spike=2.0x (+20) = 40... need 50
        # dp_pct=60% (+35) + not flat = 35... need spike too
        # dp_pct=45% (+20) + price flat (+30) = 50 → medium
        dp = _make_dp("X", dp_pct=45.0, volume_spike_ratio=None)
        result = _score_dp_print(dp, [100.0, 100.5, 101.0])

        assert result["score"] == _PTS_DP_PCT_BASE + _PTS_PRICE_FLAT   # 50
        assert result["level"] == "medium"

    def test_at_spike_boundary_15x_scores_base(self):
        dp = _make_dp("X", dp_pct=30.0, volume_spike_ratio=1.5)
        result = _score_dp_print(dp, [])
        # Threshold is strictly greater than 1.5
        assert result["breakdown"]["spike_base"] == 0

    def test_just_above_spike_base_threshold(self):
        dp = _make_dp("X", dp_pct=30.0, volume_spike_ratio=1.51)
        result = _score_dp_print(dp, [])
        assert result["breakdown"]["spike_base"] == _PTS_SPIKE_BASE


# ===========================================================================
# score_ticker  (DB integration)
# ===========================================================================

class TestScoreTicker:
    def test_returns_none_when_no_print(self, db_session):
        result = score_ticker("NVDA", WEEK, db_session)
        assert result is None

    def test_scores_correctly_with_db_data(self, db_session):
        _add_dp(db_session, ticker="NVDA", dp_pct=60.0, volume_spike_ratio=3.0)
        _add_prices(db_session, "NVDA", [100.0, 100.5, 101.0])

        result = score_ticker("NVDA", WEEK, db_session)

        assert result is not None
        assert result["ticker"] == "NVDA"
        assert result["score"] == 100
        assert result["level"] == "high"

    def test_price_flat_uses_db_closes(self, db_session):
        """Flat prices in DB → +30 pts; volatile prices → 0 pts."""
        _add_dp(db_session, ticker="AAPL", dp_pct=30.0)
        _add_prices(db_session, "AAPL", [100.0, 100.5, 101.0])   # flat

        result = score_ticker("AAPL", WEEK, db_session)
        assert result["breakdown"]["price_flat"] == _PTS_PRICE_FLAT

    def test_no_price_data_still_scores_dp_criteria(self, db_session):
        _add_dp(db_session, ticker="NOPRICE", dp_pct=60.0, volume_spike_ratio=3.0)
        # No price snapshots added

        result = score_ticker("NOPRICE", WEEK, db_session)

        assert result is not None
        expected = _PTS_DP_PCT_BASE + _PTS_DP_PCT_BONUS + _PTS_SPIKE_BASE + _PTS_SPIKE_BONUS
        assert result["score"] == expected


# ===========================================================================
# run_weekly_scan  (DB integration)
# ===========================================================================

class TestRunWeeklyScan:
    def test_returns_empty_when_no_prints(self, db_session):
        result = run_weekly_scan(db_session)
        assert result == []

    def test_auto_detects_latest_week(self, db_session):
        _add_dp(db_session, ticker="NVDA", dp_pct=60.0, volume_spike_ratio=3.0,
                week_ending=WEEK)
        _add_prices(db_session, "NVDA", [100.0, 100.5, 101.0])

        result = run_weekly_scan(db_session)

        assert len(result) == 1
        assert result[0]["ticker"] == "NVDA"

    def test_saves_signal_to_db(self, db_session):
        _add_dp(db_session, ticker="NVDA", dp_pct=60.0, volume_spike_ratio=3.0)
        _add_prices(db_session, "NVDA", [100.0, 100.5, 101.0])

        run_weekly_scan(db_session, week_ending=WEEK)

        signal = db_session.query(Signal).filter_by(ticker="NVDA").first()
        assert signal is not None
        assert signal.score == 100
        assert signal.signal_type == SIGNAL_TYPE
        assert signal.alerted is False

    def test_excludes_tickers_below_threshold(self, db_session):
        # Score = 0: dp_pct=20%, no spike, not flat
        _add_dp(db_session, ticker="LOW", dp_pct=20.0, volume_spike_ratio=None)
        _add_prices(db_session, "LOW", [100.0, 110.0])  # volatile — not flat

        result = run_weekly_scan(db_session, week_ending=WEEK)

        assert result == []
        assert db_session.query(Signal).count() == 0

    def test_filters_multiple_tickers_by_threshold(self, db_session):
        # HIGH: score 100
        _add_dp(db_session, ticker="HIGH", dp_pct=60.0, volume_spike_ratio=3.0)
        _add_prices(db_session, "HIGH", [100.0, 100.5, 101.0])

        # MEDIUM: dp_pct=45% (+20) + flat (+30) = 50
        _add_dp(db_session, ticker="MED", dp_pct=45.0, volume_spike_ratio=None)
        _add_prices(db_session, "MED", [200.0, 200.5, 201.0])

        # BELOW: dp_pct=20%, no spike, volatile
        _add_dp(db_session, ticker="SKIP", dp_pct=20.0, volume_spike_ratio=None)
        _add_prices(db_session, "SKIP", [50.0, 60.0])

        result = run_weekly_scan(db_session, week_ending=WEEK)
        saved_tickers = {r["ticker"] for r in result}

        assert "HIGH" in saved_tickers
        assert "MED"  in saved_tickers
        assert "SKIP" not in saved_tickers
        assert db_session.query(Signal).count() == 2

    def test_results_sorted_by_score_descending(self, db_session):
        for ticker, dp_pct, spike in [
            ("A", 60.0, 3.0),   # score 70 (no flat)
            ("B", 45.0, 3.0),   # score 55 (no flat)
            ("C", 60.0, None),  # score 35 → below threshold, excluded
        ]:
            _add_dp(db_session, ticker=ticker, dp_pct=dp_pct, volume_spike_ratio=spike)
            _add_prices(db_session, ticker, [100.0, 110.0])  # not flat

        result = run_weekly_scan(db_session, week_ending=WEEK)
        scores = [r["score"] for r in result]
        assert scores == sorted(scores, reverse=True)

    def test_idempotent_rerun_does_not_duplicate(self, db_session):
        _add_dp(db_session, ticker="NVDA", dp_pct=60.0, volume_spike_ratio=3.0)
        _add_prices(db_session, "NVDA", [100.0, 100.5, 101.0])

        run_weekly_scan(db_session, week_ending=WEEK)
        run_weekly_scan(db_session, week_ending=WEEK)

        count = db_session.query(Signal).filter_by(ticker="NVDA").count()
        assert count == 1

    def test_rerun_updates_score(self, db_session):
        """If the underlying dark pool data changes between runs, score is refreshed."""
        dp = _add_dp(db_session, ticker="NVDA", dp_pct=45.0, volume_spike_ratio=None)
        _add_prices(db_session, "NVDA", [100.0, 100.5, 101.0])

        run_weekly_scan(db_session, week_ending=WEEK)

        first_score = db_session.query(Signal).filter_by(ticker="NVDA").first().score

        # Simulate updated ingest with higher dp_pct
        dp.dp_pct = 60.0
        dp.volume_spike_ratio = 3.0
        db_session.flush()

        run_weekly_scan(db_session, week_ending=WEEK)
        updated_score = db_session.query(Signal).filter_by(ticker="NVDA").first().score

        assert updated_score > first_score

    def test_explicit_week_ending_overrides_auto(self, db_session):
        old_week = WEEK - timedelta(weeks=1)
        _add_dp(db_session, ticker="OLD", dp_pct=60.0, volume_spike_ratio=3.0, week_ending=old_week)
        _add_dp(db_session, ticker="NEW", dp_pct=60.0, volume_spike_ratio=3.0, week_ending=WEEK)
        _add_prices(db_session, "OLD", [100.0, 100.5, 101.0])
        _add_prices(db_session, "NEW", [200.0, 200.5, 201.0])

        result = run_weekly_scan(db_session, week_ending=old_week)

        tickers = {r["ticker"] for r in result}
        assert "OLD" in tickers
        assert "NEW" not in tickers


# ===========================================================================
# get_top_signals  (DB integration)
# ===========================================================================

class TestGetTopSignals:
    def _seed_signal(self, db_session, ticker: str, score: float) -> Signal:
        sig = Signal(
            ticker      = ticker,
            week_ending = WEEK,
            signal_type = SIGNAL_TYPE,
            score       = score,
            alerted     = False,
        )
        db_session.add(sig)
        _add_dp(db_session, ticker=ticker, dp_pct=60.0, volume_spike_ratio=2.0)
        db_session.flush()
        return sig

    def test_returns_empty_when_no_signals(self, db_session):
        assert get_top_signals(db_session) == []

    def test_sorted_by_score_descending(self, db_session):
        for ticker, score in [("A", 75.0), ("B", 50.0), ("C", 90.0)]:
            self._seed_signal(db_session, ticker, score)

        result = get_top_signals(db_session)
        scores = [r["score"] for r in result]
        assert scores == sorted(scores, reverse=True)

    def test_respects_limit(self, db_session):
        for i in range(10):
            self._seed_signal(db_session, f"T{i:02d}", float(50 + i))

        result = get_top_signals(db_session, limit=3)
        assert len(result) == 3

    def test_includes_price_close_when_available(self, db_session):
        self._seed_signal(db_session, "NVDA", 85.0)
        _add_prices(db_session, "NVDA", [490.0, 492.0, 495.0])

        result = get_top_signals(db_session)
        nvda = next(r for r in result if r["ticker"] == "NVDA")

        assert nvda["price_close"] == pytest.approx(495.0)
        assert nvda["price_date"] is not None

    def test_price_close_none_when_no_snapshot(self, db_session):
        self._seed_signal(db_session, "NOPX", 70.0)

        result = get_top_signals(db_session)
        nopx = next(r for r in result if r["ticker"] == "NOPX")

        assert nopx["price_close"] is None

    def test_includes_level_field(self, db_session):
        self._seed_signal(db_session, "H", 80.0)
        self._seed_signal(db_session, "M", 60.0)

        result = get_top_signals(db_session)
        levels = {r["ticker"]: r["level"] for r in result}

        assert levels["H"] == "high"
        assert levels["M"] == "medium"

    def test_includes_dp_print_metrics(self, db_session):
        self._seed_signal(db_session, "NVDA", 85.0)

        result = get_top_signals(db_session)
        nvda = next(r for r in result if r["ticker"] == "NVDA")

        assert nvda["dp_pct"]             is not None
        assert nvda["dp_volume"]          is not None
        assert nvda["volume_spike_ratio"] is not None


# ===========================================================================
# send_discord_alert
# ===========================================================================

def _sample_signals(n: int = 3) -> list[dict]:
    return [
        {
            "ticker":             f"TK{i}",
            "week_ending":        WEEK,
            "score":              float(90 - i * 10),
            "level":              "high" if i == 0 else "medium",
            "signal_type":        SIGNAL_TYPE,
            "dp_pct":             58.0 - i,
            "dp_volume":          5_000_000,
            "volume_spike_ratio": 3.0 - i * 0.3,
            "price_close":        100.0 + i,
            "alerted":            False,
        }
        for i in range(n)
    ]


class TestSendDiscordAlert:
    def test_returns_false_when_no_webhook_url(self):
        with patch("signals.scanner._DISCORD_WEBHOOK_URL", ""):
            result = send_discord_alert(_sample_signals())
        assert result is False

    def test_returns_false_when_signals_empty(self):
        with patch("signals.scanner._DISCORD_WEBHOOK_URL", "https://discord.example/webhook"):
            result = send_discord_alert([])
        assert result is False

    def test_posts_to_webhook_and_returns_true(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None

        mock_cm = MagicMock()
        mock_cm.post.return_value = mock_resp

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_cm
        mock_client.__exit__.return_value  = None

        with patch("signals.scanner._DISCORD_WEBHOOK_URL", "https://discord.example/webhook"):
            with patch("signals.scanner.httpx.Client", return_value=mock_client):
                result = send_discord_alert(_sample_signals())

        assert result is True
        mock_cm.post.assert_called_once()

    def test_payload_contains_all_tickers(self):
        captured = {}

        def _fake_post(url, json=None, **kw):
            captured["payload"] = json
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            return resp

        mock_cm = MagicMock()
        mock_cm.post.side_effect = _fake_post
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_cm
        mock_client.__exit__.return_value  = None

        signals = _sample_signals(3)
        with patch("signals.scanner._DISCORD_WEBHOOK_URL", "https://discord.example/webhook"):
            with patch("signals.scanner.httpx.Client", return_value=mock_client):
                send_discord_alert(signals)

        embed = captured["payload"]["embeds"][0]
        field_names = " ".join(f["name"] for f in embed["fields"])
        for sig in signals:
            assert sig["ticker"] in field_names

    def test_truncates_to_discord_alert_limit(self):
        """Should only post the top _DISCORD_ALERT_LIMIT signals even if more are passed."""
        from signals.scanner import _DISCORD_ALERT_LIMIT

        captured = {}

        def _fake_post(url, json=None, **kw):
            captured["payload"] = json
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            return resp

        mock_cm = MagicMock()
        mock_cm.post.side_effect = _fake_post
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_cm
        mock_client.__exit__.return_value  = None

        signals = _sample_signals(_DISCORD_ALERT_LIMIT + 5)
        with patch("signals.scanner._DISCORD_WEBHOOK_URL", "https://discord.example/webhook"):
            with patch("signals.scanner.httpx.Client", return_value=mock_client):
                send_discord_alert(signals)

        embed = captured["payload"]["embeds"][0]
        assert len(embed["fields"]) == _DISCORD_ALERT_LIMIT

    def test_returns_false_on_http_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        error = httpx.HTTPStatusError("429", request=MagicMock(), response=mock_resp)

        mock_cm = MagicMock()
        mock_cm.post.side_effect = error
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_cm
        mock_client.__exit__.return_value  = None

        with patch("signals.scanner._DISCORD_WEBHOOK_URL", "https://discord.example/webhook"):
            with patch("signals.scanner.httpx.Client", return_value=mock_client):
                result = send_discord_alert(_sample_signals())

        assert result is False

    def test_returns_false_on_network_error(self):
        mock_cm = MagicMock()
        mock_cm.post.side_effect = httpx.ConnectError("unreachable")
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_cm
        mock_client.__exit__.return_value  = None

        with patch("signals.scanner._DISCORD_WEBHOOK_URL", "https://discord.example/webhook"):
            with patch("signals.scanner.httpx.Client", return_value=mock_client):
                result = send_discord_alert(_sample_signals())

        assert result is False

    def test_marks_alerted_in_db_on_success(self, db_session):
        """When db is passed, alerted=True should be set on matching Signal rows."""
        # Seed Signal rows
        for sig_data in _sample_signals(2):
            db_session.add(Signal(
                ticker      = sig_data["ticker"],
                week_ending = sig_data["week_ending"],
                signal_type = SIGNAL_TYPE,
                score       = sig_data["score"],
                alerted     = False,
            ))
        db_session.commit()

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_cm = MagicMock()
        mock_cm.post.return_value = mock_resp
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_cm
        mock_client.__exit__.return_value  = None

        with patch("signals.scanner._DISCORD_WEBHOOK_URL", "https://discord.example/webhook"):
            with patch("signals.scanner.httpx.Client", return_value=mock_client):
                send_discord_alert(_sample_signals(2), db=db_session)

        alerted_count = db_session.query(Signal).filter_by(alerted=True).count()
        assert alerted_count == 2

    def test_does_not_mark_alerted_when_post_fails(self, db_session):
        for sig_data in _sample_signals(2):
            db_session.add(Signal(
                ticker      = sig_data["ticker"],
                week_ending = sig_data["week_ending"],
                signal_type = SIGNAL_TYPE,
                score       = sig_data["score"],
                alerted     = False,
            ))
        db_session.commit()

        mock_cm = MagicMock()
        mock_cm.post.side_effect = httpx.ConnectError("unreachable")
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_cm
        mock_client.__exit__.return_value  = None

        with patch("signals.scanner._DISCORD_WEBHOOK_URL", "https://discord.example/webhook"):
            with patch("signals.scanner.httpx.Client", return_value=mock_client):
                send_discord_alert(_sample_signals(2), db=db_session)

        assert db_session.query(Signal).filter_by(alerted=True).count() == 0

    def test_null_price_and_spike_renders_na(self):
        """Signals with missing optional fields should not raise."""
        signals = [{
            "ticker":             "EDGE",
            "week_ending":        WEEK,
            "score":              75.0,
            "level":              "high",
            "signal_type":        SIGNAL_TYPE,
            "dp_pct":             50.0,
            "dp_volume":          1_000_000,
            "volume_spike_ratio": None,
            "price_close":        None,
            "alerted":            False,
        }]
        captured = {}

        def _fake_post(url, json=None, **kw):
            captured["payload"] = json
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            return resp

        mock_cm = MagicMock()
        mock_cm.post.side_effect = _fake_post
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_cm
        mock_client.__exit__.return_value  = None

        with patch("signals.scanner._DISCORD_WEBHOOK_URL", "https://discord.example/webhook"):
            with patch("signals.scanner.httpx.Client", return_value=mock_client):
                result = send_discord_alert(signals)

        assert result is True
        field_value = captured["payload"]["embeds"][0]["fields"][0]["value"]
        assert "N/A" in field_value   # both spike and price should show N/A

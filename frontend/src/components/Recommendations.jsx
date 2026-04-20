import { useCallback, useEffect, useState } from "react";
import { api } from "../api/client";
import AccountManager from "./AccountManager";

// ── Score badge ─────────────────────────────────────────────────────────────

function ScoreBadge({ score, level }) {
  const ring = {
    high:   "ring-emerald-500/40 text-emerald-300 bg-emerald-500/10",
    medium: "ring-amber-500/40  text-amber-300  bg-amber-500/10",
    low:    "ring-slate-600     text-slate-400  bg-slate-700/30",
  }[level] ?? "ring-slate-600 text-slate-400";

  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-bold ring-1 ring-inset ${ring}`}>
      {score.toFixed(1)}
    </span>
  );
}

// ── Mini score bar (dp vs sentiment) ─────────────────────────────────────────

function ScoreBar({ dpScore, sentScore }) {
  const dpW   = Math.round(dpScore   * 0.6);   // weighted contribution
  const sentW = sentScore != null ? Math.round(sentScore * 0.4) : 0;
  const total = dpW + sentW;

  return (
    <div className="flex h-1.5 w-full overflow-hidden rounded-full bg-slate-700/50">
      <div
        className="bg-sky-500"
        style={{ width: `${total}%` }}
        title={`DP ${dpScore.toFixed(0)} × 60% + Sentiment ${sentScore?.toFixed(0) ?? "–"} × 40%`}
      />
    </div>
  );
}

// ── Sentiment pill ───────────────────────────────────────────────────────────

function SentimentPill({ score, bullish, bearish, tweetCount }) {
  if (score == null) {
    return <span className="text-[10px] text-slate-600">no data</span>;
  }

  const color =
    score >= 65 ? "text-emerald-400"
    : score >= 45 ? "text-slate-400"
    : "text-rose-400";

  return (
    <span className={`text-xs font-semibold tabular-nums ${color}`} title={`${bullish}B / ${bearish}Be  (${tweetCount} tweets)`}>
      {score.toFixed(0)}
      <span className="ml-1 text-[10px] font-normal text-slate-500">
        {bullish}↑ {bearish}↓
      </span>
    </span>
  );
}

// ── Main component ───────────────────────────────────────────────────────────

export default function Recommendations({ onSelectTicker }) {
  const [picks, setPicks]             = useState([]);
  const [status, setStatus]           = useState(null);
  const [loading, setLoading]         = useState(false);
  const [refreshing, setRefreshing]   = useState(false);
  const [showAccounts, setShowAccounts] = useState(false);
  const [error, setError]             = useState(null);
  const [minScore, setMinScore]       = useState(30);

  const loadPicks = useCallback(() => {
    setLoading(true);
    setError(null);
    Promise.all([
      api.getRecommendations({ min_score: minScore, limit: 25 }),
      api.getSentimentStatus(),
    ])
      .then(([recs, st]) => {
        setPicks(recs);
        setStatus(st);
      })
      .catch(() => setError("Failed to load recommendations"))
      .finally(() => setLoading(false));
  }, [minScore]);

  useEffect(() => { loadPicks(); }, [loadPicks]);

  const handleRefresh = async () => {
    setRefreshing(true);
    setError(null);
    try {
      await api.refreshSentiment();
      await loadPicks();
    } catch (err) {
      setError(err?.message ?? "Refresh failed — try again or check backend logs");
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <div className="flex flex-col gap-4">
      {/* ── Toolbar ── */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2">
          <p className="text-[10px] font-bold uppercase tracking-widest text-slate-600">
            Picks
          </p>
          {status && (
            <span className="text-[10px] text-slate-600">
              {status.active_accounts} accounts · {status.total_tweets} tweets · {status.tickers_tracked} tickers
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {/* Min score filter */}
          <label className="flex items-center gap-1.5 text-[10px] text-slate-500">
            min score
            <select
              value={minScore}
              onChange={(e) => setMinScore(Number(e.target.value))}
              className="rounded bg-slate-800 border border-slate-700 px-1.5 py-0.5 text-[10px] text-slate-300 outline-none"
            >
              <option value={30}>30</option>
              <option value={40}>40</option>
              <option value={50}>50</option>
              <option value={60}>60</option>
              <option value={70}>70</option>
            </select>
          </label>

          <button
            onClick={() => setShowAccounts(true)}
            className="rounded-lg border border-slate-700 bg-slate-800/60 px-2.5 py-1 text-[11px] text-slate-400 hover:text-slate-200 hover:border-slate-600 transition-colors"
          >
            Accounts
          </button>

          <button
            onClick={handleRefresh}
            disabled={refreshing}
            className="rounded-lg bg-sky-600 px-2.5 py-1 text-[11px] font-semibold text-white hover:bg-sky-500 disabled:opacity-50 transition-colors"
          >
            {refreshing ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </div>

      {/* ── Error ── */}
      {error && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
          {error}
        </div>
      )}

      {/* ── Empty / loading states ── */}
      {loading && (
        <p className="py-8 text-center text-xs text-slate-600">Loading…</p>
      )}
      {!loading && picks.length === 0 && !error && (
        <div className="py-12 text-center">
          <p className="text-sm text-slate-500">No recommendations yet.</p>
          <p className="mt-1 text-xs text-slate-600">
            Click <strong className="text-slate-400">Refresh</strong> to fetch tweets and score sentiment,
            or ensure dark pool data has been ingested first.
          </p>
        </div>
      )}

      {/* ── Table ── */}
      {!loading && picks.length > 0 && (
        <div className="overflow-x-auto rounded-xl border border-slate-800">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-slate-800 text-[10px] font-bold uppercase tracking-wider text-slate-600">
                <th className="px-3 py-2 text-left">#</th>
                <th className="px-3 py-2 text-left">Ticker</th>
                <th className="px-3 py-2 text-left">Score</th>
                <th className="px-3 py-2 text-left w-28">Breakdown</th>
                <th className="px-3 py-2 text-right">DP%</th>
                <th className="px-3 py-2 text-right">Spike</th>
                <th className="px-3 py-2 text-right">Sentiment</th>
                <th className="px-3 py-2 text-right">Price</th>
                <th className="px-3 py-2 text-left hidden lg:table-cell">Sector</th>
              </tr>
            </thead>
            <tbody>
              {picks.map((p, i) => (
                <tr
                  key={p.ticker}
                  onClick={() => onSelectTicker?.(p.ticker)}
                  className="border-b border-slate-800/60 hover:bg-slate-800/30 cursor-pointer transition-colors"
                >
                  <td className="px-3 py-2.5 text-slate-600 tabular-nums">{i + 1}</td>

                  <td className="px-3 py-2.5">
                    <div className="flex flex-col">
                      <span className="font-bold text-white">{p.ticker}</span>
                      {p.name && (
                        <span className="text-[10px] text-slate-500 truncate max-w-[9rem]">{p.name}</span>
                      )}
                    </div>
                  </td>

                  <td className="px-3 py-2.5">
                    <ScoreBadge score={p.combined_score} level={p.level} />
                  </td>

                  <td className="px-3 py-2.5">
                    <div className="flex flex-col gap-1.5 w-28">
                      <ScoreBar dpScore={p.dp_score} sentScore={p.sentiment_score} />
                      <div className="flex justify-between text-[9px] text-slate-600">
                        <span>DP {p.dp_score.toFixed(0)}</span>
                        <span>{p.sentiment_score != null ? `S ${p.sentiment_score.toFixed(0)}` : "no sent."}</span>
                      </div>
                    </div>
                  </td>

                  <td className="px-3 py-2.5 text-right tabular-nums text-slate-300">
                    {p.dp_pct != null ? `${p.dp_pct.toFixed(1)}%` : "—"}
                  </td>

                  <td className="px-3 py-2.5 text-right tabular-nums">
                    {p.volume_spike_ratio != null ? (
                      <span className={p.volume_spike_ratio >= 2 ? "text-sky-400" : "text-slate-300"}>
                        {p.volume_spike_ratio.toFixed(2)}x
                      </span>
                    ) : "—"}
                  </td>

                  <td className="px-3 py-2.5 text-right">
                    <SentimentPill
                      score={p.sentiment_score}
                      bullish={p.bullish_count}
                      bearish={p.bearish_count}
                      tweetCount={p.tweet_count}
                    />
                  </td>

                  <td className="px-3 py-2.5 text-right tabular-nums text-slate-300">
                    {p.price_close != null ? `$${p.price_close.toFixed(2)}` : "—"}
                  </td>

                  <td className="px-3 py-2.5 text-left text-slate-500 hidden lg:table-cell">
                    {p.sector ?? "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Score legend */}
      {picks.length > 0 && (
        <p className="text-[10px] text-slate-600 text-center">
          Score = 60% dark pool signal + 40% Twitter sentiment  ·  Breakdown bar shows weighted contributions
        </p>
      )}

      {/* Account manager modal */}
      {showAccounts && (
        <AccountManager onClose={() => { setShowAccounts(false); loadPicks(); }} />
      )}
    </div>
  );
}

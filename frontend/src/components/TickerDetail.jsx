import { useEffect, useState } from "react";
import {
  Area,
  Bar,
  ComposedChart,
  Legend,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { api } from "../api/client";
import AlertBadge from "./AlertBadge";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function formatVol(v) {
  if (v == null) return "—";
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000)     return `${(v / 1_000).toFixed(0)}K`;
  return v.toLocaleString();
}

const CHART_STYLE = {
  contentStyle: {
    background: "#1e293b",
    border: "1px solid #475569",
    borderRadius: "6px",
    fontSize: 12,
  },
  itemStyle: { color: "#cbd5e1" },
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------
export default function TickerDetail({
  ticker,
  signal,
  onClose,
  watchlistEntry,
  onWatchlistChange,
}) {
  const [priceData, setPriceData]   = useState([]);
  const [dpHistory, setDpHistory]   = useState([]);
  const [loading, setLoading]       = useState(true);
  const [adding, setAdding]         = useState(false);
  const [entryPrice, setEntryPrice] = useState("");
  const [busy, setBusy]             = useState(false);

  const isInWatchlist = watchlistEntry != null;

  useEffect(() => {
    if (!ticker) return;
    setLoading(true);
    Promise.all([
      api.getTickerPrice(ticker, 30).catch(() => []),
      api.getTickerHistory(ticker, 12).catch(() => []),
    ]).then(([price, hist]) => {
      setPriceData(price);
      setDpHistory(hist);
    }).finally(() => setLoading(false));
  }, [ticker]);

  const latestDp    = dpHistory[dpHistory.length - 1];
  const latestPrice = priceData[priceData.length - 1];

  // Single reference line at the actual signal week_ending (not ±3-day spread)
  const signalWeekEnding = signal?.week_ending
    ? String(signal.week_ending)
    : latestDp?.week_ending
      ? String(latestDp.week_ending)
      : null;

  // 30-day price stats
  const closes    = priceData.map((d) => d.close).filter((c) => c != null);
  const priceHigh = closes.length ? Math.max(...closes) : null;
  const priceLow  = closes.length ? Math.min(...closes) : null;
  const maxVol    = Math.max(...priceData.map((d) => d.volume ?? 0), 1);

  // Trade setup — prefer backend-computed values from signal prop, fall back to
  // computing from DP history (latestDp.close = price near week_ending).
  const printPrice   = signal?.print_price   ?? latestDp?.close      ?? null;
  const currentPrice = signal?.price_close   ?? latestPrice?.close   ?? null;
  const printDate    = signal?.week_ending   ?? latestDp?.week_ending ?? null;

  let bias = signal?.bias ?? null;
  let pricePct    = signal?.price_vs_print_pct ?? null;
  let targetLong  = signal?.target_long  ?? null;
  let targetShort = signal?.target_short ?? null;
  let stopLong    = signal?.stop_long    ?? null;
  let stopShort   = signal?.stop_short   ?? null;

  // If no signal prop (e.g. opened from watchlist), compute locally
  if (bias === null && printPrice != null && currentPrice != null && printPrice !== 0) {
    pricePct = ((currentPrice - printPrice) / printPrice * 100);
    if (currentPrice >= printPrice * 1.02)      bias = "long";
    else if (currentPrice <= printPrice * 0.97) bias = "short";
    else                                         bias = "neutral";
    targetLong  = (printPrice * 1.08).toFixed(2);
    targetShort = (printPrice * 0.92).toFixed(2);
    stopLong    = (printPrice * 0.96).toFixed(2);
    stopShort   = (printPrice * 1.04).toFixed(2);
  } else if (pricePct !== null) {
    pricePct = pricePct; // already a number from API
  }

  const handleAdd = async () => {
    setBusy(true);
    try {
      await api.addToWatchlist(ticker, entryPrice ? Number(entryPrice) : null, null);
      setAdding(false);
      setEntryPrice("");
      onWatchlistChange?.();
    } finally {
      setBusy(false);
    }
  };

  const handleRemove = async () => {
    setBusy(true);
    try {
      await api.removeFromWatchlist(watchlistEntry.id);
      onWatchlistChange?.();
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 z-40 bg-black/60 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Panel */}
      <div className="fixed right-0 top-0 bottom-0 z-50 flex w-full max-w-xl flex-col border-l border-slate-700 bg-slate-900 shadow-2xl overflow-y-auto">
        {/* Header */}
        <div className="flex items-start justify-between border-b border-slate-700 px-6 py-4">
          <div>
            <h2 className="text-xl font-bold font-mono text-white">{ticker}</h2>
            {latestDp && (
              <div className="mt-1">
                <AlertBadge level={latestDp.volume_spike_ratio >= 2.5 ? "high" : latestDp.volume_spike_ratio >= 1.5 ? "medium" : "low"} />
              </div>
            )}
          </div>
          <button
            onClick={onClose}
            className="mt-0.5 text-slate-500 hover:text-white transition-colors text-lg leading-none"
            aria-label="Close"
          >
            ✕
          </button>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-3 gap-3 border-b border-slate-700 px-6 py-4">
          {[
            {
              label: "DP %",
              value: latestDp?.dp_pct != null ? `${latestDp.dp_pct.toFixed(1)}%` : "—",
            },
            {
              label: "Spike Ratio",
              value: latestDp?.volume_spike_ratio != null
                ? `${latestDp.volume_spike_ratio.toFixed(2)}×`
                : "—",
            },
            {
              label: "Price",
              value: latestPrice?.close != null ? `$${latestPrice.close.toFixed(2)}` : "—",
            },
          ].map(({ label, value }) => (
            <div key={label} className="rounded-lg bg-slate-800 px-4 py-3">
              <p className="text-xs text-slate-400">{label}</p>
              <p className="mt-0.5 text-base font-semibold tabular-nums text-white">{value}</p>
            </div>
          ))}
        </div>

        {/* Trade Setup */}
        {printPrice != null && currentPrice != null && (
          <div className="border-b border-slate-700 px-6 py-4">
            <h3 className="mb-3 text-xs font-semibold uppercase tracking-widest text-slate-500">
              Trade Setup
            </h3>

            {/* Direction */}
            <div className="mb-3 flex items-center justify-between">
              <span className="text-xs text-slate-500">Direction</span>
              <span className={`text-sm font-bold ${
                bias === "long" ? "text-emerald-400" :
                bias === "short" ? "text-red-400" :
                "text-slate-400"
              }`}>
                {bias === "long"  ? "↑ Long"    :
                 bias === "short" ? "↓ Short"   :
                                    "→ Neutral — wait for confirmation"}
              </span>
            </div>

            {/* Price vs print grid */}
            <div className="mb-3 grid grid-cols-2 gap-2 rounded-lg bg-slate-800/50 p-3 text-xs">
              <div>
                <p className="text-slate-500">Print Date</p>
                <p className="mt-0.5 font-medium text-white">{String(printDate)}</p>
              </div>
              <div>
                <p className="text-slate-500">Print Price</p>
                <p className="mt-0.5 font-medium tabular-nums text-white">
                  ${Number(printPrice).toFixed(2)}
                </p>
              </div>
              <div>
                <p className="text-slate-500">Current Price</p>
                <p className="mt-0.5 font-medium tabular-nums text-white">
                  ${Number(currentPrice).toFixed(2)}
                </p>
              </div>
              <div>
                <p className="text-slate-500">Move Since Print</p>
                <p className={`mt-0.5 font-semibold tabular-nums ${
                  pricePct == null ? "text-slate-400" :
                  Number(pricePct) >= 0 ? "text-emerald-400" : "text-red-400"
                }`}>
                  {pricePct != null
                    ? `${Number(pricePct) >= 0 ? "+" : ""}${Number(pricePct).toFixed(1)}%`
                    : "—"}
                </p>
              </div>
            </div>

            {/* Targets */}
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/5 px-3 py-2.5">
                <p className="text-[10px] font-semibold uppercase tracking-wider text-emerald-400/60">
                  Long Target
                </p>
                <p className="mt-1 text-base font-bold tabular-nums text-emerald-300">
                  ${targetLong != null ? Number(targetLong).toFixed(2) : "—"}
                </p>
                <p className="mt-0.5 text-slate-500">
                  Stop: ${stopLong != null ? Number(stopLong).toFixed(2) : "—"}
                </p>
              </div>
              <div className="rounded-lg border border-red-500/20 bg-red-500/5 px-3 py-2.5">
                <p className="text-[10px] font-semibold uppercase tracking-wider text-red-400/60">
                  Short Target
                </p>
                <p className="mt-1 text-base font-bold tabular-nums text-red-300">
                  ${targetShort != null ? Number(targetShort).toFixed(2) : "—"}
                </p>
                <p className="mt-0.5 text-slate-500">
                  Stop: ${stopShort != null ? Number(stopShort).toFixed(2) : "—"}
                </p>
              </div>
            </div>

            <p className="mt-2 text-[10px] text-slate-600">
              Targets ±8% · Stops ±4% from print price. Not financial advice.
            </p>
          </div>
        )}

        {/* Watchlist action */}
        <div className="border-b border-slate-700 px-6 py-3">
          {isInWatchlist ? (
            <div className="flex items-center justify-between">
              <span className="text-sm text-slate-400">
                On watchlist ·{" "}
                <span className="capitalize text-slate-300">{watchlistEntry.status}</span>
              </span>
              <button
                onClick={handleRemove}
                disabled={busy}
                className="text-xs text-red-400 hover:text-red-300 disabled:opacity-50 transition-colors"
              >
                Remove
              </button>
            </div>
          ) : adding ? (
            <div className="flex items-center gap-2">
              <input
                autoFocus
                type="number"
                step="0.01"
                placeholder="Entry price (optional)"
                value={entryPrice}
                onChange={(e) => setEntryPrice(e.target.value)}
                className="flex-1 rounded border border-slate-600 bg-slate-800 px-3 py-1.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
              />
              <button
                onClick={handleAdd}
                disabled={busy}
                className="rounded bg-blue-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-blue-500 disabled:opacity-50 transition-colors"
              >
                Add
              </button>
              <button
                onClick={() => setAdding(false)}
                className="text-xs text-slate-500 hover:text-slate-300 transition-colors"
              >
                Cancel
              </button>
            </div>
          ) : (
            <button
              onClick={() => setAdding(true)}
              className="rounded-md border border-slate-600 px-3 py-1.5 text-xs font-medium text-slate-300 hover:border-blue-500 hover:text-blue-400 transition-colors"
            >
              + Add to Watchlist
            </button>
          )}
        </div>

        {/* Charts */}
        {loading ? (
          <div className="flex flex-1 items-center justify-center text-slate-500 text-sm">
            Loading charts…
          </div>
        ) : (
          <div className="flex-1 space-y-6 px-6 py-5">
            {/* Price chart — area + volume */}
            <div>
              <div className="mb-2 flex items-baseline justify-between">
                <h3 className="text-xs font-semibold uppercase tracking-widest text-slate-500">
                  30-Day Price
                </h3>
                {priceHigh != null && (
                  <span className="text-[11px] tabular-nums text-slate-500">
                    <span className="text-emerald-400">${priceHigh.toFixed(2)}</span>
                    {" · "}
                    <span className="text-red-400">${priceLow.toFixed(2)}</span>
                    {" · 30d H/L"}
                  </span>
                )}
              </div>
              {priceData.length === 0 ? (
                <p className="text-xs text-slate-500">No price data available.</p>
              ) : (
                <ResponsiveContainer width="100%" height={200}>
                  <ComposedChart
                    data={priceData}
                    margin={{ top: 5, right: 5, bottom: 5, left: 10 }}
                  >
                    <defs>
                      <linearGradient id="closeGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%"  stopColor="#6366f1" stopOpacity={0.3} />
                        <stop offset="95%" stopColor="#6366f1" stopOpacity={0}   />
                      </linearGradient>
                    </defs>
                    <XAxis
                      dataKey="date"
                      tick={{ fontSize: 10, fill: "#94a3b8" }}
                      tickFormatter={(v) => v.slice(5)}
                      interval="preserveStartEnd"
                    />
                    <YAxis
                      yAxisId="price"
                      domain={["auto", "auto"]}
                      tick={{ fontSize: 10, fill: "#94a3b8" }}
                      tickFormatter={(v) => `$${v.toFixed(0)}`}
                      width={44}
                    />
                    <YAxis
                      yAxisId="vol"
                      orientation="right"
                      hide
                      domain={[0, maxVol * 5]}
                    />
                    <Tooltip
                      contentStyle={CHART_STYLE.contentStyle}
                      itemStyle={CHART_STYLE.itemStyle}
                      formatter={(val, name) => {
                        if (val == null) return ["—", name];
                        if (name === "Volume") return [formatVol(val), name];
                        return [`$${Number(val).toFixed(2)}`, name];
                      }}
                    />
                    {/* Volume bars behind price */}
                    <Bar
                      yAxisId="vol"
                      dataKey="volume"
                      fill="#334155"
                      opacity={0.7}
                      maxBarSize={8}
                      name="Volume"
                    />
                    {/* Close price area */}
                    <Area
                      yAxisId="price"
                      dataKey="close"
                      stroke="#6366f1"
                      strokeWidth={1.5}
                      fill="url(#closeGrad)"
                      dot={false}
                      name="Price"
                      connectNulls
                    />
                    {/* Single signal marker at week_ending */}
                    {signalWeekEnding && (
                      <ReferenceLine
                        yAxisId="price"
                        x={signalWeekEnding}
                        stroke="#f59e0b"
                        strokeDasharray="4 3"
                        strokeWidth={1.5}
                        label={{ value: "↑", position: "top", fontSize: 11, fill: "#f59e0b" }}
                      />
                    )}
                  </ComposedChart>
                </ResponsiveContainer>
              )}
            </div>

            {/* DP volume chart */}
            <div>
              <h3 className="mb-2 text-xs font-semibold uppercase tracking-widest text-slate-500">
                12-Week Dark Pool Volume
              </h3>
              {dpHistory.length === 0 ? (
                <p className="text-xs text-slate-500">No dark pool history available.</p>
              ) : (
                <ResponsiveContainer width="100%" height={180}>
                  <ComposedChart
                    data={dpHistory}
                    margin={{ top: 5, right: 10, bottom: 5, left: 10 }}
                  >
                    <XAxis
                      dataKey="week_ending"
                      tick={{ fontSize: 10, fill: "#94a3b8" }}
                      tickFormatter={(v) => v.slice(5)}
                      interval="preserveStartEnd"
                    />
                    <YAxis
                      tick={{ fontSize: 10, fill: "#94a3b8" }}
                      tickFormatter={formatVol}
                      width={48}
                    />
                    <Tooltip
                      contentStyle={CHART_STYLE.contentStyle}
                      itemStyle={CHART_STYLE.itemStyle}
                      formatter={(val, name) => [formatVol(val), name]}
                    />
                    <Legend
                      wrapperStyle={{ fontSize: 11, color: "#94a3b8", paddingTop: 4 }}
                    />
                    <Bar
                      dataKey="dp_volume"
                      fill="#6366f1"
                      opacity={0.8}
                      name="DP Volume"
                      maxBarSize={24}
                    />
                    <Line
                      dataKey="dp_volume_4wk_avg"
                      stroke="#f59e0b"
                      dot={false}
                      strokeWidth={2}
                      name="4-wk Avg"
                      connectNulls
                    />
                  </ComposedChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>
        )}
      </div>
    </>
  );
}

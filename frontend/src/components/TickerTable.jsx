import { useMemo, useState } from "react";
import AlertBadge from "./AlertBadge";

function formatVolume(v) {
  if (v == null) return "—";
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000)     return `${(v / 1_000).toFixed(0)}K`;
  return v.toLocaleString();
}

function dpPctStyle(pct) {
  if (pct == null) return {};
  const alpha = Math.min(pct / 80, 1) * 0.22;
  return { backgroundColor: `rgba(99,102,241,${alpha.toFixed(3)})` };
}

function BiasPill({ bias }) {
  if (!bias || bias === "neutral") {
    return <span className="text-xs text-slate-600">—</span>;
  }
  if (bias === "long") {
    return (
      <span className="inline-flex items-center gap-1 rounded px-2 py-0.5 text-xs font-bold bg-emerald-500/15 text-emerald-300 ring-1 ring-inset ring-emerald-500/25">
        ↑ Long
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 rounded px-2 py-0.5 text-xs font-bold bg-red-500/15 text-red-300 ring-1 ring-inset ring-red-500/25">
      ↓ Short
    </span>
  );
}

function ScorePill({ score }) {
  const num = Math.round(score);
  let cls;
  if (num >= 85)      cls = "bg-emerald-500/15 text-emerald-300 ring-1 ring-inset ring-emerald-500/25";
  else if (num >= 70) cls = "bg-green-500/15 text-green-300 ring-1 ring-inset ring-green-500/25";
  else if (num >= 50) cls = "bg-amber-500/15 text-amber-300 ring-1 ring-inset ring-amber-500/25";
  else                cls = "bg-slate-700/50 text-slate-400";
  return (
    <span className={`inline-flex min-w-[2rem] items-center justify-center rounded px-2 py-0.5 text-xs font-bold tabular-nums ${cls}`}>
      {num}
    </span>
  );
}

const COLUMNS = [
  { key: "ticker",             label: "Ticker",      sortable: true,  align: "left"  },
  { key: "score",              label: "Score",       sortable: true,  align: "right" },
  { key: "bias",               label: "Direction",   sortable: false, align: "left"  },
  { key: "dp_pct",             label: "DP %",        sortable: true,  align: "right" },
  { key: "volume_spike_ratio", label: "Spike",       sortable: true,  align: "right" },
  { key: "price_close",        label: "Price",       sortable: true,  align: "right" },
  { key: "print_price",        label: "Print $",     sortable: true,  align: "right" },
  { key: "price_vs_print_pct", label: "Move",        sortable: true,  align: "right" },
  { key: "dp_volume",          label: "DP Vol",      sortable: true,  align: "right" },
  { key: "level",              label: "Signal",      sortable: false, align: "left"  },
];

const maxScore = 100;

export default function TickerTable({ signals, onRowClick, watchlistTickers = new Set() }) {
  const [sortKey,  setSortKey]  = useState("score");
  const [sortDir,  setSortDir]  = useState("desc");
  const [minScore, setMinScore] = useState(50);
  const [minDpPct, setMinDpPct] = useState(0);

  const sorted = useMemo(() => {
    const filtered = (signals ?? []).filter(
      (s) => s.score >= minScore && (s.dp_pct ?? 0) >= minDpPct
    );
    return [...filtered].sort((a, b) => {
      const va = a[sortKey] ?? (sortDir === "asc" ? Infinity : -Infinity);
      const vb = b[sortKey] ?? (sortDir === "asc" ? Infinity : -Infinity);
      const cmp = typeof va === "string" ? va.localeCompare(vb) : va - vb;
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [signals, sortKey, sortDir, minScore, minDpPct]);

  const handleSort = (key) => {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir("desc"); }
  };

  return (
    <div className="flex flex-col gap-2">
      {/* ── Filter bar ── */}
      <div className="flex flex-wrap items-center gap-4 rounded border border-slate-800 bg-slate-900/60 px-3 py-2">
        <div className="flex items-center gap-2">
          <span className="text-[10px] font-bold uppercase tracking-wider text-slate-600">Min Score</span>
          <input
            type="range" min={0} max={100} step={5}
            value={minScore}
            onChange={(e) => setMinScore(Number(e.target.value))}
            className="w-24 accent-sky-500"
          />
          <span className="w-6 text-right text-[10px] font-bold tabular-nums text-slate-400">{minScore}</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] font-bold uppercase tracking-wider text-slate-600">Min DP%</span>
          <input
            type="number" min={0} max={100} step={1}
            value={minDpPct}
            onChange={(e) => setMinDpPct(Number(e.target.value))}
            className="w-14 rounded border border-slate-700 bg-slate-900 px-1.5 py-0.5 text-[10px] text-slate-300 focus:outline-none focus:ring-1 focus:ring-sky-500"
          />
        </div>
        <span className="ml-auto text-[10px] text-slate-600">
          {sorted.length} result{sorted.length !== 1 ? "s" : ""}
        </span>
      </div>

      {/* ── Table ── */}
      <div className="overflow-x-auto rounded border border-slate-800">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-slate-800 bg-slate-900">
              {COLUMNS.map((col) => (
                <th
                  key={col.key}
                  onClick={() => col.sortable && handleSort(col.key)}
                  className={`px-3 py-2 text-[10px] font-bold uppercase tracking-wider text-slate-600 text-${col.align} whitespace-nowrap ${
                    col.sortable ? "cursor-pointer select-none hover:text-slate-400 transition-colors" : ""
                  }`}
                >
                  {col.label}
                  {sortKey === col.key && (
                    <span className="ml-0.5 opacity-50">{sortDir === "asc" ? "↑" : "↓"}</span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.length === 0 && (
              <tr>
                <td colSpan={COLUMNS.length} className="px-3 py-12 text-center text-xs text-slate-700">
                  No signals match the current filters.
                </td>
              </tr>
            )}
            {sorted.map((sig) => {
              const movePct = sig.price_vs_print_pct;
              const scoreBar = Math.round((sig.score / maxScore) * 100);
              return (
                <tr
                  key={sig.ticker}
                  onClick={() => onRowClick?.(sig)}
                  className="border-t border-slate-800/60 cursor-pointer hover:bg-slate-800/40 transition-colors"
                >
                  {/* Ticker */}
                  <td className="px-3 py-2">
                    <span className="font-mono font-bold text-sky-400">{sig.ticker}</span>
                    {watchlistTickers.has(sig.ticker) && (
                      <span className="ml-1.5 text-[9px] text-indigo-400" title="On watchlist">★</span>
                    )}
                  </td>
                  {/* Score with bar */}
                  <td className="px-3 py-2 text-right">
                    <div className="flex items-center justify-end gap-1.5">
                      <div className="w-12 h-1 rounded-full bg-slate-800 overflow-hidden">
                        <div
                          className={`h-full rounded-full ${
                            sig.score >= 85 ? "bg-emerald-500" :
                            sig.score >= 70 ? "bg-green-500" :
                            sig.score >= 50 ? "bg-amber-500" : "bg-slate-600"
                          }`}
                          style={{ width: `${scoreBar}%` }}
                        />
                      </div>
                      <ScorePill score={sig.score} />
                    </div>
                  </td>
                  {/* Bias */}
                  <td className="px-3 py-2"><BiasPill bias={sig.bias} /></td>
                  {/* DP% */}
                  <td className="px-3 py-2 text-right tabular-nums text-slate-300" style={dpPctStyle(sig.dp_pct)}>
                    {sig.dp_pct != null ? `${sig.dp_pct.toFixed(1)}%` : "—"}
                  </td>
                  {/* Spike */}
                  <td className="px-3 py-2 text-right tabular-nums text-slate-400">
                    {sig.volume_spike_ratio != null ? `${sig.volume_spike_ratio.toFixed(2)}×` : "—"}
                  </td>
                  {/* Price */}
                  <td className="px-3 py-2 text-right tabular-nums text-slate-300">
                    {sig.price_close != null ? `$${sig.price_close.toFixed(2)}` : "—"}
                  </td>
                  {/* Print $ */}
                  <td className="px-3 py-2 text-right tabular-nums text-slate-500">
                    {sig.print_price != null ? `$${sig.print_price.toFixed(2)}` : "—"}
                  </td>
                  {/* Move */}
                  <td className={`px-3 py-2 text-right tabular-nums font-semibold ${
                    movePct == null ? "text-slate-700" :
                    movePct >= 0 ? "text-emerald-400" : "text-red-400"
                  }`}>
                    {movePct != null ? `${movePct >= 0 ? "+" : ""}${movePct.toFixed(1)}%` : "—"}
                  </td>
                  {/* DP Vol */}
                  <td className="px-3 py-2 text-right tabular-nums text-slate-500">
                    {formatVolume(sig.dp_volume)}
                  </td>
                  {/* Signal */}
                  <td className="px-3 py-2">
                    <AlertBadge level={sig.level} />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

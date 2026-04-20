import { useEffect, useState } from "react";
import { api } from "../api/client";

function formatSize(v) {
  if (v == null) return "—";
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
  return v.toLocaleString();
}

function formatDollars(v) {
  if (v == null) return "—";
  if (v >= 1e9) return `$${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

export default function PrintsFeed({ onSelectTicker }) {
  const [prints, setPrints] = useState([]);
  const [error,  setError]  = useState(false);

  const load = () =>
    api.getPrints({ days: 7, limit: 100 })
      .then((data) => { setPrints(data); setError(false); })
      .catch(() => setError(true));

  useEffect(() => {
    load();
    const id = setInterval(load, 60_000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-slate-800 shrink-0">
        <span className="text-[10px] font-bold uppercase tracking-widest text-slate-500">
          Prints
        </span>
        <span className="text-[10px] tabular-nums text-slate-600">{prints.length}</span>
      </div>

      {/* Column headers */}
      <div className="grid grid-cols-[2fr_1fr_1fr] px-3 py-1.5 border-b border-slate-800 shrink-0">
        <span className="text-[9px] font-bold uppercase tracking-wider text-slate-700">Ticker</span>
        <span className="text-[9px] font-bold uppercase tracking-wider text-slate-700 text-right">Block</span>
        <span className="text-[9px] font-bold uppercase tracking-wider text-slate-700 text-right">DP%</span>
      </div>

      {/* Rows */}
      <div className="flex-1 overflow-y-auto">
        {error ? (
          <p className="px-3 py-4 text-[10px] text-slate-700 text-center">Backend offline</p>
        ) : prints.length === 0 ? (
          <p className="px-3 py-6 text-[10px] text-slate-700 text-center leading-relaxed">
            No prints yet.<br />Run ingest to populate.
          </p>
        ) : (
          prints.map((p) => (
            <button
              key={`${p.ticker}-${p.print_date}`}
              onClick={() => onSelectTicker?.(p.ticker)}
              className="w-full grid grid-cols-[2fr_1fr_1fr] px-3 py-1.5 hover:bg-slate-800/50 transition-colors border-b border-slate-800/30 text-left"
            >
              <div>
                <span className="text-[11px] font-mono font-bold text-sky-400">{p.ticker}</span>
                {p.name && (
                  <span className="block text-[9px] text-slate-600 truncate max-w-[80px]">{p.name}</span>
                )}
              </div>
              <span className="text-[10px] tabular-nums text-slate-300 text-right self-center">
                {formatDollars(p.dp_dollars)}
              </span>
              <span className={`text-[10px] tabular-nums text-right self-center font-semibold ${
                (p.dp_pct ?? 0) >= 60 ? "text-indigo-400" :
                (p.dp_pct ?? 0) >= 40 ? "text-slate-300" : "text-slate-500"
              }`}>
                {p.dp_pct != null ? `${p.dp_pct.toFixed(1)}%` : "—"}
              </span>
            </button>
          ))
        )}
      </div>
    </div>
  );
}

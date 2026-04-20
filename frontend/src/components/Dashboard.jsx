import { useCallback, useEffect, useState } from "react";
import { api } from "../api/client";
import FileDropZone from "./FileDropZone";
import TickerTable from "./TickerTable";

export default function Dashboard({ onSelectTicker, watchlistTickers = new Set() }) {
  const [signals,     setSignals]     = useState([]);
  const [loading,     setLoading]     = useState(true);
  const [error,       setError]       = useState(null);
  const [weekEnding,  setWeekEnding]  = useState(null);
  const [showUpload,  setShowUpload]  = useState(false);

  const fetchSignals = useCallback(() => {
    setLoading(true);
    return api
      .getSignals({ limit: 200 })
      .then((data) => {
        setSignals(data);
        setWeekEnding(data[0]?.week_ending ?? null);
        setError(null);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { fetchSignals(); }, [fetchSignals]);

  const handleIngestComplete = () => { fetchSignals(); setShowUpload(false); };

  const highSignals = signals.filter((s) => s.score >= 75).length;

  return (
    <div className="flex flex-col gap-3">
      {/* ── Top bar: stats + load-data toggle ── */}
      <div className="flex items-center gap-5 rounded-lg border border-slate-800 bg-slate-900/60 px-4 py-2">
        <Stat label="Week" value={weekEnding ?? "—"} />
        <div className="h-4 w-px bg-slate-800" />
        <Stat label="Signals" value={signals.length || "—"} />
        <div className="h-4 w-px bg-slate-800" />
        <Stat
          label="High"
          value={highSignals || "—"}
          valueClass={highSignals > 0 ? "text-emerald-400" : "text-slate-500"}
        />
        <button
          onClick={() => setShowUpload((v) => !v)}
          className="ml-auto flex items-center gap-1.5 rounded border border-slate-700 bg-slate-800/60 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wider text-slate-400 hover:text-slate-200 hover:border-slate-600 transition-colors"
        >
          <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
          </svg>
          Load Data
        </button>
      </div>

      {/* ── Upload zone (collapsible) ── */}
      {showUpload && (
        <FileDropZone onComplete={handleIngestComplete} />
      )}

      {/* ── Signals table ── */}
      {loading ? (
        <div className="flex h-40 items-center justify-center text-xs text-slate-600">
          Loading signals…
        </div>
      ) : error ? (
        <div className="rounded border border-red-900/40 bg-red-950/30 px-4 py-3 text-xs text-red-400">
          {error} — is the backend running?
        </div>
      ) : (
        <TickerTable
          signals={signals}
          onRowClick={onSelectTicker}
          watchlistTickers={watchlistTickers}
        />
      )}
    </div>
  );
}

function Stat({ label, value, valueClass = "text-white" }) {
  return (
    <div className="flex items-baseline gap-2">
      <span className="text-[10px] font-bold uppercase tracking-widest text-slate-600">{label}</span>
      <span className={`text-sm font-bold tabular-nums ${valueClass}`}>{value}</span>
    </div>
  );
}

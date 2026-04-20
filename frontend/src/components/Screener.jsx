import { useCallback, useEffect, useMemo, useState } from "react";
import { api } from "../api/client";

// ---------------------------------------------------------------------------
// Sector color map
// ---------------------------------------------------------------------------
const SECTOR_STYLES = {
  "Technology":              "bg-indigo-500/15 text-indigo-300 ring-indigo-500/25",
  "Healthcare":              "bg-green-500/15 text-green-300 ring-green-500/25",
  "Financial Services":      "bg-amber-500/15 text-amber-300 ring-amber-500/25",
  "Consumer Cyclical":       "bg-orange-500/15 text-orange-300 ring-orange-500/25",
  "Energy":                  "bg-red-500/15 text-red-300 ring-red-500/25",
  "Utilities":               "bg-teal-500/15 text-teal-300 ring-teal-500/25",
  "Real Estate":             "bg-purple-500/15 text-purple-300 ring-purple-500/25",
  "Communication Services":  "bg-cyan-500/15 text-cyan-300 ring-cyan-500/25",
  "Consumer Defensive":      "bg-lime-500/15 text-lime-300 ring-lime-500/25",
  "Industrials":             "bg-slate-400/15 text-slate-300 ring-slate-400/25",
  "Basic Materials":         "bg-stone-500/15 text-stone-300 ring-stone-500/25",
};

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------
function SectorBadge({ sector }) {
  if (!sector) return <span className="text-slate-600 text-xs">—</span>;
  const cls = SECTOR_STYLES[sector] ?? "bg-slate-700/30 text-slate-400 ring-slate-600/30";
  return (
    <span className={`inline-block rounded-md px-2 py-0.5 text-[10px] font-semibold ring-1 ring-inset ${cls}`}>
      {sector}
    </span>
  );
}

function BiasPill({ bias }) {
  if (!bias || bias === "neutral") return <span className="text-slate-600 text-xs">—</span>;
  if (bias === "long") return (
    <span className="inline-flex items-center gap-1 rounded px-2 py-0.5 text-xs font-bold bg-emerald-500/15 text-emerald-300 ring-1 ring-inset ring-emerald-500/25">
      ↑ Long
    </span>
  );
  return (
    <span className="inline-flex items-center gap-1 rounded px-2 py-0.5 text-xs font-bold bg-red-500/15 text-red-300 ring-1 ring-inset ring-red-500/25">
      ↓ Short
    </span>
  );
}

function dpPctStyle(pct) {
  if (pct == null) return {};
  const alpha = Math.min(pct / 80, 1) * 0.22;
  return { backgroundColor: `rgba(99,102,241,${alpha.toFixed(3)})` };
}

function formatDollars(v) {
  if (v == null) return "—";
  if (v >= 1e9) return `$${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

function formatShares(v) {
  if (v == null) return "—";
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
  return v.toLocaleString();
}

// ---------------------------------------------------------------------------
// Column definitions
// ---------------------------------------------------------------------------
const COLUMNS = [
  { key: "print_date",  label: "Date",      sortable: true,  align: "left"  },
  { key: "ticker",      label: "Ticker",    sortable: true,  align: "left"  },
  { key: "sector",      label: "Sector",    sortable: true,  align: "left"  },
  { key: "dp_pct",      label: "DP %",      sortable: true,  align: "right" },
  { key: "dp_dollars",  label: "$ Block",   sortable: true,  align: "right" },
  { key: "dp_volume",   label: "Shares",    sortable: true,  align: "right" },
  { key: "price_close", label: "Price",     sortable: true,  align: "right" },
  { key: "bias",        label: "Direction", sortable: false, align: "left"  },
];

const DAYS_OPTIONS = [1, 3, 5, 7, 14, 30];

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
export default function Screener({ onSelectTicker }) {
  const [prints,   setPrints]   = useState([]);
  const [loading,  setLoading]  = useState(true);
  const [error,    setError]    = useState(null);

  // Filters
  const [days,      setDays]      = useState(7);
  const [sector,    setSector]    = useState("");
  const [minDpPct,  setMinDpPct]  = useState(0);
  const [minVolDol, setMinVolDol] = useState(0);   // min $ block size (millions)

  // Sort
  const [sortKey, setSortKey] = useState("dp_dollars");
  const [sortDir, setSortDir] = useState("desc");

  const fetchPrints = useCallback(() => {
    setLoading(true);
    api.getPrints({
      days,
      sector:      sector   || undefined,
      min_dp_pct:  minDpPct || undefined,
      limit:       500,
    })
      .then((data) => { setPrints(data); setError(null); })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [days, sector, minDpPct]);

  useEffect(() => { fetchPrints(); }, [fetchPrints]);

  const handleSort = (key) => {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir("desc"); }
  };

  // Client-side filter by $ block size + sort
  const sorted = useMemo(() => {
    const minDollars = minVolDol * 1e6;
    const filtered = prints.filter((p) =>
      minDollars === 0 || (p.dp_dollars ?? 0) >= minDollars
    );
    return [...filtered].sort((a, b) => {
      const va = a[sortKey] ?? (sortDir === "asc" ? "zzz" : "");
      const vb = b[sortKey] ?? (sortDir === "asc" ? "zzz" : "");
      const cmp = typeof va === "string" ? String(va).localeCompare(String(vb)) : va - vb;
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [prints, sortKey, sortDir, minVolDol]);

  // Derive unique sectors from loaded data
  const sectors = useMemo(
    () => [...new Set(prints.map((p) => p.sector).filter(Boolean))].sort(),
    [prints]
  );

  // Summary stats
  const totalDollars = prints.reduce((s, p) => s + (p.dp_dollars ?? 0), 0);

  return (
    <div>
      {/* Summary row */}
      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        {[
          { label: "Prints",         value: sorted.length.toLocaleString() },
          { label: "Total $ Flow",   value: formatDollars(totalDollars) },
          { label: "Unique Tickers", value: new Set(sorted.map((p) => p.ticker)).size.toLocaleString() },
          { label: "Sectors",        value: sectors.length || "—" },
        ].map(({ label, value }) => (
          <div key={label} className="rounded border border-slate-800 bg-slate-900/60 px-4 py-3">
            <p className="text-[10px] font-bold uppercase tracking-widest text-slate-600">{label}</p>
            <p className="mt-1 text-xl font-bold tabular-nums text-white">{value}</p>
          </div>
        ))}
      </div>

      {/* Filters */}
      <div className="mb-3 flex flex-wrap items-center gap-4 rounded border border-slate-800 bg-slate-900/60 px-3 py-2">
        {/* Period */}
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-slate-500">Period</span>
          <div className="flex gap-1">
            {DAYS_OPTIONS.map((d) => (
              <button
                key={d}
                onClick={() => setDays(d)}
                className={`rounded px-2 py-0.5 text-xs font-medium transition-colors ${
                  days === d
                    ? "bg-indigo-600/25 text-indigo-300 ring-1 ring-indigo-500/30"
                    : "text-slate-500 hover:text-slate-300"
                }`}
              >
                {d}d
              </button>
            ))}
          </div>
        </div>

        <div className="h-4 w-px bg-slate-700/60" />

        {/* Sector */}
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-slate-500">Sector</span>
          <select
            value={sector}
            onChange={(e) => setSector(e.target.value)}
            className="rounded-md border border-slate-600 bg-slate-900/60 px-2 py-1 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500"
          >
            <option value="">All sectors</option>
            {sectors.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>

        {/* Min DP % */}
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-slate-500">Min DP %</span>
          <input
            type="number" min={0} max={100} step={5}
            value={minDpPct}
            onChange={(e) => setMinDpPct(Number(e.target.value))}
            className="w-16 rounded-md border border-slate-600 bg-slate-900/60 px-2 py-1 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500"
          />
        </div>

        {/* Min $ block (M) */}
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-slate-500">Min $</span>
          <input
            type="number" min={0} step={1}
            value={minVolDol}
            onChange={(e) => setMinVolDol(Number(e.target.value))}
            className="w-20 rounded-md border border-slate-600 bg-slate-900/60 px-2 py-1 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500"
            placeholder="M"
          />
          <span className="text-xs text-slate-600">M</span>
        </div>

        <span className="ml-auto text-xs text-slate-500">
          {sorted.length.toLocaleString()} result{sorted.length !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Table */}
      {loading ? (
        <div className="flex h-40 items-center justify-center text-xs text-slate-600">
          Loading prints…
        </div>
      ) : error ? (
        <div className="rounded border border-red-900/40 bg-red-950/30 px-4 py-3 text-xs text-red-400">
          {error} — is the backend running?
        </div>
      ) : (
        <div className="overflow-x-auto rounded border border-slate-800">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-slate-800 bg-slate-900">
                {COLUMNS.map((col) => (
                  <th
                    key={col.key}
                    onClick={() => col.sortable && handleSort(col.key)}
                    className={`px-3 py-2 text-[10px] font-bold uppercase tracking-wider text-slate-600 text-${col.align} whitespace-nowrap ${
                      col.sortable
                        ? "cursor-pointer select-none hover:text-slate-400 transition-colors"
                        : ""
                    }`}
                  >
                    {col.label}
                    {sortKey === col.key && (
                      <span className="ml-1 opacity-60">{sortDir === "asc" ? "↑" : "↓"}</span>
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sorted.length === 0 && (
                <tr>
                  <td colSpan={COLUMNS.length} className="px-3 py-12 text-center text-xs text-slate-700">
                    No prints found — run ingest to populate.
                  </td>
                </tr>
              )}
              {sorted.map((p) => (
                <tr
                  key={`${p.ticker}-${p.print_date}`}
                  onClick={() => onSelectTicker?.(p.ticker)}
                  className="border-t border-slate-800/60 cursor-pointer transition-colors hover:bg-slate-800/40"
                >
                  <td className="px-3 py-2 tabular-nums text-slate-600 whitespace-nowrap">
                    {String(p.print_date)}
                  </td>
                  <td className="px-3 py-2">
                    <p className="font-mono font-bold text-sky-400">{p.ticker}</p>
                    {p.name && (
                      <p className="max-w-[120px] truncate text-[9px] text-slate-600">{p.name}</p>
                    )}
                  </td>
                  <td className="px-3 py-2">
                    <SectorBadge sector={p.sector} />
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-slate-300" style={dpPctStyle(p.dp_pct)}>
                    {p.dp_pct != null ? `${p.dp_pct.toFixed(1)}%` : "—"}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums font-semibold text-slate-200">
                    {formatDollars(p.dp_dollars)}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-slate-500">
                    {formatShares(p.dp_volume)}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-slate-300">
                    {p.price_close != null ? `$${p.price_close.toFixed(2)}` : "—"}
                  </td>
                  <td className="px-3 py-2">
                    <BiasPill bias={p.bias} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

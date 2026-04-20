import { useState } from "react";
import { api } from "../api/client";

const STATUS_STYLES = {
  watching: "bg-blue-500/20 text-blue-400 border-blue-500/40",
  entered:  "bg-green-500/20 text-green-400 border-green-500/40",
  closed:   "bg-slate-500/20 text-slate-400 border-slate-500/40",
};

export default function Watchlist({ items, onRefresh, onSelectTicker }) {
  const [editingId, setEditingId]     = useState(null);
  const [editingNotes, setEditingNotes] = useState("");

  const handleStatusChange = async (id, status) => {
    await api.updateWatchlistEntry(id, { status });
    onRefresh?.();
  };

  const startEdit = (entry) => {
    setEditingId(entry.id);
    setEditingNotes(entry.notes ?? "");
  };

  const saveNotes = async (id) => {
    await api.updateWatchlistEntry(id, { notes: editingNotes });
    setEditingId(null);
    onRefresh?.();
  };

  const handleRemove = async (id) => {
    await api.removeFromWatchlist(id);
    onRefresh?.();
  };

  if (items.length === 0) {
    return (
      <div className="rounded-lg border border-slate-700 bg-slate-800 px-6 py-16 text-center">
        <p className="text-slate-400">Your watchlist is empty.</p>
        <p className="mt-1 text-sm text-slate-500">
          Open a signal row and click "Add to Watchlist".
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-700">
      <table className="w-full text-sm">
        <thead className="bg-slate-900 text-slate-400">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium">Ticker</th>
            <th className="px-4 py-2.5 text-left font-medium">Status</th>
            <th className="px-4 py-2.5 text-right font-medium">Entry Price</th>
            <th className="px-4 py-2.5 text-left font-medium">Notes</th>
            <th className="px-4 py-2.5 text-left font-medium">Added</th>
            <th className="px-4 py-2.5 text-center font-medium">Remove</th>
          </tr>
        </thead>
        <tbody>
          {items.map((entry) => (
            <tr
              key={entry.id}
              className="border-t border-slate-700/50 hover:bg-slate-800/40 transition-colors"
            >
              {/* Ticker */}
              <td className="px-4 py-2.5">
                <button
                  onClick={() => onSelectTicker?.(entry.ticker)}
                  className="font-mono font-semibold text-white hover:text-blue-400 transition-colors"
                >
                  {entry.ticker}
                </button>
              </td>

              {/* Status dropdown */}
              <td className="px-4 py-2.5">
                <select
                  value={entry.status}
                  onChange={(e) => handleStatusChange(entry.id, e.target.value)}
                  className={`cursor-pointer rounded border bg-transparent px-2 py-0.5 text-xs font-semibold focus:outline-none ${
                    STATUS_STYLES[entry.status] ?? STATUS_STYLES.watching
                  }`}
                >
                  <option value="watching">WATCHING</option>
                  <option value="entered">ENTERED</option>
                  <option value="closed">CLOSED</option>
                </select>
              </td>

              {/* Entry price */}
              <td className="px-4 py-2.5 text-right tabular-nums text-slate-300">
                {entry.entry_price != null ? `$${entry.entry_price.toFixed(2)}` : "—"}
              </td>

              {/* Inline-editable notes */}
              <td className="px-4 py-2.5 max-w-xs">
                {editingId === entry.id ? (
                  <input
                    autoFocus
                    value={editingNotes}
                    onChange={(e) => setEditingNotes(e.target.value)}
                    onBlur={() => saveNotes(entry.id)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") saveNotes(entry.id);
                      if (e.key === "Escape") setEditingId(null);
                    }}
                    className="w-full rounded border border-slate-600 bg-slate-900 px-2 py-0.5 text-xs text-slate-200 focus:outline-none focus:border-blue-500"
                  />
                ) : (
                  <button
                    onClick={() => startEdit(entry)}
                    title="Click to edit"
                    className="text-left text-xs text-slate-400 hover:text-slate-200 transition-colors"
                  >
                    {entry.notes || (
                      <span className="italic text-slate-600">Add notes…</span>
                    )}
                  </button>
                )}
              </td>

              {/* Added date */}
              <td className="px-4 py-2.5 text-xs tabular-nums text-slate-500">
                {entry.added_date}
              </td>

              {/* Remove */}
              <td className="px-4 py-2.5 text-center">
                <button
                  onClick={() => handleRemove(entry.id)}
                  className="text-xs text-red-400 hover:text-red-300 transition-colors"
                >
                  Remove
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

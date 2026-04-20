import { useEffect, useRef, useState } from "react";
import { api } from "../api/client";

export default function AccountManager({ onClose }) {
  const [accounts, setAccounts] = useState([]);
  const [input, setInput]       = useState("");
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState(null);
  const inputRef = useRef(null);

  const load = () =>
    api.getSentimentAccounts()
      .then(setAccounts)
      .catch(() => setError("Failed to load accounts"));

  useEffect(() => {
    load();
    inputRef.current?.focus();
  }, []);

  const handleAdd = async () => {
    const handle = input.trim().replace(/^@/, "");
    if (!handle) return;
    setLoading(true);
    setError(null);
    try {
      await api.addSentimentAccount(handle);
      setInput("");
      await load();
    } catch {
      setError("Failed to add account");
    } finally {
      setLoading(false);
    }
  };

  const handleRemove = async (handle) => {
    setError(null);
    try {
      await api.removeSentimentAccount(handle);
      await load();
    } catch {
      setError("Failed to remove account");
    }
  };

  const active   = accounts.filter((a) => a.is_active);
  const inactive = accounts.filter((a) => !a.is_active);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div className="w-96 rounded-xl border border-slate-700 bg-[#0d1220] shadow-2xl flex flex-col max-h-[80vh]">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
          <span className="text-sm font-bold text-white">Twitter Accounts</span>
          <button
            onClick={onClose}
            className="text-slate-500 hover:text-slate-300 text-lg leading-none"
          >
            ×
          </button>
        </div>

        {/* Account list */}
        <div className="flex-1 overflow-y-auto px-4 py-3 space-y-1">
          {active.length === 0 && (
            <p className="text-xs text-slate-500 py-2">No active accounts.</p>
          )}
          {active.map((a) => (
            <div
              key={a.id}
              className="flex items-center justify-between rounded-lg bg-slate-800/40 px-3 py-2"
            >
              <span className="text-xs font-mono text-slate-200">@{a.handle}</span>
              <button
                onClick={() => handleRemove(a.handle)}
                className="text-[10px] text-red-400/70 hover:text-red-400 transition-colors"
              >
                remove
              </button>
            </div>
          ))}

          {inactive.length > 0 && (
            <>
              <p className="text-[10px] font-bold uppercase tracking-widest text-slate-600 pt-3 pb-1">
                Inactive
              </p>
              {inactive.map((a) => (
                <div
                  key={a.id}
                  className="flex items-center justify-between rounded-lg bg-slate-800/20 px-3 py-2 opacity-50"
                >
                  <span className="text-xs font-mono text-slate-400 line-through">
                    @{a.handle}
                  </span>
                  <button
                    onClick={() => handleRemove(a.handle)}
                    className="text-[10px] text-sky-400/70 hover:text-sky-400 transition-colors"
                    title="Re-add"
                  >
                    restore
                  </button>
                </div>
              ))}
            </>
          )}
        </div>

        {/* Add handle */}
        <div className="border-t border-slate-800 px-4 py-3">
          {error && (
            <p className="mb-2 text-[11px] text-red-400">{error}</p>
          )}
          <div className="flex gap-2">
            <input
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleAdd()}
              placeholder="@handle or handle"
              className="flex-1 rounded-lg bg-slate-800 px-3 py-1.5 text-xs text-slate-200 placeholder-slate-500 outline-none focus:ring-1 focus:ring-sky-500/50 border border-slate-700"
            />
            <button
              onClick={handleAdd}
              disabled={loading || !input.trim()}
              className="rounded-lg bg-sky-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-sky-500 disabled:opacity-40 transition-colors"
            >
              {loading ? "…" : "Add"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

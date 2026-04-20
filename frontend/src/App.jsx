import { useEffect, useState } from "react";
import { api } from "./api/client";
import Dashboard from "./components/Dashboard";
import PrintsFeed from "./components/PrintsFeed";
import Recommendations from "./components/Recommendations";
import Screener from "./components/Screener";
import TickerDetail from "./components/TickerDetail";
import Watchlist from "./components/Watchlist";

const TABS = ["Signals", "Screener", "Watchlist", "Picks"];

function LogoIcon({ className }) {
  return (
    <svg className={className} viewBox="0 0 16 16" fill="currentColor">
      <rect x="1"   y="9" width="3" height="6" rx="0.75" />
      <rect x="6.5" y="5" width="3" height="10" rx="0.75" />
      <rect x="12"  y="1" width="3" height="14" rx="0.75" opacity="0.65" />
    </svg>
  );
}

export default function App() {
  const [activeTab, setActiveTab]           = useState("Signals");
  const [selectedTicker, setSelectedTicker] = useState(null);
  const [selectedSignal,  setSelectedSignal]  = useState(null);
  const [watchlist, setWatchlist]           = useState([]);

  const refreshWatchlist = () =>
    api.getWatchlist().then(setWatchlist).catch(console.error);

  useEffect(() => { refreshWatchlist(); }, []);

  const watchlistTickers = new Set(watchlist.map((w) => w.ticker));

  const handleSelectTicker = (sigOrTicker) => {
    if (sigOrTicker && typeof sigOrTicker === "object") {
      setSelectedTicker(sigOrTicker.ticker);
      setSelectedSignal(sigOrTicker);
    } else {
      setSelectedTicker(sigOrTicker);
      setSelectedSignal(null);
    }
  };

  return (
    <div className="h-screen bg-[#080c14] text-slate-200 flex flex-col overflow-hidden">
      {/* ── Nav bar ── */}
      <header className="shrink-0 border-b border-slate-800 bg-[#080c14]">
        <div className="flex h-10 items-center gap-5 px-4">
          <div className="flex items-center gap-2">
            <LogoIcon className="h-4 w-4 text-sky-400" />
            <span className="text-sm font-bold tracking-tight text-white">
              Dark Pool <span className="text-sky-400">Tracker</span>
            </span>
          </div>

          <div className="h-4 w-px bg-slate-800" />

          <nav className="flex items-center gap-0.5">
            {TABS.map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`rounded px-3 py-1 text-xs font-semibold transition-colors ${
                  activeTab === tab
                    ? "bg-sky-500/10 text-sky-300 ring-1 ring-inset ring-sky-500/20"
                    : "text-slate-500 hover:text-slate-300"
                }`}
              >
                {tab}
                {tab === "Watchlist" && watchlist.length > 0 && (
                  <span className="ml-1.5 rounded-full bg-sky-500/20 px-1.5 py-0.5 text-[10px] font-normal text-sky-400">
                    {watchlist.length}
                  </span>
                )}
              </button>
            ))}
          </nav>
        </div>
      </header>

      {/* ── Body: main + prints sidebar ── */}
      <div className="flex flex-1 overflow-hidden">
        {/* Main content */}
        <main className="flex-1 overflow-y-auto p-4">
          {activeTab === "Signals" && (
            <Dashboard
              onSelectTicker={handleSelectTicker}
              watchlistTickers={watchlistTickers}
            />
          )}
          {activeTab === "Screener" && (
            <Screener onSelectTicker={handleSelectTicker} />
          )}
          {activeTab === "Watchlist" && (
            <>
              <p className="mb-3 text-[10px] font-bold uppercase tracking-widest text-slate-600">
                Watchlist
              </p>
              <Watchlist
                items={watchlist}
                onRefresh={refreshWatchlist}
                onSelectTicker={(ticker) => {
                  handleSelectTicker(ticker);
                  setActiveTab("Signals");
                }}
              />
            </>
          )}
          {activeTab === "Picks" && (
            <Recommendations onSelectTicker={handleSelectTicker} />
          )}
        </main>

        {/* Right: always-visible prints feed */}
        <aside className="w-52 shrink-0 border-l border-slate-800 overflow-hidden flex flex-col">
          <PrintsFeed onSelectTicker={handleSelectTicker} />
        </aside>
      </div>

      {/* Slide-out detail panel */}
      {selectedTicker && (
        <TickerDetail
          ticker={selectedTicker}
          signal={selectedSignal}
          onClose={() => { setSelectedTicker(null); setSelectedSignal(null); }}
          watchlistEntry={watchlist.find((w) => w.ticker === selectedTicker) ?? null}
          onWatchlistChange={refreshWatchlist}
        />
      )}
    </div>
  );
}

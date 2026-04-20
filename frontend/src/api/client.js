// Empty string → relative URLs → Vite proxy forwards /api/* to the backend.
// Set VITE_API_URL in .env only if you need to override (e.g. a deployed backend).
const BASE_URL = import.meta.env.VITE_API_URL ?? "";

async function request(path, options = {}) {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    throw new Error(`API ${options.method ?? "GET"} ${path} → ${res.status} ${res.statusText}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

export const api = {
  // ----- Signals -----

  /** List top signals. Params: { week, min_score, limit } */
  getSignals: ({ week, min_score, limit } = {}) => {
    const p = new URLSearchParams();
    if (week != null)      p.set("week", week);
    if (min_score != null) p.set("min_score", min_score);
    if (limit != null)     p.set("limit", limit);
    const qs = p.toString();
    return request(`/api/tickers/signals${qs ? `?${qs}` : ""}`);
  },

  /** Ticker autocomplete — returns up to 10 matches. */
  searchTickers: (q) =>
    request(`/api/tickers/search?q=${encodeURIComponent(q)}`),

  /** 12-week dark pool history for a ticker (DpHistoryPoint[]). */
  getTickerHistory: (ticker, weeks = 12) =>
    request(`/api/tickers/${encodeURIComponent(ticker)}/history?weeks=${weeks}`),

  /** 30-day daily OHLCV + has_signal flag (PricePoint[]). */
  getTickerPrice: (ticker, days = 30) =>
    request(`/api/tickers/${encodeURIComponent(ticker)}/price?days=${days}`),

  // ----- Watchlist -----

  /** All watchlist entries ordered by added_date desc. */
  getWatchlist: () => request("/api/watchlist/"),

  /** Add ticker to watchlist (idempotent). */
  addToWatchlist: (ticker, entry_price = null, notes = null) =>
    request("/api/watchlist/", {
      method: "POST",
      body: JSON.stringify({ ticker, entry_price, notes }),
    }),

  /** Partial update: { status?, notes? } */
  updateWatchlistEntry: (id, updates) =>
    request(`/api/watchlist/${id}`, {
      method: "PATCH",
      body: JSON.stringify(updates),
    }),

  /** Remove a watchlist entry by ID. */
  removeFromWatchlist: (id) =>
    request(`/api/watchlist/${id}`, { method: "DELETE" }),

  /**
   * Daily DP prints screener.
   * Params: { days, sector, min_dp_pct, min_volume, limit }
   */
  getPrints: ({ days, sector, min_dp_pct, min_volume, limit } = {}) => {
    const p = new URLSearchParams();
    if (days        != null) p.set("days",        days);
    if (sector      != null) p.set("sector",      sector);
    if (min_dp_pct  != null) p.set("min_dp_pct",  min_dp_pct);
    if (min_volume  != null) p.set("min_volume",  min_volume);
    if (limit       != null) p.set("limit",       limit);
    const qs = p.toString();
    return request(`/api/tickers/prints${qs ? `?${qs}` : ""}`);
  },

  // ----- Ingest -----

  /**
   * Upload a FINRA .txt file. Returns { job_id, status, filename }.
   * Uses FormData — do NOT pass through the regular request() helper
   * since that sets Content-Type: application/json, breaking multipart.
   */
  uploadFinraFile: (formData) =>
    fetch(`${BASE_URL}/api/ingest/upload`, { method: "POST", body: formData })
      .then(async (res) => {
        if (!res.ok) {
          const text = await res.text().catch(() => res.statusText);
          throw new Error(`API POST /api/ingest/upload → ${res.status} ${text}`);
        }
        return res.json();
      }),

  /**
   * Trigger auto-download via Playwright on the local machine.
   * Returns { job_id, status }.
   * Requires `playwright` installed + `python -m playwright install chromium`.
   */
  fetchFromFinra: () =>
    request("/api/ingest/fetch", { method: "POST" }),

  /** Poll pipeline status for a job. Returns stage/progress/done/error. */
  getIngestStatus: (jobId) =>
    request(`/api/ingest/status/${encodeURIComponent(jobId)}`),

  /** List ingested weeks with ticker counts, newest first. */
  getIngestHistory: () =>
    request("/api/ingest/history"),

  // ----- Sentiment -----

  /** List all monitored Twitter accounts. */
  getSentimentAccounts: () =>
    request("/api/sentiment/accounts"),

  /** Add a Twitter handle. Body: { handle: "astocks92" } */
  addSentimentAccount: (handle) =>
    request("/api/sentiment/accounts", {
      method: "POST",
      body: JSON.stringify({ handle }),
    }),

  /** Deactivate a Twitter handle. */
  removeSentimentAccount: (handle) =>
    request(`/api/sentiment/accounts/${encodeURIComponent(handle)}`, {
      method: "DELETE",
    }),

  /** Trigger a full tweet-fetch + sentiment re-score. */
  refreshSentiment: () =>
    request("/api/sentiment/refresh", { method: "POST" }),

  /** Sentiment stats: total tweets, latest tweet time, tickers tracked. */
  getSentimentStatus: () =>
    request("/api/sentiment/status"),

  /** Per-ticker sentiment scores (most recent, sorted by score desc). */
  getTickerSentiments: (limit = 100) =>
    request(`/api/sentiment/tickers?limit=${limit}`),

  // ----- Recommendations -----

  /**
   * Ranked swing-trade picks (60% dark pool + 40% sentiment).
   * Params: { min_score, limit }
   */
  getRecommendations: ({ min_score = 50, limit = 20 } = {}) => {
    const p = new URLSearchParams();
    p.set("min_score", min_score);
    p.set("limit",     limit);
    return request(`/api/recommendations/?${p.toString()}`);
  },
};

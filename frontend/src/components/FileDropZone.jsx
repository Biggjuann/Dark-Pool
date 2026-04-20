import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../api/client";

// ---- Stage metadata ----
// "downloading" only appears for the auto-fetch flow
const UPLOAD_STAGES = ["parsing", "prices", "metadata", "scanning", "alerting", "complete"];
const FETCH_STAGES  = ["downloading", "parsing", "prices", "metadata", "scanning", "alerting", "complete"];

const STAGE_LABELS = {
  downloading: "Downloading",
  parsing:     "Parsing",
  prices:      "Prices",
  metadata:    "Metadata",
  scanning:    "Scanning",
  alerting:    "Alerting",
  complete:    "Complete",
  failed:      "Failed",
};

// ---- Upload icon ----
function UploadIcon({ className }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
      <path strokeLinecap="round" strokeLinejoin="round"
        d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
    </svg>
  );
}

// ---- Pipeline progress bar ----
function PipelineProgress({ stage, stages }) {
  const activeIdx = stages.indexOf(stage);
  return (
    <div className="flex items-center gap-1 flex-wrap">
      {stages.filter((s) => s !== "complete").map((s, i) => {
        const active = s === stage;
        const done   = activeIdx > i || stage === "complete";
        return (
          <span key={s} className="flex items-center gap-1 text-xs">
            {i > 0 && <span className="text-slate-600 select-none">›</span>}
            <span
              className={
                done    ? "text-green-400 font-medium" :
                active  ? "text-blue-400 font-medium animate-pulse" :
                          "text-slate-500"
              }
            >
              {STAGE_LABELS[s]}
            </span>
          </span>
        );
      })}
    </div>
  );
}

export default function FileDropZone({ onComplete }) {
  const [dragging, setDragging] = useState(false);
  const [busy,     setBusy]     = useState(false);
  const [jobId,    setJobId]    = useState(null);
  const [status,   setStatus]   = useState(null);
  const [summary,  setSummary]  = useState(null);
  const [error,    setError]    = useState(null);

  const inputRef = useRef(null);
  const pollRef  = useRef(null);

  // Cleanup polling on unmount
  useEffect(() => () => clearInterval(pollRef.current), []);

  const reset = () => {
    clearInterval(pollRef.current);
    setBusy(false);
    setJobId(null);
    setStatus(null);
    setSummary(null);
    setError(null);
  };

  const startPolling = useCallback((id, _stages) => {
    pollRef.current = setInterval(async () => {
      try {
        const s = await api.getIngestStatus(id);
        setStatus(s);
        if (s.done) {
          clearInterval(pollRef.current);
          setBusy(false);
          if (s.error) {
            setError(s.error);
          } else {
            setSummary(s);
            onComplete?.();
          }
        }
      } catch (err) {
        clearInterval(pollRef.current);
        setError(err.message);
        setBusy(false);
      }
    }, 2000);
  }, [onComplete]);

  // ---- Handle file from drag/drop or browse ----
  const handleFile = useCallback(async (file) => {
    if (!file) return;
    if (!file.name.toLowerCase().endsWith(".txt")) {
      setError("Only .txt files are accepted.");
      return;
    }
    if (file.size > 50 * 1024 * 1024) {
      setError("File exceeds the 50 MB limit.");
      return;
    }

    reset();
    setBusy(true);
    setStatus({ stage: "parsing", progress: 0, done: false, error: null });

    try {
      const formData = new FormData();
      formData.append("file", file);
      const result = await api.uploadFinraFile(formData);
      setJobId(result.job_id);
      startPolling(result.job_id);
    } catch (err) {
      setError(err.message);
      setBusy(false);
    }
  }, [startPolling]);

  const [stages, setStages] = useState(UPLOAD_STAGES);

  const handleFetch = useCallback(async () => {
    reset();
    setBusy(true);
    setStages(FETCH_STAGES);
    setStatus({ stage: "downloading", progress: 0, done: false, error: null });
    try {
      const result = await api.fetchFromFinra();
      setJobId(result.job_id);
      startPolling(result.job_id, FETCH_STAGES);
    } catch (err) {
      setError(err.message);
      setBusy(false);
    }
  }, [startPolling]);

  return (
    <div className="space-y-3">
      {/* ---- Drop zone ---- */}
      <div
        onDrop={(e) => { e.preventDefault(); setDragging(false); handleFile(e.dataTransfer.files?.[0]); }}
        onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onClick={() => !busy && inputRef.current?.click()}
        className={[
          "relative flex flex-col items-center justify-center gap-2",
          "rounded-xl border-2 border-dashed px-6 py-7 text-center transition-colors",
          dragging
            ? "border-blue-400 bg-blue-500/10"
            : "border-slate-600 bg-slate-800/40 hover:border-slate-500 hover:bg-slate-800/70",
          busy ? "cursor-not-allowed opacity-60" : "cursor-pointer",
        ].join(" ")}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".txt"
          className="hidden"
          onChange={(e) => handleFile(e.target.files?.[0])}
        />

        <UploadIcon className="h-7 w-7 text-slate-500" />

        <div>
          <p className="text-sm font-medium text-slate-300">
            {busy ? "Processing…" : "Drop FINRA weekly file here"}
          </p>
          <p className="mt-0.5 text-xs text-slate-500">
            Pipe-delimited .txt · max 50 MB
          </p>
        </div>

        {/* Action buttons */}
        <div className="mt-1 flex flex-wrap justify-center gap-2" onClick={(e) => e.stopPropagation()}>
          <button
            disabled={busy}
            onClick={handleFetch}
            className="rounded border border-slate-600 px-3 py-1 text-xs text-slate-400
                       hover:border-blue-500 hover:text-blue-400 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            Auto-download latest ↓
          </button>
          <a
            href="https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/weekly-short-sale-volume-data"
            target="_blank"
            rel="noopener noreferrer"
            className="rounded border border-slate-600 px-3 py-1 text-xs text-slate-400
                       hover:border-slate-400 hover:text-slate-200 transition-colors"
          >
            Browse FINRA files →
          </a>
        </div>
      </div>

      {/* ---- Pipeline progress ---- */}
      {busy && status && (
        <div className="rounded-lg border border-slate-700 bg-slate-800/60 px-4 py-2.5">
          <PipelineProgress stage={status.stage} stages={stages} />
        </div>
      )}

      {/* ---- Error ---- */}
      {error && (
        <div className="flex items-start justify-between gap-3 rounded-lg border border-red-500/30
                        bg-red-500/10 px-3 py-2 text-xs text-red-400">
          <span className="break-all">{error}</span>
          <button
            onClick={reset}
            className="shrink-0 text-red-400 hover:text-red-300"
            aria-label="Dismiss"
          >
            ✕
          </button>
        </div>
      )}

      {/* ---- Success summary ---- */}
      {summary && !error && (
        <div className="rounded-lg border border-green-500/30 bg-green-500/10 px-4 py-3 text-xs">
          <p className="mb-1.5 font-semibold text-green-400">Ingest complete</p>
          <div className="space-y-0.5 text-slate-300">
            <p>
              Week ending:{" "}
              <span className="font-medium text-white">{summary.week_ending}</span>
            </p>
            <p>
              Tickers processed:{" "}
              <span className="font-medium text-white">{summary.tickers_processed?.toLocaleString()}</span>
            </p>
            <p>
              High-conviction signals (≥75):{" "}
              <span className="font-medium text-white">{summary.top_signals_count}</span>
            </p>
          </div>
          <button
            onClick={reset}
            className="mt-2 text-green-400 hover:text-green-300 transition-colors"
          >
            Upload another file
          </button>
        </div>
      )}
    </div>
  );
}

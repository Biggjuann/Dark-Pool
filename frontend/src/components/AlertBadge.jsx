const LEVEL_CONFIG = {
  high:   { label: "HIGH", dot: "bg-red-400",    className: "bg-red-500/10 text-red-400 ring-1 ring-inset ring-red-500/30" },
  medium: { label: "MED",  dot: "bg-amber-400",  className: "bg-amber-500/10 text-amber-400 ring-1 ring-inset ring-amber-500/30" },
  low:    { label: "LOW",  dot: "bg-slate-500",  className: "bg-slate-700/40 text-slate-400 ring-1 ring-inset ring-slate-600/40" },
};

export default function AlertBadge({ level, score }) {
  const config = LEVEL_CONFIG[level] ?? LEVEL_CONFIG.low;
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-xs font-semibold tracking-wide ${config.className}`}
    >
      <span className={`h-1.5 w-1.5 rounded-full ${config.dot}`} />
      {config.label}
      {score != null && (
        <span className="opacity-60">· {Math.round(score)}</span>
      )}
    </span>
  );
}

"use client";

import { useEffect, useState } from "react";
import { Loader2, CheckCircle2, AlertCircle } from "lucide-react";
import { cn } from "@/lib/utils";

interface PipelineStatus {
  running: boolean;
  ticker: string | null;
  phase: string | null;
  current_doc: string | null;
  docs_total: number;
  docs_done: number;
  started_at: number | null;
  error: string | null;
}

const PHASE_LABELS: Record<string, string> = {
  registering: "Registering PDFs",
  parsing: "Parsing documents",
  normalizing: "Normalizing data",
  loading: "Loading to database",
  done: "Complete",
  error: "Error",
};

export function PipelineProgress({ onComplete }: { onComplete?: () => void }) {
  const [status, setStatus] = useState<PipelineStatus | null>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    let interval: ReturnType<typeof setInterval>;

    const poll = async () => {
      try {
        const res = await fetch("/api/pipeline/status");
        const data: PipelineStatus = await res.json();
        setStatus(data);

        if (data.running || data.phase === "done" || data.phase === "error") {
          setVisible(true);
        }

        if (!data.running && (data.phase === "done" || data.phase === "error")) {
          // Keep visible for 5s after completion, then hide
          setTimeout(() => {
            setVisible(false);
            onComplete?.();
          }, 5000);
        }
      } catch {
        // ignore fetch errors
      }
    };

    poll();
    interval = setInterval(poll, 1500);
    return () => clearInterval(interval);
  }, [onComplete]);

  if (!visible || !status || (!status.running && !status.phase)) return null;

  const pct =
    status.docs_total > 0
      ? Math.round((status.docs_done / status.docs_total) * 100)
      : 0;

  const isDone = status.phase === "done";
  const isError = status.phase === "error";

  return (
    <div
      className={cn(
        "rounded-lg border p-4 space-y-3 transition-all",
        isDone
          ? "border-green-500/30 bg-green-500/5"
          : isError
          ? "border-red-500/30 bg-red-500/5"
          : "border-yellow-500/30 bg-yellow-500/5"
      )}
    >
      <div className="flex items-center gap-2">
        {isDone ? (
          <CheckCircle2 className="h-4 w-4 text-green-400" />
        ) : isError ? (
          <AlertCircle className="h-4 w-4 text-red-400" />
        ) : (
          <Loader2 className="h-4 w-4 animate-spin text-yellow-400" />
        )}
        <span className="text-sm font-medium">
          {PHASE_LABELS[status.phase || ""] || status.phase}
          {status.ticker ? ` — ${status.ticker}` : ""}
        </span>
      </div>

      {/* Progress bar */}
      {status.phase === "parsing" && status.docs_total > 0 && (
        <div className="space-y-1.5">
          <div className="h-2 w-full rounded-full bg-muted overflow-hidden">
            <div
              className="h-full rounded-full bg-yellow-500 transition-all duration-500"
              style={{ width: `${pct}%` }}
            />
          </div>
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>
              {status.docs_done}/{status.docs_total} documents
            </span>
            <span>{pct}%</span>
          </div>
          {status.current_doc && (
            <p className="text-xs text-muted-foreground font-mono truncate">
              {status.current_doc}
            </p>
          )}
        </div>
      )}

      {isError && status.error && (
        <p className="text-xs text-red-400">{status.error}</p>
      )}
    </div>
  );
}

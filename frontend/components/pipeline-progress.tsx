"use client";

import { useEffect, useRef, useState, useCallback } from "react";
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
  failed_count?: number;
}

const PHASE_LABELS: Record<string, string> = {
  registering: "Registering PDFs",
  parsing: "Parsing documents",
  normalizing: "Normalizing data",
  loading: "Loading to database",
  done: "Complete",
  done_with_errors: "Complete with failures",
  error: "Error",
};

const TERMINAL_PHASES = new Set(["done", "done_with_errors", "error"]);

export function PipelineProgress({ onComplete }: { onComplete?: () => void }) {
  const [status, setStatus] = useState<PipelineStatus | null>(null);
  const [visible, setVisible] = useState(false);
  const onCompleteRef = useRef(onComplete);
  onCompleteRef.current = onComplete;
  const hideTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const didFinishRef = useRef(false);

  useEffect(() => {
    const poll = async () => {
      try {
        const res = await fetch("/api/pipeline/status");
        const data: PipelineStatus = await res.json();
        setStatus(data);

        if (data.running) {
          // Pipeline is active — show and reset finish flag
          setVisible(true);
          didFinishRef.current = false;
          if (hideTimerRef.current) {
            clearTimeout(hideTimerRef.current);
            hideTimerRef.current = null;
          }
        } else if (data.phase && TERMINAL_PHASES.has(data.phase) && !didFinishRef.current) {
          // Just finished — show result, schedule hide once
          didFinishRef.current = true;
          setVisible(true);
          if (hideTimerRef.current) clearTimeout(hideTimerRef.current);
          hideTimerRef.current = setTimeout(() => {
            setVisible(false);
            onCompleteRef.current?.();
            hideTimerRef.current = null;
          }, 5000);
        }
      } catch {
        // ignore fetch errors
      }
    };

    poll();
    const interval = setInterval(poll, 1500);
    return () => {
      clearInterval(interval);
      if (hideTimerRef.current) clearTimeout(hideTimerRef.current);
    };
  }, []); // no deps — refs handle callbacks

  if (!visible || !status) return null;

  const pct =
    status.docs_total > 0
      ? Math.round((status.docs_done / status.docs_total) * 100)
      : 0;

  const isDone = status.phase === "done";
  const isDoneWithErrors = status.phase === "done_with_errors";
  const isError = status.phase === "error";

  return (
    <div
      className={cn(
        "rounded-lg border p-4 space-y-3 transition-all",
        isDone
          ? "border-green-500/30 bg-green-500/5"
          : isDoneWithErrors
          ? "border-amber-500/30 bg-amber-500/5"
          : isError
          ? "border-red-500/30 bg-red-500/5"
          : "border-yellow-500/30 bg-yellow-500/5"
      )}
    >
      <div className="flex items-center gap-2">
        {isDone ? (
          <CheckCircle2 className="h-4 w-4 text-green-400" />
        ) : isDoneWithErrors ? (
          <AlertCircle className="h-4 w-4 text-amber-400" />
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

      {/* Progress bar — show during parsing and all later active phases */}
      {status.running && status.docs_total > 0 && (
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

      {isDoneWithErrors && status.failed_count != null && status.failed_count > 0 && (
        <p className="text-xs text-amber-400">
          {status.failed_count} document{status.failed_count > 1 ? "s" : ""} failed to parse
        </p>
      )}

      {isError && status.error && (
        <p className="text-xs text-red-400">{status.error}</p>
      )}
    </div>
  );
}

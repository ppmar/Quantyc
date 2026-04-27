"use client";

import { useEffect, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

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
  registering: "Registering",
  fetching: "Fetching",
  parsing: "Parsing",
  normalizing: "Normalizing",
  loading: "Loading",
  done: "Complete",
  done_with_errors: "Complete with errors",
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
        const res = await fetch(`${API_BASE}/api/pipeline/status`);
        const data: PipelineStatus = await res.json();
        setStatus(data);

        if (data.running) {
          setVisible(true);
          didFinishRef.current = false;
          if (hideTimerRef.current) {
            clearTimeout(hideTimerRef.current);
            hideTimerRef.current = null;
          }
        } else if (data.phase && TERMINAL_PHASES.has(data.phase) && !didFinishRef.current) {
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
        // ignore
      }
    };

    poll();
    const interval = setInterval(poll, 1500);
    return () => {
      clearInterval(interval);
      if (hideTimerRef.current) clearTimeout(hideTimerRef.current);
    };
  }, []);

  if (!visible || !status) return null;

  const pct =
    status.docs_total > 0
      ? Math.round((status.docs_done / status.docs_total) * 100)
      : 0;

  const isDone = status.phase === "done";
  const isError = status.phase === "error";

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3">
        {!isDone && !isError && (
          <div className="h-1.5 w-1.5 rounded-full bg-amber animate-pulse" />
        )}
        <span className="text-[13px] text-zinc-400">
          {PHASE_LABELS[status.phase || ""] || status.phase}
          {status.ticker ? ` \u2014 ${status.ticker}` : ""}
        </span>
        {status.running && status.docs_total > 0 && (
          <span className="text-[12px] font-mono text-zinc-600">
            {status.docs_done}/{status.docs_total}
          </span>
        )}
      </div>

      {status.running && status.docs_total > 0 && (
        <div className="h-px w-full bg-zinc-800 overflow-hidden">
          <div
            className="h-full bg-amber/60 transition-all duration-500"
            style={{ width: `${pct}%` }}
          />
        </div>
      )}

      {isError && status.error && (
        <p className="text-[12px] text-red-400">{status.error}</p>
      )}
    </div>
  );
}

"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Download, Play, RefreshCw } from "lucide-react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";
const DEFAULT_SCAN_COUNT = 200;

interface ScheduleInfo {
  enabled: boolean;
  interval_hours: number;
  next_run: string | null;
  tickers: string[];
}

export function IngestPanel() {
  const [ticker, setTicker] = useState("");
  const count = DEFAULT_SCAN_COUNT;
  const [loading, setLoading] = useState(false);
  const [runAllLoading, setRunAllLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [showPanel, setShowPanel] = useState(false);
  const [schedule, setSchedule] = useState<ScheduleInfo | null>(null);

  useEffect(() => {
    fetch(`${API_BASE}/api/schedule`)
      .then((r) => r.json())
      .then(setSchedule)
      .catch(() => {});
  }, []);

  const handleIngest = async () => {
    const tickers = ticker
      .toUpperCase()
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
    if (tickers.length === 0) return;

    setLoading(true);
    setMessage(null);
    try {
      const res = await fetch(`${API_BASE}/api/ingest`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tickers, count }),
      });
      const data = await res.json();
      if (data.error) {
        setMessage(data.error);
      } else {
        setMessage(`Scanning announcements for ${tickers.join(", ")}…`);
        setTicker("");
      }
    } catch {
      setMessage("Failed to start ingest");
    } finally {
      setLoading(false);
    }
  };

  const handleRunAll = async () => {
    setRunAllLoading(true);
    setMessage(null);
    try {
      const res = await fetch(`${API_BASE}/api/schedule/run`, {
        method: "POST",
      });
      const data = await res.json();
      if (data.error) {
        setMessage(data.error);
      } else {
        const tickers = schedule?.tickers?.join(", ") || "all pilot tickers";
        setMessage(`Ingest started for ${tickers}`);
      }
    } catch {
      setMessage("Failed to start ingest");
    } finally {
      setRunAllLoading(false);
    }
  };

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-medium flex items-center gap-1.5">
            <Download className="h-4 w-4" />
            ASX Ingest
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button
              onClick={handleRunAll}
              disabled={runAllLoading || loading}
              size="sm"
              variant="outline"
              className="h-7 text-xs gap-1"
            >
              {runAllLoading ? (
                <RefreshCw className="h-3 w-3 animate-spin" />
              ) : (
                <Play className="h-3 w-3" />
              )}
              Run All
            </Button>
            <button
              onClick={() => setShowPanel(!showPanel)}
              className="text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              {showPanel ? "Hide" : "Show"}
            </button>
          </div>
        </div>
      </CardHeader>
      {showPanel && (
        <CardContent className="space-y-3">
          <div className="flex gap-2">
            <input
              type="text"
              value={ticker}
              onChange={(e) => setTicker(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && !loading && handleIngest()}
              placeholder="Ticker(s), e.g. SX2, RMS"
              className="flex-1 h-8 rounded-md border border-border bg-background px-3 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            />
            <Button
              onClick={handleIngest}
              disabled={loading || !ticker.trim()}
              size="sm"
            >
              {loading ? "Starting..." : "Fetch"}
            </Button>
          </div>
          {schedule && (
            <div className="text-xs text-muted-foreground space-y-0.5">
              <p>
                Auto-ingest: {schedule.enabled ? "on" : "off"} — every{" "}
                {schedule.interval_hours}h
              </p>
              <p>Tickers: {schedule.tickers.join(", ") || "none"}</p>
              {schedule.next_run && (
                <p>Next run: {new Date(schedule.next_run).toLocaleString()}</p>
              )}
            </div>
          )}
          {message && (
            <p className="text-xs text-yellow-400">{message}</p>
          )}
        </CardContent>
      )}
    </Card>
  );
}

"use client";

import { useState } from "react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Download } from "lucide-react";

const COUNT_OPTIONS = [5, 10, 20, 50];

export function IngestPanel() {
  const [ticker, setTicker] = useState("");
  const [count, setCount] = useState(10);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [showPanel, setShowPanel] = useState(false);

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
      const res = await api.ingest(tickers, count);
      if (res.error) {
        setMessage(res.error);
      } else {
        setMessage(`Ingest started for ${tickers.join(", ")} (last ${count})`);
        setTicker("");
      }
    } catch {
      setMessage("Failed to start ingest");
    } finally {
      setLoading(false);
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
          <button
            onClick={() => setShowPanel(!showPanel)}
            className="text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            {showPanel ? "Hide" : "Show"}
          </button>
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
            <select
              value={count}
              onChange={(e) => setCount(Number(e.target.value))}
              className="h-8 rounded-md border border-border bg-background px-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
            >
              {COUNT_OPTIONS.map((n) => (
                <option key={n} value={n}>
                  Last {n}
                </option>
              ))}
            </select>
            <Button
              onClick={handleIngest}
              disabled={loading || !ticker.trim()}
              size="sm"
            >
              {loading ? "Starting..." : "Fetch"}
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">
            Fetches announcements from ASX, downloads PDFs into memory, and
            parses relevant documents. Free API returns up to 5 most recent.
          </p>
          {message && (
            <p className="text-xs text-yellow-400">{message}</p>
          )}
        </CardContent>
      )}
    </Card>
  );
}

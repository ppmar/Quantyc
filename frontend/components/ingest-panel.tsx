"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Download, Play, RefreshCw } from "lucide-react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";
const DEFAULT_SCAN_COUNT = 200;
const DEFAULT_TICKERS = [
  // Majors & mid-tier
  "BHP", "RIO", "FMG", "S32", "NST", "EVN", "NEM", "RRL", "PRU", "RMS",
  "GMD", "SLR", "CMM", "WGX", "RED", "SBM", "GOR", "DEG", "BGL", "PNR",
  "SXG", "WAF", "CAI", "DEV", "IGO", "MIN",
  // Lithium
  "LYC", "PLS", "CXO", "LTR", "SYR", "GLN", "INR", "AZS", "LKE", "LPD",
  "LPI", "LTM", "A11", "ASN", "AZL", "CHR", "CTR", "DLI", "ESS", "EV1",
  "FFX", "GL1", "GT1", "JRL", "LIT", "PL3", "PMT", "SYA", "TLG", "ZEU",
  // Base metals
  "OZL", "SFR", "AIS", "HGO", "AVM", "HCH", "29M", "C29", "CUL", "CUX",
  "FEX", "HAV", "IVR", "MMG", "NIC", "PM8", "QPM", "STM", "STK", "TMR", "XRF",
  // Gold juniors
  "AR3", "CHN", "CNJ", "CYM", "ERM", "FDM", "HMY",
  // Uranium
  "PDN", "DYL", "LOT", "BMN", "PEN", "TOE", "ALX", "AGE", "92E", "EL8",
  "AEE", "BKY", "ERA", "SLX", "VMY",
  // Mineral sands & rare earths
  "ILU", "ARU", "HAS", "NTU", "PEK", "ASM", "KRM",
  // Nickel & battery metals
  "PAN", "AR2", "POS", "AUZ", "NWC", "JMS", "BMM", "EMN", "TMT",
  // Iron ore
  "GRR", "MGX", "RHI", "MGT", "CIA", "KZR", "RMC",
  // Diversified / other
  "BOE", "WR1", "NVA", "AGY", "MCR", "VUL", "CEL",
  "NEW", "CLQ", "COB", "MNS", "GRE", "SVM", "AUT", "EQX", "MLX", "VMS",
  "ELT", "RXL", "STG", "GWR", "NXM", "MM8", "ASO", "AQI", "BMO", "GED",
  "KSN", "DTM", "BGD", "FG1", "KAU", "LM8", "PGO", "RKB", "TG1", "VRC",
  "MEG", "A1M", "RMX", "AUC", "EMR", "PUR", "KCN", "AZY", "FAU", "BTR",
  "KAL", "BTN", "TBR", "TIE", "PDI", "RSG", "SPR", "TAM", "WGC", "BCN",
  "OKR", "TRM", "SVY", "POL",
  // International (TSX / NYSE)
  "AEM", "ABX", "GOLD", "BTG", "KGC", "NGD", "WPM", "FNV", "ELD", "IMG",
  "OR", "CG", "AR", "LUG", "CS", "FM", "TECK", "HBM", "IVN", "TKO",
  "NFG", "MAG", "SSRM", "PAAS", "OGC", "OSK", "SEA", "TFPM", "TGZ", "TXG",
  "K", "LGD", "LNR", "OGN", "STLR", "ARTG", "AMM", "SIL", "SSL", "USGD",
  "VIT", "FOM", "GCM", "KNT", "MOX", "NG", "P", "PRB", "RNX", "ABRA",
];

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
      const res = await fetch(`${API_BASE}/api/ingest`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tickers: DEFAULT_TICKERS, count: DEFAULT_SCAN_COUNT }),
      });
      const data = await res.json();
      if (data.error) {
        setMessage(data.error);
      } else {
        setMessage(`Scanning ${DEFAULT_TICKERS.length} tickers: ${DEFAULT_TICKERS.join(", ")}…`);
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
              Fetch All ({DEFAULT_TICKERS.length})
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

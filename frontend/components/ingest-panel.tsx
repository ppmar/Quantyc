"use client";

import { useEffect, useState } from "react";

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

export function IngestPanel() {
  const [ticker, setTicker] = useState("");
  const [loading, setLoading] = useState(false);
  const [runAllLoading, setRunAllLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);

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
        body: JSON.stringify({ tickers, count: DEFAULT_SCAN_COUNT }),
      });
      const data = await res.json();
      if (data.error) {
        setMessage(data.error);
      } else {
        setMessage(`Scanning ${tickers.join(", ")}...`);
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
        setMessage(`Scanning ${DEFAULT_TICKERS.length} tickers...`);
      }
    } catch {
      setMessage("Failed to start ingest");
    } finally {
      setRunAllLoading(false);
    }
  };

  return (
    <div>
      <div className="flex items-center gap-3">
        <button
          onClick={handleRunAll}
          disabled={runAllLoading || loading}
          className="px-3 py-1.5 text-[13px] font-medium text-zinc-300 bg-white/[0.04] hover:bg-white/[0.08] border border-border rounded-sm transition-colors disabled:opacity-40"
        >
          {runAllLoading ? "Scanning..." : `Fetch All (${DEFAULT_TICKERS.length})`}
        </button>
        <button
          onClick={() => setExpanded(!expanded)}
          className="text-[13px] text-zinc-600 hover:text-zinc-400 transition-colors"
        >
          {expanded ? "Hide" : "Custom"}
        </button>
        {message && (
          <p className="text-[12px] text-amber">{message}</p>
        )}
      </div>

      {expanded && (
        <div className="flex gap-2 mt-3">
          <input
            type="text"
            value={ticker}
            onChange={(e) => setTicker(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && !loading && handleIngest()}
            placeholder="Ticker(s), e.g. DEG, RMS"
            className="flex-1 h-8 rounded-sm border border-border bg-transparent px-3 text-[13px] text-zinc-200 placeholder:text-zinc-700 focus:outline-none focus:border-zinc-600 transition-colors"
          />
          <button
            onClick={handleIngest}
            disabled={loading || !ticker.trim()}
            className="px-3 h-8 text-[13px] font-medium text-zinc-300 bg-white/[0.04] hover:bg-white/[0.08] border border-border rounded-sm transition-colors disabled:opacity-40"
          >
            {loading ? "..." : "Fetch"}
          </button>
        </div>
      )}
    </div>
  );
}

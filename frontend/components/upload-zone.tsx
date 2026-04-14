"use client";

import { useState, useCallback, useRef } from "react";
import { Upload, FileText, CheckCircle2, Loader2, X } from "lucide-react";
import { cn } from "@/lib/utils";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

const DOC_TYPES = [
  { value: "auto", label: "Auto-detect" },
  { value: "appendix_5b", label: "Appendix 5B" },
  { value: "issue_of_securities", label: "Issue of Securities" },
  { value: "quarterly_activity", label: "Quarterly Activity" },
  { value: "annual_report", label: "Annual Report" },
  { value: "half_year_report", label: "Half Year Report" },
  { value: "resource_update", label: "Resource Update" },
  { value: "placement", label: "Placement / Capital Raise" },
  { value: "presentation", label: "Presentation" },
];

interface UploadResult {
  filename: string;
  document_id: number;
  doc_type: string;
}

interface UploadZoneProps {
  onComplete?: () => void;
}

export function UploadZone({ onComplete }: UploadZoneProps) {
  const [dragOver, setDragOver] = useState(false);
  const [ticker, setTicker] = useState("");
  const [docType, setDocType] = useState("auto");
  const [files, setFiles] = useState<File[]>([]);
  const [uploading, setUploading] = useState(false);
  const [results, setResults] = useState<UploadResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const dropped = Array.from(e.dataTransfer.files).filter((f) =>
      f.name.toLowerCase().endsWith(".pdf")
    );
    if (dropped.length) setFiles((prev) => [...prev, ...dropped]);
  }, []);

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = Array.from(e.target.files || []).filter((f) =>
      f.name.toLowerCase().endsWith(".pdf")
    );
    if (selected.length) setFiles((prev) => [...prev, ...selected]);
  };

  const removeFile = (idx: number) => {
    setFiles((prev) => prev.filter((_, i) => i !== idx));
  };

  const handleUpload = async () => {
    if (!ticker.trim() || files.length === 0) return;
    setUploading(true);
    setError(null);
    setResults(null);

    const formData = new FormData();
    formData.append("ticker", ticker.trim().toUpperCase());
    if (docType !== "auto") formData.append("doc_type", docType);
    files.forEach((f) => formData.append("files", f));

    try {
      const res = await fetch(`${API_BASE}/api/upload`, {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || "Upload failed");
      } else {
        setResults(data.files);
        setFiles([]);
        setTimeout(() => onComplete?.(), 2000);
      }
    } catch {
      setError("Connection failed");
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        {/* Ticker input */}
        <div>
          <label className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Company Ticker
          </label>
          <input
            type="text"
            value={ticker}
            onChange={(e) => setTicker(e.target.value.toUpperCase())}
            placeholder="e.g. DEG, SXG, BGL"
            className="mt-1 w-full rounded-md border border-border bg-card px-3 py-2 text-sm font-mono text-foreground placeholder:text-muted-foreground/50 focus:outline-none focus:ring-2 focus:ring-primary/50"
          />
        </div>

        {/* Doc type selector */}
        <div>
          <label className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Document Type
          </label>
          <select
            value={docType}
            onChange={(e) => setDocType(e.target.value)}
            className="mt-1 w-full rounded-md border border-border bg-card px-3 py-2 text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-primary/50"
          >
            {DOC_TYPES.map((t) => (
              <option key={t.value} value={t.value}>
                {t.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Drop zone */}
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragLeave={() => setDragOver(false)}
        onDrop={handleDrop}
        onClick={() => inputRef.current?.click()}
        className={cn(
          "relative cursor-pointer rounded-lg border-2 border-dashed p-8 text-center transition-all",
          dragOver
            ? "border-primary bg-primary/5 scale-[1.01]"
            : "border-border/50 hover:border-primary/40 hover:bg-muted/30"
        )}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".pdf"
          multiple
          onChange={handleFileSelect}
          className="hidden"
        />
        <Upload
          className={cn(
            "mx-auto h-10 w-10 mb-3",
            dragOver ? "text-primary" : "text-muted-foreground/40"
          )}
        />
        <p className="text-sm font-medium text-foreground">
          Drop PDF files here or click to browse
        </p>
        <p className="text-xs text-muted-foreground mt-1">
          Appendix 5B, resource updates, studies, capital raises, drill results
        </p>
      </div>

      {/* File list */}
      {files.length > 0 && (
        <div className="space-y-1.5">
          {files.map((f, i) => (
            <div
              key={i}
              className="flex items-center justify-between rounded-md bg-muted/30 px-3 py-2"
            >
              <div className="flex items-center gap-2 text-sm min-w-0">
                <FileText className="h-4 w-4 text-primary shrink-0" />
                <span className="truncate">{f.name}</span>
                <span className="text-xs text-muted-foreground shrink-0">
                  {(f.size / 1024).toFixed(0)} KB
                </span>
              </div>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  removeFile(i);
                }}
                className="text-muted-foreground hover:text-destructive p-1"
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Upload button */}
      {files.length > 0 && (
        <button
          onClick={handleUpload}
          disabled={uploading || !ticker.trim()}
          className={cn(
            "w-full rounded-md bg-primary px-4 py-2.5 text-sm font-semibold text-primary-foreground transition-all hover:bg-primary/90",
            (uploading || !ticker.trim()) && "opacity-50 cursor-not-allowed"
          )}
        >
          {uploading ? (
            <span className="inline-flex items-center gap-2">
              <Loader2 className="h-4 w-4 animate-spin" /> Processing...
            </span>
          ) : (
            `Upload & Analyse ${files.length} file${files.length > 1 ? "s" : ""} for ${ticker || "..."}`
          )}
        </button>
      )}

      {/* Results */}
      {results && (
        <div className="rounded-md bg-green-500/10 border border-green-500/20 p-3 space-y-1.5">
          <div className="flex items-center gap-1.5 text-sm font-medium text-green-400">
            <CheckCircle2 className="h-4 w-4" />
            {results.length} file{results.length > 1 ? "s" : ""} uploaded — pipeline running
          </div>
          {results.map((r) => (
            <div key={r.document_id} className="text-xs text-muted-foreground flex items-center gap-2">
              <span className="font-mono">{r.filename}</span>
              <span className="rounded bg-muted px-1.5 py-0.5 text-[10px]">
                {r.doc_type}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="rounded-md bg-destructive/10 border border-destructive/20 p-3 text-sm text-destructive">
          {error}
        </div>
      )}
    </div>
  );
}

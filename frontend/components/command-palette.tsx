"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { api, type Company } from "@/lib/api";

export function CommandPalette() {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [companies, setCompanies] = useState<Company[]>([]);
  const [highlight, setHighlight] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const loadedRef = useRef(false);

  // Global shortcut: ⌘K / Ctrl+K to open, Escape to close.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen((o) => !o);
      } else if (e.key === "Escape") {
        setOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Lazy-load the ticker list on first open.
  useEffect(() => {
    if (open && !loadedRef.current) {
      loadedRef.current = true;
      api.companies().then(setCompanies).catch(() => {});
    }
    if (open) {
      setQuery("");
      setHighlight(0);
      setTimeout(() => inputRef.current?.focus(), 10);
    }
  }, [open]);

  const results = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return companies.slice(0, 8);
    const starts = companies.filter((c) =>
      c.ticker.toLowerCase().startsWith(q)
    );
    const contains = companies.filter(
      (c) =>
        !c.ticker.toLowerCase().startsWith(q) &&
        (c.ticker.toLowerCase().includes(q) ||
          (c.name ?? "").toLowerCase().includes(q))
    );
    return [...starts, ...contains].slice(0, 8);
  }, [companies, query]);

  const go = useCallback(
    (ticker: string) => {
      setOpen(false);
      router.push(`/company/${ticker}`);
    },
    [router]
  );

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[90] flex items-start justify-center bg-black/55 backdrop-blur-[2px] pt-[18vh]"
      onClick={() => setOpen(false)}
    >
      <div
        className="q-card q-card-hero w-full max-w-md animate-expand-in overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2.5 border-b border-border px-4 py-3">
          <svg
            width="14"
            height="14"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            className="text-zinc-600 shrink-0"
          >
            <circle cx="11" cy="11" r="7" />
            <path d="m21 21-4.3-4.3" />
          </svg>
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setHighlight(0);
            }}
            onKeyDown={(e) => {
              if (e.key === "ArrowDown") {
                e.preventDefault();
                setHighlight((h) => Math.min(h + 1, results.length - 1));
              } else if (e.key === "ArrowUp") {
                e.preventDefault();
                setHighlight((h) => Math.max(h - 1, 0));
              } else if (e.key === "Enter" && results[highlight]) {
                go(results[highlight].ticker);
              }
            }}
            placeholder="Jump to ticker…"
            className="w-full bg-transparent text-[14px] text-zinc-100 placeholder:text-zinc-600 focus:outline-none"
          />
          <kbd className="rounded border border-white/[0.08] px-1.5 py-0.5 text-[10px] font-mono text-zinc-600">
            esc
          </kbd>
        </div>

        <div className="max-h-72 overflow-y-auto py-1.5">
          {results.length === 0 && (
            <p className="px-4 py-6 text-center text-[13px] text-zinc-600">
              No match.
            </p>
          )}
          {results.map((c, i) => (
            <button
              key={c.ticker}
              type="button"
              onMouseEnter={() => setHighlight(i)}
              onClick={() => go(c.ticker)}
              className={`flex w-full items-center gap-3 px-4 py-2 text-left transition-colors ${
                i === highlight ? "bg-amber/10" : ""
              }`}
            >
              <span
                className={`font-mono text-[13px] font-medium w-12 shrink-0 ${
                  i === highlight ? "text-amber" : "text-amber/80"
                }`}
              >
                {c.ticker}
              </span>
              <span className="truncate text-[13px] text-zinc-400">
                {c.name || "—"}
              </span>
              {i === highlight && (
                <span className="ml-auto text-[10px] font-mono text-zinc-600">
                  ↵
                </span>
              )}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

/** Small hint button for the sidebar/topbar. */
export function CommandPaletteHint() {
  return (
    <button
      type="button"
      onClick={() =>
        window.dispatchEvent(
          new KeyboardEvent("keydown", { key: "k", metaKey: true })
        )
      }
      className="q-control flex w-full items-center gap-2 text-zinc-600 hover:text-zinc-400"
    >
      <span className="text-[12px]">Search tickers</span>
      <kbd className="ml-auto rounded border border-white/[0.08] px-1.5 py-0.5 text-[10px] font-mono">
        ⌘K
      </kbd>
    </button>
  );
}

"use client";

import type { TabVisibility } from "@/types/snapshot";

const TAB_DEFS: { key: keyof TabVisibility; label: string }[] = [
  { key: "summary", label: "Summary" },
  { key: "financials", label: "Financials" },
  { key: "capital", label: "Capital" },
  { key: "operations", label: "Operations" },
  { key: "documents", label: "Documents" },
  { key: "holders", label: "Holders" },
];

export function TabBar({
  tabs,
  active,
  onChange,
}: {
  tabs: TabVisibility;
  active: string;
  onChange: (tab: string) => void;
}) {
  const visible = TAB_DEFS.filter((t) => tabs[t.key]);

  return (
    <div className="flex gap-6 border-b border-white/[0.06]">
      {visible.map((tab) => (
        <button
          key={tab.key}
          onClick={() => onChange(tab.key)}
          className={`pb-2.5 text-sm font-medium transition-colors relative ${
            active === tab.key
              ? "text-zinc-100"
              : "text-zinc-500 hover:text-zinc-300"
          }`}
        >
          {tab.label}
          {active === tab.key && (
            <span className="absolute bottom-0 left-0 right-0 h-px bg-zinc-100" />
          )}
        </button>
      ))}
    </div>
  );
}

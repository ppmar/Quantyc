"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutGrid,
  Building2,
  FileText,
  Activity,
  ShieldAlert,
  Gem,
} from "lucide-react";
import { CommandPaletteHint } from "@/components/command-palette";

const sections: {
  title: string;
  links: { href: string; label: string; icon: React.ElementType }[];
}[] = [
  {
    title: "Intelligence",
    links: [
      { href: "/", label: "Overview", icon: LayoutGrid },
      { href: "/companies", label: "Companies", icon: Building2 },
      { href: "/valuations", label: "Valuations", icon: Gem },
    ],
  },
  {
    title: "Pipeline",
    links: [
      { href: "/documents", label: "Documents", icon: FileText },
      { href: "/review", label: "Review", icon: ShieldAlert },
      { href: "/health", label: "Health", icon: Activity },
    ],
  },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed inset-y-0 left-0 z-40 hidden w-[210px] flex-col border-r border-border bg-background/70 backdrop-blur-md lg:flex">
      {/* Wordmark */}
      <div className="flex h-14 items-center px-5">
        <Link href="/" className="flex items-baseline gap-1.5">
          <span className="text-[14px] font-semibold tracking-[0.22em] text-amber">
            QUANTYC
          </span>
        </Link>
      </div>

      {/* Search */}
      <div className="px-3 pb-1">
        <CommandPaletteHint />
      </div>

      {/* Nav */}
      <nav className="flex-1 space-y-6 px-3 pt-4">
        {sections.map((s) => (
          <div key={s.title}>
            <p className="q-label px-2.5 mb-1.5">{s.title}</p>
            <div className="space-y-0.5">
              {s.links.map(({ href, label, icon: Icon }) => {
                const active =
                  href === "/" ? pathname === "/" : pathname.startsWith(href);
                return (
                  <Link
                    key={href}
                    href={href}
                    data-active={active}
                    className="q-navlink"
                  >
                    <Icon size={14} strokeWidth={1.8} className="shrink-0" />
                    {label}
                  </Link>
                );
              })}
            </div>
          </div>
        ))}
      </nav>

      {/* Footer status */}
      <div className="border-t border-border px-5 py-4">
        <div className="flex items-center gap-2">
          <span className="q-live-dot" />
          <span className="text-[11px] text-zinc-500">Live · ASX juniors</span>
        </div>
        <p className="mt-1 text-[10px] font-mono text-zinc-700">
          mining intelligence terminal
        </p>
      </div>
    </aside>
  );
}

/** Compact top bar for small screens (sidebar hidden below lg). */
export function MobileNav() {
  const pathname = usePathname();
  const links = sections.flatMap((s) => s.links);
  return (
    <nav className="sticky top-0 z-40 flex h-12 items-center gap-1 overflow-x-auto border-b border-border bg-background/85 px-4 backdrop-blur-md lg:hidden">
      <span className="mr-3 text-[12px] font-semibold tracking-[0.2em] text-amber">
        QUANTYC
      </span>
      {links.map(({ href, label }) => {
        const active =
          href === "/" ? pathname === "/" : pathname.startsWith(href);
        return (
          <Link
            key={href}
            href={href}
            className={`whitespace-nowrap rounded-md px-2.5 py-1.5 text-[12px] transition-colors ${
              active ? "text-amber bg-amber/10" : "text-zinc-500"
            }`}
          >
            {label}
          </Link>
        );
      })}
    </nav>
  );
}

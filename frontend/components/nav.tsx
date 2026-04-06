"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Building2,
  FileText,
  TrendingUp,
  AlertTriangle,
  Play,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useState } from "react";
import { api } from "@/lib/api";

const links = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/companies", label: "Companies", icon: Building2 },
  { href: "/documents", label: "Documents", icon: FileText },
  { href: "/valuations", label: "Valuations", icon: TrendingUp },
  { href: "/review", label: "Review", icon: AlertTriangle },
];

export function Nav() {
  const pathname = usePathname();
  const [parsing, setParsing] = useState(false);

  async function handleParse() {
    setParsing(true);
    try {
      await api.parse();
      setTimeout(() => setParsing(false), 3000);
    } catch {
      setParsing(false);
    }
  }

  return (
    <nav className="sticky top-0 z-50 border-b border-border/50 bg-background/80 backdrop-blur-xl">
      <div className="mx-auto flex h-14 max-w-7xl items-center justify-between px-4 sm:px-6 lg:px-8">
        <div className="flex items-center gap-6">
          <Link href="/" className="flex items-center gap-2">
            <div className="h-6 w-6 rounded-md bg-primary flex items-center justify-center">
              <span className="text-xs font-bold text-primary-foreground">Q</span>
            </div>
            <span className="font-semibold tracking-tight text-foreground">
              Quantyc
            </span>
          </Link>
          <div className="hidden md:flex items-center gap-1">
            {links.map(({ href, label, icon: Icon }) => {
              const active =
                href === "/" ? pathname === "/" : pathname.startsWith(href);
              return (
                <Link
                  key={href}
                  href={href}
                  className={cn(
                    "flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                    active
                      ? "bg-primary/10 text-primary"
                      : "text-muted-foreground hover:text-foreground hover:bg-muted"
                  )}
                >
                  <Icon className="h-3.5 w-3.5" />
                  {label}
                </Link>
              );
            })}
          </div>
        </div>
        <button
          onClick={handleParse}
          disabled={parsing}
          className={cn(
            "inline-flex items-center gap-1.5 rounded-md bg-primary px-3.5 py-1.5 text-sm font-semibold text-primary-foreground transition-all hover:bg-primary/90",
            parsing && "opacity-60 cursor-not-allowed"
          )}
        >
          <Play className="h-3.5 w-3.5" />
          {parsing ? "Running..." : "Run Pipeline"}
        </button>
      </div>
    </nav>
  );
}

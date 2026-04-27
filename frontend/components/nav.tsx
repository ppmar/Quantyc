"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/", label: "Overview" },
  { href: "/companies", label: "Companies" },
  { href: "/documents", label: "Documents" },
  { href: "/review", label: "Review" },
];

export function Nav() {
  const pathname = usePathname();

  return (
    <nav className="sticky top-0 z-50 border-b border-border bg-background/90 backdrop-blur-md">
      <div className="mx-auto flex h-12 max-w-6xl items-center justify-between px-6 lg:px-8">
        <div className="flex items-center gap-8">
          <Link href="/" className="flex items-center gap-2">
            <span className="text-[13px] font-semibold tracking-wide text-amber">
              QUANTYC
            </span>
          </Link>
          <div className="flex items-center gap-1">
            {links.map(({ href, label }) => {
              const active =
                href === "/" ? pathname === "/" : pathname.startsWith(href);
              return (
                <Link
                  key={href}
                  href={href}
                  className={`relative px-3 py-3.5 text-[13px] transition-colors ${
                    active
                      ? "text-zinc-100"
                      : "text-zinc-500 hover:text-zinc-300"
                  }`}
                >
                  {label}
                  {active && (
                    <span className="absolute bottom-0 left-3 right-3 h-px bg-zinc-100" />
                  )}
                </Link>
              );
            })}
          </div>
        </div>
      </div>
    </nav>
  );
}

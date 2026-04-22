import Link from "next/link";
import { ArrowLeft } from "lucide-react";

export function CompanyHeader({
  ticker,
  name,
  metaLine,
}: {
  ticker: string;
  name: string;
  metaLine: string;
}) {
  return (
    <div>
      <Link
        href="/"
        className="inline-flex items-center gap-1.5 text-sm text-zinc-500 hover:text-zinc-300 transition-colors mb-4"
      >
        <ArrowLeft className="h-3.5 w-3.5" /> Dashboard
      </Link>
      <div className="space-y-1">
        <p className="text-sm font-medium tracking-wider text-[#f5b642]">
          {ticker}
        </p>
        <h1 className="text-3xl font-medium tracking-tight text-zinc-50">
          {name}
        </h1>
        {metaLine && (
          <p className="text-sm text-zinc-500">{metaLine}</p>
        )}
      </div>
    </div>
  );
}

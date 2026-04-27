import Link from "next/link";

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
        href="/companies"
        className="inline-flex items-center gap-1 text-[13px] text-zinc-600 hover:text-zinc-400 transition-colors mb-5"
      >
        <span className="text-[11px]">&larr;</span> Companies
      </Link>
      <div className="space-y-1">
        <p className="text-[13px] font-mono font-medium tracking-wider text-amber">
          {ticker}
        </p>
        <h1 className="text-3xl font-medium tracking-tight text-zinc-50">
          {name}
        </h1>
        {metaLine && (
          <p className="text-[13px] text-zinc-500">{metaLine}</p>
        )}
      </div>
    </div>
  );
}

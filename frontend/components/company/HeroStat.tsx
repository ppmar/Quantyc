export function HeroStat({
  label,
  value,
  subtext,
  caption,
}: {
  label: string;
  value: string;
  subtext?: string | null;
  caption?: string;
}) {
  return (
    <div>
      <p className="text-xs uppercase tracking-wider text-zinc-500 mb-2">
        {label}
      </p>
      <p className="text-[44px] font-medium tracking-[-0.035em] leading-none text-zinc-50">
        {value}
      </p>
      {subtext && (
        <p className="text-sm text-zinc-400 mt-1.5">{subtext}</p>
      )}
      {caption && (
        <p className="text-sm text-zinc-500 mt-2 leading-relaxed">{caption}</p>
      )}
    </div>
  );
}

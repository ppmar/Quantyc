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
    <div className="q-card q-card-hero px-6 py-5">
      <p className="q-label mb-2.5">{label}</p>
      <p className="font-mono text-[40px] tracking-[-0.03em] leading-none text-zinc-50">
        {value}
      </p>
      {subtext && (
        <p className="text-sm text-amber/80 mt-2">{subtext}</p>
      )}
      {caption && (
        <p className="text-sm text-zinc-500 mt-2 leading-relaxed">{caption}</p>
      )}
    </div>
  );
}

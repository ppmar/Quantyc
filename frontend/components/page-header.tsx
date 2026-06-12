export function PageHeader({
  title,
  subtitle,
  right,
}: {
  title: string;
  subtitle?: React.ReactNode;
  right?: React.ReactNode;
}) {
  return (
    <div className="flex items-end justify-between gap-4">
      <div>
        <h1 className="q-display text-[28px] leading-none text-zinc-100">
          {title}
        </h1>
        {subtitle && (
          <p className="mt-1.5 text-[13px] text-zinc-500">{subtitle}</p>
        )}
      </div>
      {right}
    </div>
  );
}

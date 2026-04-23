import type { CashSection, CapitalSection } from "@/types/snapshot";
import { HeroStat } from "./HeroStat";

export function HeroGrid({
  cash,
  capital,
}: {
  cash?: CashSection;
  capital?: CapitalSection;
}) {
  if (!cash && !capital) return null;

  const hasBoth = !!cash && !!capital;

  return (
    <div className={`grid gap-12 ${hasBoth ? "grid-cols-1 sm:grid-cols-2" : "grid-cols-1"}`}>
      {cash && (
        <HeroStat
          label="Cash Position"
          value={cash.amount_display}
          subtext={cash.runway_display}
          caption={cash.prose}
        />
      )}
      {capital && (
        <HeroStat
          label="Quoted Shares"
          value={capital.shares_display}
          subtext={capital.shares_label}
          caption={[capital.prose, capital.fully_diluted_prose].filter(Boolean).join(" ") || undefined}
        />
      )}
    </div>
  );
}

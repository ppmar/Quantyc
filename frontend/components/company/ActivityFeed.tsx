import type { ActivityEvent } from "@/types/snapshot";
import { ActivityItem } from "./ActivityItem";
import { ArrowRight } from "lucide-react";
import Link from "next/link";

export function ActivityFeed({
  events,
  ticker,
}: {
  events: ActivityEvent[];
  ticker: string;
}) {
  if (events.length === 0) return null;

  return (
    <div>
      <p className="text-xs uppercase tracking-wider text-zinc-500 mb-2">
        Recent Activity
      </p>
      <div className="divide-y divide-white/[0.06]">
        {events.slice(0, 5).map((event) => (
          <ActivityItem key={event.id} event={event} />
        ))}
      </div>
      {events.length > 5 && (
        <Link
          href={`/company/${ticker}?tab=documents`}
          className="inline-flex items-center gap-1 text-sm text-zinc-500 hover:text-zinc-300 mt-3 transition-colors"
        >
          View all activity <ArrowRight className="h-3.5 w-3.5" />
        </Link>
      )}
    </div>
  );
}

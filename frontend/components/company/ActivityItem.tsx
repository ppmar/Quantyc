import type { ActivityEvent } from "@/types/snapshot";

export function ActivityItem({ event }: { event: ActivityEvent }) {
  const content = (
    <div className="py-3">
      <div className="flex items-baseline justify-between gap-4">
        <p className="text-sm font-medium text-zinc-200">{event.headline}</p>
        <p className="text-xs text-zinc-500 shrink-0">{event.relative_date}</p>
      </div>
      {event.detail && (
        <p className="text-sm text-zinc-400 mt-0.5 leading-relaxed">
          {event.detail}
        </p>
      )}
    </div>
  );

  if (event.source_url) {
    return (
      <a
        href={event.source_url}
        target="_blank"
        rel="noopener noreferrer"
        className="block hover:bg-white/[0.02] -mx-2 px-2 rounded transition-colors"
      >
        {content}
      </a>
    );
  }

  return content;
}

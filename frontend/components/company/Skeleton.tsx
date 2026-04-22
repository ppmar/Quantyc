export function SnapshotSkeleton() {
  return (
    <div className="space-y-10 animate-pulse">
      {/* Header skeleton */}
      <div>
        <div className="h-4 w-24 bg-zinc-800 rounded mb-4" />
        <div className="space-y-2">
          <div className="h-4 w-12 bg-zinc-800/60 rounded" />
          <div className="h-8 w-56 bg-zinc-800 rounded" />
          <div className="h-4 w-72 bg-zinc-800/40 rounded" />
        </div>
      </div>

      {/* Tab bar skeleton */}
      <div className="flex gap-6 border-b border-white/[0.06] pb-2.5">
        {[80, 68, 56, 72].map((w, i) => (
          <div key={i} className="h-4 bg-zinc-800/40 rounded" style={{ width: w }} />
        ))}
      </div>

      {/* Hero grid skeleton */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-12">
        <div>
          <div className="h-3 w-24 bg-zinc-800/40 rounded mb-3" />
          <div className="h-12 w-40 bg-zinc-800 rounded mb-2" />
          <div className="h-4 w-48 bg-zinc-800/40 rounded mb-2" />
          <div className="h-4 w-64 bg-zinc-800/30 rounded" />
        </div>
        <div>
          <div className="h-3 w-28 bg-zinc-800/40 rounded mb-3" />
          <div className="h-12 w-32 bg-zinc-800 rounded mb-2" />
          <div className="h-4 w-36 bg-zinc-800/40 rounded" />
        </div>
      </div>

      {/* Chart skeleton */}
      <div>
        <div className="h-3 w-28 bg-zinc-800/40 rounded mb-4" />
        <div className="h-64 bg-zinc-800/20 rounded" />
      </div>

      {/* Activity skeleton */}
      <div>
        <div className="h-3 w-24 bg-zinc-800/40 rounded mb-4" />
        {[1, 2, 3].map((i) => (
          <div key={i} className="py-3 border-b border-white/[0.04]">
            <div className="flex justify-between mb-1">
              <div className="h-4 w-28 bg-zinc-800/40 rounded" />
              <div className="h-3 w-20 bg-zinc-800/30 rounded" />
            </div>
            <div className="h-4 w-80 bg-zinc-800/20 rounded" />
          </div>
        ))}
      </div>
    </div>
  );
}

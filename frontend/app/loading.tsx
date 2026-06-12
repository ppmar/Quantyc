export default function Loading() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <div className="h-7 w-44 q-shimmer" />
        <div className="h-4 w-72 q-shimmer" />
      </div>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        {[1, 2, 3, 4, 5].map((i) => (
          <div key={i} className="h-24 q-shimmer" />
        ))}
      </div>
      <div className="h-72 q-shimmer" />
    </div>
  );
}

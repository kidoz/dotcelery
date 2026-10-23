import type { Filters } from "../../../shared/model/filters.ts";

export function taskQuery(filters: Filters, pageCap = 1000): string {
  const query = new URLSearchParams({
    page: String(filters.page),
    pageSize: String(Math.min(filters.size, pageCap)),
    oldestFirst: String(filters.order === "oldest"),
  });
  for (const [key, value] of Object.entries({
    search: filters.search,
    state: filters.state,
    taskName: filters.name,
    queue: filters.queue,
    worker: filters.worker,
  }))
    if (value) query.set(key, value);
  if (filters.range)
    query.set(
      "from",
      new Date(Date.now() - Number(filters.range) * 60000).toISOString(),
    );
  return query.toString();
}

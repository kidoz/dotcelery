export const emptyFilters = {
  search: "",
  state: "",
  name: "",
  queue: "",
  worker: "",
  range: "",
  order: "newest",
  page: 1,
  size: 25,
};

export type Filters = typeof emptyFilters;

export function taskFilterKey(filters: Filters): string {
  return JSON.stringify([
    filters.search,
    filters.state,
    filters.name,
    filters.queue,
    filters.worker,
    filters.range,
    filters.order,
    filters.page,
    filters.size,
  ]);
}

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

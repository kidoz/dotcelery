import { taskStates } from "../shared/domain/taskState.ts";
import { emptyFilters } from "../shared/model/filters.ts";
import type { Filters } from "../shared/model/filters.ts";
import type { View } from "../shared/model/navigation.ts";

export const views = {
  overview: ["Execution overview", "A clear view of your background work."],
  tasks: [
    "Task history",
    "Find an execution, understand its outcome, and take the next step.",
  ],
  queues: [
    "Queue health",
    "See where work is waiting and which queues have consumers.",
  ],
  workers: [
    "Worker fleet",
    "Monitor availability, heartbeats, and execution capacity.",
  ],
} as const;

export interface Route {
  view: View;
  filters: Filters;
  window: number;
  taskId: string | null;
}

export function readRoute(search: string): Route {
  const params = new URLSearchParams(search);
  const view = params.get("view");
  const filters = { ...emptyFilters };
  for (const key of [
    "search",
    "state",
    "name",
    "queue",
    "worker",
    "range",
    "order",
  ] as const)
    filters[key] = params.get(key) ?? filters[key];
  if (!taskStates.includes(filters.state)) filters.state = "";
  if (!["", "15", "60", "1440"].includes(filters.range)) filters.range = "";
  if (!["newest", "oldest"].includes(filters.order)) filters.order = "newest";
  filters.page = Math.max(
    1,
    Math.min(10000, Number.parseInt(params.get("page") ?? "", 10) || 1),
  );
  // The API enforces the configured page-size cap; retain custom caps in shared links.
  filters.size = Math.max(
    1,
    Math.min(1000, Number.parseInt(params.get("size") ?? "", 10) || 25),
  );
  return {
    view: view && Object.hasOwn(views, view) ? (view as View) : "overview",
    filters,
    window: [15, 60, 1440].includes(Number(params.get("window")))
      ? Number(params.get("window"))
      : 60,
    taskId: params.get("task") || null,
  };
}

export function routeQuery(route: Route): string {
  const query = new URLSearchParams({ view: route.view });
  for (const [key, value] of Object.entries(route.filters))
    if (value !== emptyFilters[key as keyof Filters])
      query.set(key, String(value));
  if (route.window !== 60) query.set("window", String(route.window));
  if (route.taskId) query.set("task", route.taskId);
  return `?${query}`;
}

import { taskQuery } from "../../features/tasks/api/taskQuery.ts";
import type { Route } from "../routing.ts";

export type RequestKey =
  | "overview"
  | "metrics"
  | "workers"
  | "queues"
  | "recent"
  | "tasks"
  | "taskNames"
  | "points";

export interface DashboardRequest {
  key: RequestKey;
  path: string;
  cacheFor: number;
}

export function requestPlan(
  route: Route,
  pageCap: number,
  hasHistory: boolean,
  overviewWindowMinutes = 60,
): DashboardRequest[] {
  switch (route.view) {
    case "workers":
      return [{ key: "workers", path: "workers", cacheFor: 0 }];
    case "queues":
      return [{ key: "queues", path: "queues", cacheFor: 0 }];
    case "tasks":
      return [
        {
          key: "tasks",
          path: `tasks/search?${taskQuery(route.filters, pageCap)}`,
          cacheFor: 0,
        },
        { key: "taskNames", path: "task-names", cacheFor: 60000 },
        { key: "workers", path: "workers", cacheFor: 60000 },
        { key: "queues", path: "queues", cacheFor: 60000 },
      ];
    case "overview": {
      const requests: DashboardRequest[] = [
        { key: "overview", path: "overview", cacheFor: 0 },
        { key: "workers", path: "workers", cacheFor: 0 },
        { key: "recent", path: "tasks/search?pageSize=7", cacheFor: 0 },
      ];
      if (route.window !== overviewWindowMinutes)
        requests.push({
          key: "metrics",
          path: `metrics?windowMinutes=${route.window}`,
          cacheFor: 0,
        });
      if (hasHistory) {
        const from = new Date(Date.now() - route.window * 60000).toISOString();
        requests.push({
          key: "points",
          path: `metrics/timeseries?${new URLSearchParams({ from, granularity: route.window === 1440 ? "Hour" : "Minute" })}`,
          cacheFor: 0,
        });
      }
      return requests;
    }
  }
}

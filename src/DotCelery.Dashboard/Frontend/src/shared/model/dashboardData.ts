import type {
  Capabilities,
  Metrics,
  Overview,
  QueueStats,
  TaskList,
  TimePoint,
  WorkerInfo,
} from "../api/contracts.ts";

export interface DashboardData {
  capabilities?: Capabilities;
  overview?: Overview;
  metrics?: Metrics;
  workers?: WorkerInfo[];
  queues?: QueueStats[];
  recent?: TaskList;
  tasks?: TaskList;
  taskNames?: string[];
  points?: TimePoint[];
  updatedAt?: string;
  error: string;
}

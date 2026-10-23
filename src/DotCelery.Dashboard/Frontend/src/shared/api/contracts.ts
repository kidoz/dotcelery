// JSON contracts exposed by the dashboard REST API v1.

export interface Capabilities {
  readOnly: boolean;
  canCancel: boolean;
  canRetry: boolean;
  hasHistory: boolean;
  hasTaskRecords: boolean;
  retentionSeconds: number;
  snapshotSeconds: number;
  refreshSeconds: number;
  metricsWindowMinutes: number;
  pageSize: number;
}

export interface TaskSummary {
  taskId: string;
  taskName: string;
  state: string;
  queue?: string | null;
  worker?: string | null;
  sentAt?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
  duration?: string | null;
  retries: number;
  exceptionMessage?: string | null;
  eta?: string | null;
}

export interface TaskList {
  tasks: TaskSummary[];
  page: number;
  pageSize: number;
  hasMore: boolean;
}

export interface QueueStats {
  name: string;
  messageCount: number;
  inProgressCount: number;
  consumerCount: number;
  lastMessageAt?: string | null;
}

export interface WorkerInfo {
  workerId: string;
  hostname: string;
  status: number | string;
  queues: string[];
  activeTasks: number;
  concurrency: number;
  processedCount: number;
  lastHeartbeat: string;
}

export interface TaskMetrics {
  totalCount: number;
  failureCount: number;
}

export interface Metrics {
  failureCount: number;
  tasksPerSecond: number;
  averageExecutionTime?: string | null;
  byTaskName?: Record<string, TaskMetrics> | null;
}

export interface Overview {
  metrics: Metrics;
  stateCounts: Record<string, number>;
  activeWorkers: number;
  workerCount: number;
  queues: QueueStats[];
}

export interface TimePoint {
  timestamp: string;
  successCount: number;
  failureCount: number;
}

export interface ActionPreview {
  cancelTaskIds: string[];
  retryTaskIds: string[];
}

export interface BulkResult {
  successCount: number;
  failureCount: number;
  failedTaskIds?: string[] | null;
}

export type Action = "retry" | "cancel";
export interface ActionRequest {
  action: Action;
  ids: string[];
}

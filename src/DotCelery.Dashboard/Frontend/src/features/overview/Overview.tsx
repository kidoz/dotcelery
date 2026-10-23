import {
  duration,
  number,
  workerStatus,
  queueHealth,
  windowLabel,
} from "../../shared/lib/format.ts";
import { Badge } from "../../shared/ui/Badge.tsx";
import { EmptyRow, Table } from "../../shared/ui/Table.tsx";
import { Link } from "../../shared/ui/Link.tsx";
import { Panel } from "../../shared/ui/Panel.tsx";
import { TaskLink } from "../../shared/ui/TaskLink.tsx";
import { Time } from "../../shared/ui/Time.tsx";
import type { DashboardData } from "../../shared/model/dashboardData.ts";
import type { Navigation } from "../../shared/model/navigation.ts";
import { CompletionChart } from "./components/CompletionChart.tsx";

export function Overview({
  data,
  window,
  navigation,
}: {
  data: DashboardData;
  window: number;
  navigation: Navigation;
}) {
  const { overview, metrics, capabilities, queues, workers } = data;
  const counts = overview?.stateCounts ?? {};
  const waiting =
    overview?.queues.reduce((sum, queue) => sum + queue.messageCount, 0) ?? 0;
  const running = overview?.queues.length
    ? overview.queues.reduce((sum, queue) => sum + queue.inProgressCount, 0)
    : (counts.Started ?? 0) + (counts.Progress ?? 0);
  const attention = Object.entries(metrics?.byTaskName ?? {})
    .filter(([, metric]) => metric.failureCount > 0)
    .sort((a, b) => b[1].failureCount - a[1].failureCount)
    .slice(0, 4);
  const cards = [
    ["Queued", number(waiting), "Now · waiting in queues", "⇉"],
    ["Running", number(running), "Now · tasks in progress", "↗"],
    ["Failed", number(metrics?.failureCount ?? 0), windowLabel(window), "!"],
    [
      "Workers online",
      `${number(overview?.activeWorkers ?? 0)} / ${number(overview?.workerCount ?? 0)}`,
      "Now · registered workers",
      "▤",
    ],
    [
      "Throughput",
      `${number(metrics?.tasksPerSecond ?? 0)} /s`,
      windowLabel(window),
      "⌁",
    ],
    [
      "Avg. duration",
      duration(metrics?.averageExecutionTime),
      windowLabel(window),
      "◷",
    ],
  ];
  return (
    <section aria-label="Overview" className="space-y-5">
      <div
        className="grid grid-cols-2 gap-3 xl:grid-cols-6"
        aria-busy={!overview || !metrics}
      >
        {cards.map(([label, value, meta, icon]) => (
          <article key={label} className="stat-card">
            <h3 className="stat-label">
              <span aria-hidden="true">{icon}</span>
              {label}
            </h3>
            <div
              className={`stat-value ${label === "Failed" ? "text-danger" : ""}`}
            >
              {overview && metrics ? value : "—"}
            </div>
            <p className="stat-meta">{meta}</p>
          </article>
        ))}
      </div>
      <div className="grid gap-5 xl:grid-cols-[1.65fr_1fr]">
        <Panel
          title="Task completions"
          description="Successful and failed executions over time"
          action={
            <div className="flex gap-3 text-xs">
              <span className="text-brand">● Successful</span>
              <span className="text-danger">● Failed</span>
            </div>
          }
        >
          <CompletionChart
            points={data.points}
            hasHistory={capabilities?.hasHistory}
            window={window}
            snapshotSeconds={capabilities?.snapshotSeconds}
          />
        </Panel>
        <Panel
          title="Tasks that need attention"
          description="Failure counts in the selected window"
        >
          {attention.length ? (
            attention.map(([name, metric]) => (
              <div key={name} className="mini-row">
                <div>
                  <Link
                    href={`?view=tasks&name=${encodeURIComponent(name)}&state=Failure`}
                    onNavigate={() =>
                      navigation.inspect({ name, state: "Failure" })
                    }
                  >
                    {name}
                  </Link>
                  <small>
                    {number(metric.totalCount)} executions ·{" "}
                    {number(
                      metric.totalCount
                        ? (metric.failureCount / metric.totalCount) * 100
                        : 0,
                    )}
                    % failed
                  </small>
                </div>
                <span className="badge badge-failure">
                  {number(metric.failureCount)} failed
                </span>
              </div>
            ))
          ) : (
            <p className="empty">
              {metrics
                ? "No task failures in this window. Failures will appear here when recorded."
                : "Loading task breakdown…"}
            </p>
          )}
        </Panel>
      </div>
      <Panel
        title="Recent activity"
        description="All states · most recently updated first"
        action={
          <Link
            href="?view=tasks"
            onNavigate={() => navigation.navigate("tasks")}
          >
            View all tasks →
          </Link>
        }
      >
        <Table
          label="Recent task activity"
          headings={["Task", "State", "Worker", "Duration", "Updated"]}
        >
          {data.recent?.tasks.length ? (
            data.recent.tasks.map((task) => (
              <tr key={task.taskId}>
                <td>
                  <TaskLink
                    task={task}
                    href={navigation.taskHref(task.taskId)}
                    open={navigation.openTask}
                  />
                </td>
                <td>
                  <Badge state={task.state} />
                </td>
                <td>{task.worker || "—"}</td>
                <td>{duration(task.duration)}</td>
                <td>
                  <Time
                    value={task.completedAt ?? task.startedAt ?? task.sentAt}
                  />
                </td>
              </tr>
            ))
          ) : (
            <EmptyRow columns={5}>
              {!data.recent
                ? "Loading tasks…"
                : capabilities?.hasTaskRecords
                  ? "No task activity recorded yet."
                  : "Task history is unavailable: no task storage is configured."}
            </EmptyRow>
          )}
        </Table>
      </Panel>
      <div className="grid gap-5 xl:grid-cols-2">
        <Panel
          title="Queue health"
          description="Waiting tasks and available consumers"
          action={
            <Link
              href="?view=queues"
              onNavigate={() => navigation.navigate("queues")}
            >
              All queues →
            </Link>
          }
        >
          {queues?.length ? (
            queues.slice(0, 3).map((queue) => (
              <div key={queue.name} className="mini-row">
                <div>
                  <Link
                    href={`?view=tasks&queue=${encodeURIComponent(queue.name)}`}
                    onNavigate={() => navigation.inspect({ queue: queue.name })}
                  >
                    {queue.name}
                  </Link>
                  <small>
                    {queueHealth(queue) === "Warning"
                      ? "Waiting work has no active consumers"
                      : `${number(queue.consumerCount)} consumers available`}
                  </small>
                </div>
                <div className="flex gap-3 text-xs text-muted">
                  <span>
                    <strong>{number(queue.messageCount)}</strong> waiting
                  </span>
                  <span>
                    <strong>{number(queue.inProgressCount)}</strong> running
                  </span>
                </div>
              </div>
            ))
          ) : (
            <p className="empty">
              {queues ? "No queue metrics recorded yet." : "Loading queues…"}
            </p>
          )}
        </Panel>
        <Panel
          title="Worker availability"
          description="Live heartbeat and execution capacity"
          action={
            <Link
              href="?view=workers"
              onNavigate={() => navigation.navigate("workers")}
            >
              All workers →
            </Link>
          }
        >
          {workers?.length ? (
            workers.slice(0, 3).map((worker) => (
              <div className="mini-row" key={worker.workerId}>
                <div>
                  <Link
                    href={`?view=tasks&worker=${encodeURIComponent(worker.workerId)}`}
                    onNavigate={() =>
                      navigation.inspect({ worker: worker.workerId })
                    }
                  >
                    {worker.workerId}
                  </Link>
                  <small>
                    {number(worker.activeTasks)} / {number(worker.concurrency)}{" "}
                    slots active · heartbeat{" "}
                    <Time value={worker.lastHeartbeat} />
                  </small>
                </div>
                <Badge state={workerStatus(worker)} />
              </div>
            ))
          ) : (
            <p className="empty">
              {workers ? "No workers registered." : "Loading workers…"}
            </p>
          )}
        </Panel>
      </div>
    </section>
  );
}

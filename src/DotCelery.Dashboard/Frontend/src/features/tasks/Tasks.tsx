import { useEffect, useState } from "react";
import { duration, number } from "../../shared/lib/format.ts";
import { Badge } from "../../shared/ui/Badge.tsx";
import { EmptyRow, Table } from "../../shared/ui/Table.tsx";
import { SelectAll } from "./components/SelectAll.tsx";
import { TaskLink } from "../../shared/ui/TaskLink.tsx";
import { Time } from "../../shared/ui/Time.tsx";
import { emptyFilters } from "../../shared/model/filters.ts";
import { stateLabel, taskStates } from "../../shared/domain/taskState.ts";
import { Filter } from "./components/Filter.tsx";
import type { Filters } from "../../shared/model/filters.ts";
import type { ActionRequest } from "../../shared/api/contracts.ts";
import type { DashboardData } from "../../shared/model/dashboardData.ts";
import type { Navigation } from "../../shared/model/navigation.ts";

export function Tasks({
  data,
  filters,
  update,
  navigation,
  selected,
  select,
  cancelled,
  plan,
}: {
  data: DashboardData;
  filters: Filters;
  update: (filters: Partial<Filters>) => void;
  navigation: Navigation;
  selected: Set<string>;
  select: (ids: Set<string>) => void;
  cancelled: Set<string>;
  plan: (request: ActionRequest) => void;
}) {
  const [search, setSearch] = useState(filters.search);
  useEffect(() => setSearch(filters.search), [filters.search]);
  const caps = data.capabilities;
  const tasks = data.tasks?.tasks ?? [];
  const writable =
    !!caps && !caps.readOnly && (caps.canCancel || caps.canRetry);
  const allSelected =
    tasks.length > 0 && tasks.every((task) => selected.has(task.taskId));
  const effectiveSize = Math.min(filters.size, caps?.pageSize ?? filters.size);
  const sizes = [...new Set([10, 25, 50, effectiveSize, caps?.pageSize ?? 50])]
    .filter((size) => size <= (caps?.pageSize ?? 1000))
    .sort((a, b) => a - b);
  const start = data.tasks ? (data.tasks.page - 1) * data.tasks.pageSize : 0;
  const options = (values: string[], current: string) =>
    [...new Set([...values.filter(Boolean), ...(current ? [current] : [])])]
      .sort()
      .map((value) => (
        <option key={value} value={value}>
          {value}
        </option>
      ));
  const chips = {
    search: "Contains",
    state: "State",
    name: "Task",
    queue: "Queue",
    worker: "Worker",
    range: "Updated within",
  };
  return (
    <section aria-label="Task history" className="panel">
      <form
        className="filter-toolbar"
        role="search"
        aria-label="Search task history"
        onSubmit={(event) => {
          event.preventDefault();
          update({ search: search.trim() });
        }}
      >
        <div className="search-box">
          <label className="sr-only" htmlFor="search">
            Task ID or name contains
          </label>
          <input
            type="search"
            id="search"
            placeholder="Search task ID or name…"
            maxLength={500}
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
          <button className="button secondary" type="submit">
            Search
          </button>
        </div>
        <Filter
          label="State"
          value={filters.state}
          onChange={(state) => update({ state })}
        >
          <option value="">All states</option>
          {taskStates.map((state) => (
            <option key={state} value={state}>
              {stateLabel(state)}
              {data.overview?.stateCounts[state] !== undefined
                ? ` (${number(data.overview.stateCounts[state])})`
                : ""}
            </option>
          ))}
        </Filter>
        <Filter
          label="Task name"
          value={filters.name}
          onChange={(name) => update({ name })}
        >
          <option value="">All task names</option>
          {options(data.taskNames ?? [], filters.name)}
        </Filter>
        <Filter
          label="Queue"
          value={filters.queue}
          onChange={(queue) => update({ queue })}
        >
          <option value="">All queues</option>
          {options(
            data.queues?.map((queue) => queue.name) ?? [],
            filters.queue,
          )}
        </Filter>
        <Filter
          label="Worker"
          value={filters.worker}
          onChange={(worker) => update({ worker })}
        >
          <option value="">All workers</option>
          {options(
            data.workers?.map((worker) => worker.workerId) ?? [],
            filters.worker,
          )}
        </Filter>
        <Filter
          label="Updated"
          value={filters.range}
          onChange={(range) => update({ range })}
        >
          <option value="">Any retained time</option>
          <option value="15">Last 15 minutes</option>
          <option value="60">Last hour</option>
          <option value="1440">Last 24 hours</option>
        </Filter>
        <Filter
          label="Order"
          value={filters.order}
          onChange={(order) => update({ order })}
        >
          <option value="newest">Newest first</option>
          <option value="oldest">Oldest first</option>
        </Filter>
      </form>
      <div
        className="flex flex-wrap gap-2 px-5 pb-3"
        aria-label="Active filters"
      >
        {(Object.keys(chips) as (keyof typeof chips)[])
          .filter((key) => filters[key])
          .map((key) => (
            <button
              key={key}
              className="chip"
              aria-label={`Remove ${chips[key]} filter`}
              onClick={() => {
                if (key === "search") setSearch("");
                update({ [key]: emptyFilters[key] });
              }}
            >
              {chips[key]}:{" "}
              {key === "range"
                ? `${filters[key]} min`
                : stateLabel(filters[key])}{" "}
              <span aria-hidden="true">×</span>
            </button>
          ))}
      </div>
      <div className="task-toolbar">
        <span role="status" aria-live="polite">
          {!data.tasks
            ? "Loading tasks…"
            : tasks.length
              ? `Showing ${number(start + 1)}–${number(start + tasks.length)} · ${data.tasks.hasMore ? "more available" : "end of results"}`
              : "No matching tasks"}
        </span>
        {writable && (
          <div className="flex flex-wrap items-center gap-2">
            <span>{selected.size} selected</span>
            {caps.canRetry && (
              <button
                className="button secondary"
                disabled={!selected.size}
                onClick={() => plan({ action: "retry", ids: [...selected] })}
              >
                Retry selected
              </button>
            )}
            {caps.canCancel && (
              <button
                className="button danger-outline"
                disabled={!selected.size}
                onClick={() => plan({ action: "cancel", ids: [...selected] })}
              >
                Cancel selected
              </button>
            )}
            <button
              className="text-button"
              disabled={!selected.size}
              onClick={() => select(new Set())}
            >
              Clear selection
            </button>
          </div>
        )}
      </div>
      <Table
        label="Task history"
        headings={[
          writable ? (
            <SelectAll
              checked={allSelected}
              indeterminate={selected.size > 0 && !allSelected}
              disabled={!tasks.length}
              onChange={(checked) =>
                select(
                  checked
                    ? new Set(tasks.map((task) => task.taskId))
                    : new Set(),
                )
              }
            />
          ) : null,
          "Task / ID",
          "State",
          "Queue",
          "Worker",
          "Duration",
          "Updated",
        ]}
      >
        {tasks.length ? (
          tasks.map((task) => (
            <tr key={task.taskId}>
              <td className="w-10">
                {writable && (
                  <input
                    type="checkbox"
                    aria-label={`Select ${task.taskName} ${task.taskId}`}
                    checked={selected.has(task.taskId)}
                    onChange={(event) => {
                      const next = new Set(selected);
                      if (event.target.checked) next.add(task.taskId);
                      else next.delete(task.taskId);
                      select(next);
                    }}
                  />
                )}
              </td>
              <td>
                <TaskLink
                  task={task}
                  href={navigation.taskHref(task.taskId)}
                  open={navigation.openTask}
                />
              </td>
              <td>
                <Badge state={task.state} />
                {cancelled.has(task.taskId) &&
                  !["Revoked", "Success", "Failure", "Rejected"].includes(
                    task.state,
                  ) && (
                    <small className="subtext">Cancellation requested</small>
                  )}
              </td>
              <td>{task.queue || "—"}</td>
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
          <EmptyRow columns={7}>
            {!data.tasks
              ? "Loading tasks…"
              : caps?.hasTaskRecords
                ? "No tasks match these filters. Clear a filter or choose a wider time range."
                : "Task history is unavailable: no task storage is configured."}
          </EmptyRow>
        )}
      </Table>
      <div className="pagination">
        <Filter
          label="Rows per page"
          value={effectiveSize}
          onChange={(size) => update({ size: Number(size) })}
        >
          {sizes.map((size) => (
            <option key={size} value={size}>
              {size}
            </option>
          ))}
        </Filter>
        <span>Page {number(data.tasks?.page ?? filters.page)}</span>
        <button
          className="button secondary"
          disabled={!data.tasks || data.tasks.page <= 1}
          onClick={() => update({ page: Math.max(1, filters.page - 1) })}
        >
          ← Previous
        </button>
        <button
          className="button secondary"
          disabled={!data.tasks?.hasMore}
          onClick={() => update({ page: filters.page + 1 })}
        >
          Next →
        </button>
      </div>
    </section>
  );
}

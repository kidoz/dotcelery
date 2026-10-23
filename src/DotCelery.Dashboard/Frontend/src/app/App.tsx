import { useEffect, useRef, useState } from "react";
import logo from "../../../../../assets/dotcelery-logo.png";
import { Link } from "../shared/ui/Link.tsx";
import { number } from "../shared/lib/format.ts";
import { Overview } from "../features/overview/Overview.tsx";
import { Queues } from "../features/queues/Queues.tsx";
import { Workers } from "../features/workers/Workers.tsx";
import { Tasks } from "../features/tasks/Tasks.tsx";
import { ActionDialog } from "../features/tasks/components/ActionDialog.tsx";
import { TaskDetails } from "../features/tasks/components/TaskDetails.tsx";
import { emptyFilters } from "../shared/model/filters.ts";
import { readRoute, routeQuery, views } from "./routing.ts";
import type { Filters } from "../shared/model/filters.ts";
import type { Route } from "./routing.ts";
import type { View } from "../shared/model/navigation.ts";
import type { ActionRequest, BulkResult } from "../shared/api/contracts.ts";
import { useDashboard } from "./hooks/useDashboard.ts";

type ActiveAction = ActionRequest & {
  actionGeneration: number;
  navigationGeneration: number;
};

export default function App() {
  const prefix = document.body.dataset.prefix ?? "/celery";
  const title = document.body.dataset.title ?? "DotCelery Dashboard";
  const api = `${prefix}/api/v1`;
  const [route, setRoute] = useState(() => readRoute(location.search));
  const [paused, setPaused] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [cancelled, setCancelled] = useState<Set<string>>(new Set());
  const [action, setAction] = useState<ActiveAction | null>(null);
  const generations = useRef({ action: 0, navigation: 0 });
  const [message, setMessage] = useState({ text: "", error: false });
  const blocked = selected.size > 0 || route.taskId !== null || action !== null;
  const { data, busy, refresh } = useDashboard(api, route, paused, blocked);
  const caps = data.capabilities;

  useEffect(() => {
    const currentGenerations = generations.current;
    const onPopState = () => {
      currentGenerations.navigation++;
      currentGenerations.action++;
      setSelected(new Set());
      setAction(null);
      setRoute(readRoute(location.search));
    };
    window.addEventListener("popstate", onPopState);
    return () => {
      window.removeEventListener("popstate", onPopState);
      currentGenerations.navigation++;
      currentGenerations.action++;
    };
  }, []);

  function applyRoute(next: Route, push = false) {
    generations.current.navigation++;
    history[push ? "pushState" : "replaceState"](
      null,
      "",
      `${prefix}/${routeQuery(next)}`,
    );
    setRoute(next);
  }
  function navigate(view: View) {
    setSelected(new Set());
    applyRoute({ ...route, view, taskId: null }, true);
  }
  function inspect(filters: Partial<Filters>) {
    setSelected(new Set());
    applyRoute(
      {
        ...route,
        view: "tasks",
        taskId: null,
        filters: { ...emptyFilters, ...filters },
      },
      true,
    );
  }
  function update(filters: Partial<Filters>) {
    setSelected(new Set());
    applyRoute({
      ...route,
      filters: { ...route.filters, page: 1, ...filters },
    });
  }
  const taskHref = (id: string) =>
    `${prefix}/${routeQuery({ ...route, view: "tasks", taskId: id })}`;
  function openTask(id: string) {
    applyRoute({ ...route, taskId: id }, true);
  }
  const navigation = { navigate, inspect, taskHref, openTask };
  function planAction(request: ActionRequest) {
    setAction({
      ...request,
      actionGeneration: ++generations.current.action,
      navigationGeneration: generations.current.navigation,
    });
  }
  function dismissAction() {
    generations.current.action++;
    setAction(null);
  }
  function complete(
    owner: ActiveAction,
    request: ActionRequest,
    result: BulkResult,
  ) {
    if (request.action === "cancel")
      setCancelled(
        (current) =>
          new Set([
            ...current,
            ...request.ids.filter((id) => !result.failedTaskIds?.includes(id)),
          ]),
      );
    const label =
      request.action === "cancel"
        ? result.successCount === 1
          ? "cancellation request accepted"
          : "cancellation requests accepted"
        : result.successCount === 1
          ? "task requeued"
          : "tasks requeued";
    setMessage({
      text: `${result.successCount} ${label}.${result.failureCount ? ` ${result.failureCount} failed: ${result.failedTaskIds?.join(", ") ?? "unknown tasks"}.` : ""}`,
      error: result.failureCount > 0,
    });
    // A submitted mutation may finish after Back or a newer action. Report its
    // result, but only its original, still-current UI context may be cleared.
    if (
      owner.actionGeneration === generations.current.action &&
      owner.navigationGeneration === generations.current.navigation
    ) {
      setSelected(new Set());
      dismissAction();
      applyRoute({ ...route, taskId: null });
    }
    refresh();
  }
  const connection = data.error
    ? "Data may be stale"
    : selected.size
      ? "Updates paused for selection"
      : blocked
        ? "Updates paused while a dialog is open"
        : paused
          ? "Updates paused"
          : caps
            ? `Polling · ${caps.refreshSeconds}s`
            : "Connecting";
  return (
    <>
      <a
        className="skip-link"
        href="#main"
        onClick={(event) => {
          event.preventDefault();
          document.getElementById("main")?.focus();
        }}
      >
        Skip to content
      </a>
      <aside className="sidebar">
        <Link
          className="brand"
          href="?view=overview"
          onNavigate={() => navigate("overview")}
        >
          <img src={logo} alt="DotCelery" width={176} height={62} />
          <small>Task monitoring</small>
        </Link>
        <div className="nav-label">WORKSPACE</div>
        <nav aria-label="Dashboard">
          {(["overview", "tasks", "queues", "workers"] as const).map(
            (view, index) => (
              <a
                key={view}
                href={`?view=${view}`}
                aria-current={route.view === view ? "page" : undefined}
                onClick={(event) => {
                  if (
                    event.ctrlKey ||
                    event.metaKey ||
                    event.shiftKey ||
                    event.altKey
                  )
                    return;
                  event.preventDefault();
                  navigate(view);
                }}
              >
                <span aria-hidden="true">{["◫", "≡", "⇉", "▤"][index]}</span>
                {view[0]?.toUpperCase()}
                {view.slice(1)}
                {view === "tasks" && (
                  <span className="nav-count">
                    {data.overview
                      ? number(
                          Object.values(data.overview.stateCounts).reduce(
                            (sum, value) => sum + value,
                            0,
                          ),
                        )
                      : "—"}
                  </span>
                )}
              </a>
            ),
          )}
        </nav>
        <div className="sidebar-footer">
          <span className="status-dot" />
          Distributed task queue<small>.NET · DotCelery</small>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <span className="font-semibold">{title}</span>
          {caps?.readOnly && <span className="tag">Read only</span>}
          <span className="ml-auto flex items-center text-xs text-muted">
            <span
              className={`status-dot ${data.error || paused || blocked ? "stale" : ""}`}
            />
            {connection}
          </span>
        </header>
        <main id="main" tabIndex={-1}>
          <div className="page-heading">
            <div>
              <p className="eyebrow">MONITOR &amp; MANAGE</p>
              <h1>{views[route.view][0]}</h1>
              <p className="subtitle">{views[route.view][1]}</p>
            </div>
            <div className="refresh-controls">
              {route.view === "overview" && (
                <>
                  <label className="sr-only" htmlFor="metrics-window">
                    Metrics time window
                  </label>
                  <select
                    id="metrics-window"
                    value={route.window}
                    onChange={(event) =>
                      applyRoute({
                        ...route,
                        window: Number(event.target.value),
                      })
                    }
                  >
                    <option value={15}>Last 15 minutes</option>
                    <option value={60}>Last hour</option>
                    <option value={1440}>Last 24 hours</option>
                  </select>
                </>
              )}
              <button
                className="button secondary"
                aria-pressed={paused}
                onClick={() => setPaused((value) => !value)}
              >
                {paused ? "Resume updates" : "Pause updates"}
              </button>
              <button
                className="button secondary"
                aria-label="Refresh dashboard"
                disabled={busy || blocked}
                onClick={refresh}
              >
                ↻ <span>Refresh</span>
              </button>
              <small className="w-full text-right text-muted">
                {data.updatedAt
                  ? `Updated ${new Date(data.updatedAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`
                  : "Waiting for data"}
              </small>
            </div>
          </div>
          {data.error && (
            <div className="notice error-notice" role="alert">
              {data.error}
            </div>
          )}
          {message.text && (
            <div
              className={`notice ${message.error ? "error-notice" : ""}`}
              role="status"
              aria-live="polite"
            >
              {message.text}
            </div>
          )}
          {route.view === "overview" && (
            <Overview
              data={data}
              window={route.window}
              navigation={navigation}
            />
          )}
          {route.view === "tasks" && (
            <Tasks
              data={data}
              filters={route.filters}
              update={update}
              navigation={navigation}
              selected={selected}
              select={setSelected}
              cancelled={cancelled}
              plan={planAction}
            />
          )}
          {route.view === "queues" && (
            <Queues data={data} navigation={navigation} />
          )}
          {route.view === "workers" && (
            <Workers data={data} navigation={navigation} />
          )}
          <footer className="page-footer">
            <span>
              DotCelery <span className="mx-2 text-line">/</span> Background
              work, in view.
            </span>
            <span>
              {caps
                ? `History retained for ${number(caps.retentionSeconds / 3600)}h · recorded by participating hosts`
                : "Task history follows your storage retention."}
            </span>
          </footer>
        </main>
      </div>
      {route.taskId && (
        <TaskDetails
          key={route.taskId}
          api={api}
          id={route.taskId}
          capabilities={caps}
          cancelled={cancelled.has(route.taskId)}
          link={taskHref(route.taskId)}
          dismiss={() => applyRoute({ ...route, taskId: null })}
          plan={planAction}
        />
      )}
      {action && (
        <ActionDialog
          key={action.actionGeneration}
          api={api}
          request={action}
          dismiss={dismissAction}
          complete={(request, result) => complete(action, request, result)}
        />
      )}
    </>
  );
}

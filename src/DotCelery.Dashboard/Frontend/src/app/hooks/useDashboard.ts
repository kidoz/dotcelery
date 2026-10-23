import { useEffect, useRef, useState } from "react";
import { errorMessage, isAbort } from "../../shared/api/errors.ts";
import { requestJson } from "../../shared/api/client.ts";
import { RequestSession } from "../../shared/api/RequestSession.ts";
import { requestPlan } from "../data/requestPlan.ts";
import type { RequestKey } from "../data/requestPlan.ts";
import type { Route } from "../routing.ts";
import type { Capabilities, Overview } from "../../shared/api/contracts.ts";
import type { DashboardData } from "../../shared/model/dashboardData.ts";

export function useDashboard(
  api: string,
  route: Route,
  paused: boolean,
  blocked: boolean,
) {
  const [data, setData] = useState<DashboardData>({ error: "" });
  const [busy, setBusy] = useState(false);
  const [visible, setVisible] = useState(!document.hidden);
  const [manual, setManual] = useState(0);
  const capabilities = useRef<Capabilities | undefined>(undefined);
  const lastRead = useRef<Partial<Record<RequestKey, number>>>({});
  const previous = useRef({ manual: -1, key: "" });
  const { view, filters, window: metricsWindow } = route;

  useEffect(() => {
    const onVisibility = () => setVisible(!document.hidden);
    document.addEventListener("visibilitychange", onVisibility);
    return () => document.removeEventListener("visibilitychange", onVisibility);
  }, []);

  useEffect(() => {
    const key = JSON.stringify([view, filters, metricsWindow]);
    const force = previous.current.manual !== manual;
    const requested = force || previous.current.key !== key;
    previous.current = { manual, key };
    // Initial deep links need capabilities even while the details dialog blocks polling.
    if (
      !visible ||
      (blocked && capabilities.current) ||
      (paused && !requested)
    ) {
      setBusy(false);
      return;
    }
    const session = new RequestSession();
    const ticket = session.begin();
    const options = { signal: ticket.signal };
    const inFlight = new Map<RequestKey, boolean>();
    const failures = new Map<RequestKey, string>();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let disposed = false;
    const current = () => !disposed && session.isCurrent(ticket);
    const errors = () =>
      failures.size
        ? `${[...new Set(failures.values())].join(" ")} Displayed data may be stale. Use Refresh to try again.`
        : "";

    function poll(caps: Capabilities, forceStatic = false) {
      const active: Route = {
        view,
        filters,
        window: metricsWindow,
        taskId: null,
      };
      for (const job of requestPlan(
        active,
        caps.pageSize,
        caps.hasHistory,
        caps.metricsWindowMinutes,
      )) {
        if (inFlight.has(job.key)) continue;
        if (
          !forceStatic &&
          job.cacheFor &&
          Date.now() - (lastRead.current[job.key] ?? 0) < job.cacheFor
        )
          continue;
        inFlight.set(job.key, job.cacheFor === 0);
        void requestJson<DashboardData[RequestKey]>(
          `${api}/${job.path}`,
          options,
        )
          .then((value) => {
            if (!current()) return;
            lastRead.current[job.key] = Date.now();
            failures.delete(job.key);
            const patch = { [job.key]: value } as Partial<DashboardData>;
            // The overview response already includes queue gauges; avoid a duplicate request.
            if (job.key === "overview") {
              patch.queues = (value as Overview).queues;
              if (metricsWindow === caps.metricsWindowMinutes)
                patch.metrics = (value as Overview).metrics;
            }
            setData((state) => ({
              ...state,
              ...patch,
              error: errors(),
              updatedAt: new Date().toISOString(),
            }));
          })
          .catch((error: unknown) => {
            if (!current() || isAbort(error)) return;
            failures.set(job.key, errorMessage(error));
            setData((state) => ({ ...state, error: errors() }));
          })
          .finally(() => {
            inFlight.delete(job.key);
            if (current()) setBusy([...inFlight.values()].some(Boolean));
          });
      }
      setBusy([...inFlight.values()].some(Boolean));
      // Each endpoint updates independently. A slow filter-list or metrics request
      // neither withholds other responses nor delays the next live-data refresh.
      if (!paused)
        timer = setTimeout(
          () => poll(caps),
          Math.max(1, caps.refreshSeconds) * 1000,
        );
    }

    async function start() {
      try {
        const caps =
          capabilities.current ??
          (await requestJson<Capabilities>(`${api}/capabilities`, options));
        if (!current()) return;
        capabilities.current = caps;
        setData((state) => ({ ...state, capabilities: caps, error: "" }));
        if (!blocked) poll(caps, force);
        else setBusy(false);
      } catch (error) {
        if (!current() || isAbort(error)) return;
        setData((state) => ({
          ...state,
          error: `${errorMessage(error)} Use Refresh to try again.`,
        }));
        setBusy(false);
        if (!paused && !blocked)
          timer = setTimeout(() => {
            void start();
          }, 5000);
      }
    }
    setBusy(true);
    void start();
    return () => {
      disposed = true;
      session.invalidate();
      clearTimeout(timer);
    };
  }, [api, view, filters, metricsWindow, paused, blocked, visible, manual]);

  return { data, busy, refresh: () => setManual((value) => value + 1) };
}

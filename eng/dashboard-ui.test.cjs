const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  duration,
  durationMs,
  workerStatus,
} = require("../src/DotCelery.Dashboard/Frontend/src/shared/lib/format.ts");
const {
  requestJson,
} = require("../src/DotCelery.Dashboard/Frontend/src/shared/api/client.ts");
const {
  RequestSession,
} = require("../src/DotCelery.Dashboard/Frontend/src/shared/api/RequestSession.ts");
const {
  readRoute,
  routeQuery,
} = require("../src/DotCelery.Dashboard/Frontend/src/app/routing.ts");
const {
  taskQuery,
} = require("../src/DotCelery.Dashboard/Frontend/src/features/tasks/api/taskQuery.ts");
const {
  requestPlan,
} = require("../src/DotCelery.Dashboard/Frontend/src/app/data/requestPlan.ts");
const {
  stateLabel,
} = require("../src/DotCelery.Dashboard/Frontend/src/shared/domain/taskState.ts");

test("subsecond task durations retain precision instead of displaying 0s", () => {
  assert.equal(duration("00:00:00.2480000"), "248 ms");
  assert.equal(duration("00:00:00.0000050"), "<1 ms");
  assert.equal(duration("00:00:01.2400000"), "1.24 s");
  assert.equal(durationMs("1.02:03:04.5000000"), 93784500);
  assert.equal(duration(null), "—");
});

test("every numeric worker state has its own readable label", () => {
  assert.deepEqual(
    [0, 1, 2, 3].map((status) => workerStatus({ status })),
    ["Online", "Paused", "Unresponsive", "Offline"],
  );
  assert.equal(workerStatus({ status: "Paused" }), "Paused");
  assert.equal(workerStatus({ status: 99 }), "Unknown");
});

test("newer filter requests invalidate old responses even when abort is ignored", async () => {
  const session = new RequestSession();
  let completeOld;
  const old = session.begin();
  const response = new Promise((resolve) => {
    completeOld = resolve;
  });
  const latest = session.begin();
  completeOld(["stale rows"]);
  await response;
  assert.equal(old.signal.aborted, true);
  assert.equal(session.isCurrent(old), false);
  assert.equal(session.isCurrent(latest), true);
  session.invalidate();
  assert.equal(latest.signal.aborted, true);
  assert.equal(session.isCurrent(latest), false);
});

test("HTTP errors reject actions and preserve useful authorization messages", async () => {
  const forbidden = async () =>
    new Response('{"title":"Forbidden"}', { status: 403 });
  await assert.rejects(requestJson("/api", {}, forbidden), /Access denied/);
  await assert.rejects(
    requestJson(
      "/api",
      {},
      async () => new Response("<html>broken</html>", { status: 503 }),
    ),
    /HTTP 503/,
  );
});

test("state changing requests carry verification headers and preserve cancellation", async () => {
  const controller = new AbortController();
  await requestJson(
    "/api/retry",
    { method: "POST", body: "{}", signal: controller.signal },
    async (url, options) => {
      assert.equal(options.headers.get("X-DotCelery-Dashboard"), "1");
      assert.equal(options.headers.get("Content-Type"), "application/json");
      controller.abort();
      assert.equal(options.signal.aborted, true);
      return new Response('{"successCount":1}', { status: 200 });
    },
  );
});

test("network failures and timeouts produce actionable messages", async () => {
  await assert.rejects(
    requestJson("/api", {}, async () => {
      throw new TypeError("Failed to fetch");
    }),
    /Could not reach the dashboard server/,
  );
  await assert.rejects(
    requestJson("/api", {}, async () => {
      throw new DOMException("Deadline exceeded", "TimeoutError");
    }),
    /timed out/,
  );
});

test("shared links round-trip filters, custom page caps, time windows, and task IDs", () => {
  const route = readRoute(
    "?view=tasks&queue=high%20priority&state=Failure&page=3&size=17&window=1440&task=task%2Fone",
  );
  assert.deepEqual(readRoute(routeQuery(route)), route);
  const query = new URLSearchParams(taskQuery(route.filters, 10));
  assert.equal(query.get("pageSize"), "10");
  assert.equal(query.get("queue"), "high priority");
  assert.equal(query.get("state"), "Failure");
  const invalid = readRoute(
    "?view=__proto__&state=NotAState&page=-2&size=99999&window=30",
  );
  assert.equal(invalid.view, "overview");
  assert.equal(invalid.filters.state, "");
  assert.equal(invalid.filters.page, 1);
  assert.equal(invalid.filters.size, 1000);
  assert.equal(invalid.window, 60);
});

test("Headers instances retain caller headers and cannot replace verification", async () => {
  await requestJson(
    "/api",
    {
      headers: new Headers({
        Accept: "application/json",
        "X-DotCelery-Dashboard": "0",
      }),
    },
    async (_url, options) => {
      assert.equal(options.headers.get("Accept"), "application/json");
      assert.equal(options.headers.get("X-DotCelery-Dashboard"), "1");
      assert.equal(options.credentials, "same-origin");
      return new Response(null, { status: 204 });
    },
  );
});

test("request plans isolate active views and cache task filter metadata", () => {
  assert.equal(stateLabel("__proto__"), "__proto__");
  assert.equal(stateLabel("constructor"), "constructor");
  for (const view of ["workers", "queues"]) {
    assert.deepEqual(
      requestPlan(readRoute(`?view=${view}`), 50, true).map((job) => job.key),
      [view],
    );
  }
  const tasks = requestPlan(readRoute("?view=tasks"), 17, true);
  assert.deepEqual(
    tasks.map((job) => job.key),
    ["tasks", "taskNames", "workers", "queues"],
  );
  assert.equal(tasks.find((job) => job.key === "tasks").cacheFor, 0);
  assert.ok(
    tasks
      .filter((job) => job.key !== "tasks")
      .every((job) => job.cacheFor === 60000),
  );
  const overview = requestPlan(readRoute("?view=overview"), 50, true);
  assert.deepEqual(
    overview.map((job) => job.key),
    ["overview", "workers", "recent", "points"],
  );
  assert.ok(
    requestPlan(readRoute("?window=15"), 50, true).some(
      (job) => job.key === "metrics" && job.path.includes("windowMinutes=15"),
    ),
  );
  assert.ok(
    requestPlan(readRoute(""), 50, true, 120).some(
      (job) => job.key === "metrics",
    ),
  );
  assert.ok(
    !requestPlan(readRoute(""), 50, false).some((job) => job.key === "points"),
  );
});

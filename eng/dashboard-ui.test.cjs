const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  duration,
  durationMs,
  workerStatus,
  requestJson,
  RequestSession,
} = require("../src/DotCelery.Dashboard/wwwroot/dashboard-core.js");

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
      assert.equal(options.headers["X-DotCelery-Dashboard"], "1");
      assert.equal(options.headers["Content-Type"], "application/json");
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

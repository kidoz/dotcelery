import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

const prefix = "/jobs/monitor";
const task = {
  taskId: "task-123",
  taskName: "demo.email",
  state: "Failure",
  queue: "celery",
  worker: "worker-1",
  duration: "00:00:00.2480000",
  retries: 2,
  sentAt: "2026-10-23T09:00:00Z",
  exceptionMessage: "<script>window.injected = true</script>",
};
const caps = {
  readOnly: false,
  canCancel: true,
  canRetry: true,
  hasHistory: false,
  hasTaskRecords: true,
  retentionSeconds: 86400,
  snapshotSeconds: 60,
  refreshSeconds: 1,
  metricsWindowMinutes: 60,
  pageSize: 17,
};
const metrics = {
  failureCount: 1,
  tasksPerSecond: 2,
  averageExecutionTime: task.duration,
  byTaskName: { "demo.email": { totalCount: 4, failureCount: 1 } },
};
const queues = [
  { name: "celery", messageCount: 2, inProgressCount: 1, consumerCount: 1 },
];
const workers = [
  {
    workerId: "worker-1",
    hostname: "demo-host",
    status: 1,
    queues: ["celery"],
    activeTasks: 1,
    concurrency: 4,
    processedCount: 3,
    lastHeartbeat: "2026-10-23T09:00:00Z",
  },
];

async function fixture(page: Page, overrides: Partial<typeof caps> = {}) {
  await page.route(`**${prefix}/api/v1/**`, async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.slice(`${prefix}/api/v1/`.length);
    let body: unknown;
    switch (path) {
      case "capabilities":
        body = { ...caps, ...overrides };
        break;
      case "overview":
        body = {
          metrics,
          stateCounts: { Failure: 1 },
          queues,
          activeWorkers: 0,
          workerCount: 1,
        };
        break;
      case "metrics":
        body = metrics;
        break;
      case "metrics/timeseries":
        body = [
          {
            timestamp: "2026-10-23T09:00:00Z",
            successCount: 3,
            failureCount: 1,
          },
        ];
        break;
      case "workers":
        body = workers;
        break;
      case "queues":
        body = queues;
        break;
      case "tasks/search":
        body = {
          tasks: [task],
          page: Number(url.searchParams.get("page") ?? 1),
          pageSize: 17,
          hasMore: true,
        };
        break;
      case "task-names":
        body = [task.taskName];
        break;
      case `tasks/${task.taskId}`:
        body = task;
        break;
      case "tasks/actions/preview":
        body = { retryTaskIds: [task.taskId], cancelTaskIds: [] };
        break;
      case "tasks/bulk/retry":
        expect(route.request().headers()["x-dotcelery-dashboard"]).toBe("1");
        expect(route.request().postDataJSON()).toEqual({
          taskIds: [task.taskId],
          terminate: true,
        });
        body = { successCount: 1, failureCount: 0 };
        break;
      default:
        await route.fulfill({ status: 404 });
        return;
    }
    await route.fulfill({ json: body });
  });
}

test("built embedded assets load at nested prefixes, slashless roots, and SPA paths", async ({
  page,
}, testInfo) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await fixture(page);
  await page.goto(prefix);
  await expect(
    page.getByRole("heading", { name: "Execution overview" }),
  ).toBeVisible();
  await expect(page.getByText("248 ms").first()).toBeVisible();
  await expect(
    page.getByRole("img", { name: "DotCelery", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Historical metrics are not configured."),
  ).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("overview-desktop.png"),
    fullPage: true,
  });
  await page
    .getByRole("navigation", { name: "Dashboard" })
    .getByRole("link", { name: "Queues" })
    .click();
  await expect(
    page.getByRole("heading", { name: "Message queues" }),
  ).toBeVisible();
  await page
    .getByRole("navigation")
    .getByRole("link", { name: "Workers" })
    .click();
  await expect(page.getByText("Paused", { exact: true })).toBeVisible();
  await page.goto(`${prefix}/deep/path?view=workers`);
  await expect(page.getByText("demo-host")).toBeVisible();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({
    path: testInfo.outputPath("workers-mobile.png"),
    fullPage: true,
  });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  expect(errors).toEqual([]);
});

test("filters, paging, selection, preview, and retry preserve the API contract", async ({
  page,
}) => {
  await fixture(page);
  await page.goto(`${prefix}/?view=tasks`);
  await page
    .getByRole("combobox", { name: "State", exact: true })
    .selectOption("Failure");
  await page.getByLabel("Task ID or name contains").fill("email");
  const request = page.waitForRequest(
    (request) =>
      request.url().includes("tasks/search?") &&
      new URL(request.url()).searchParams.get("search") === "email",
  );
  await page.getByRole("button", { name: "Search", exact: true }).click();
  const query = new URL((await request).url()).searchParams;
  expect(query.get("state")).toBe("Failure");
  expect(query.get("pageSize")).toBe("17");
  await expect(page).toHaveURL(/search=email/);
  await page.getByRole("button", { name: "Next →" }).click();
  await expect(page.getByText("Page 2", { exact: true })).toBeVisible();
  await page.getByLabel("Select all tasks on this page").check();
  await expect(page.getByText("Updates paused for selection")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Refresh dashboard" }),
  ).toBeDisabled();
  await page.getByRole("button", { name: "Retry selected" }).click();
  await expect(page.getByRole("dialog", { name: "Retry tasks" })).toContainText(
    "1 of 1",
  );
  await page.getByRole("button", { name: "Retry 1 task", exact: true }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "1 task requeued." }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
});

test("deep-linked details escape exceptions and browser Back restores navigation", async ({
  page,
}) => {
  await fixture(page);
  await page.goto(`${prefix}/?view=tasks&task=task-123`);
  await expect(page.getByRole("dialog")).toContainText(task.exceptionMessage);
  await expect(
    page.getByRole("button", { name: "Retry task", exact: true }),
  ).toBeVisible();
  await expect(page.locator("dialog script")).toHaveCount(0);
  await page.getByRole("button", { name: "Close task details" }).click();
  await expect(page).not.toHaveURL(/task=task-123/);
  await page.getByRole("link", { name: "demo.email", exact: true }).click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.goBack();
  await expect(page.getByRole("dialog")).toHaveCount(0);
});

test("read-only dashboards hide mutation and selection controls", async ({
  page,
}) => {
  await fixture(page, { readOnly: true, canCancel: false, canRetry: false });
  const posts: string[] = [];
  page.on("request", (request) => {
    if (request.method() === "POST") posts.push(request.url());
  });
  await page.goto(`${prefix}/?view=tasks&task=task-123`);
  await expect(page.getByRole("dialog")).toContainText("Read-only access");
  await page.getByRole("button", { name: "Close task details" }).click();
  await expect(page.getByText("Read only", { exact: true })).toBeVisible();
  await expect(page.getByRole("checkbox")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Retry selected" }),
  ).toHaveCount(0);
  expect(posts).toEqual([]);
});

test("uncertain action results require a fresh preview before another mutation", async ({
  page,
}) => {
  await fixture(page);
  let mutations = 0;
  await page.route(`**${prefix}/api/v1/tasks/bulk/retry`, async (route) => {
    mutations++;
    await route.fulfill({ status: 503, body: "<html>offline</html>" });
  });
  await page.goto(`${prefix}/?view=tasks`);
  await page.getByLabel("Select all tasks on this page").check();
  await page.getByRole("button", { name: "Retry selected" }).click();
  await page.getByRole("button", { name: "Retry 1 task", exact: true }).click();
  await expect(page.getByRole("dialog")).toContainText(
    "The result may be uncertain",
  );
  await expect(
    page.getByRole("button", { name: "Retry 1 task", exact: true }),
  ).toBeDisabled();
  expect(mutations).toBe(1);
});

test("late filter responses cannot replace newer task results", async ({
  page,
}) => {
  await fixture(page);
  let releaseOld: (() => void) | undefined;
  const oldStarted = new Promise<void>((resolve) => {
    void page.route(`**${prefix}/api/v1/tasks/search?**`, async (route) => {
      const search = new URL(route.request().url()).searchParams.get("search");
      if (search === "old") {
        resolve();
        await new Promise<void>((release) => {
          releaseOld = release;
        });
      }
      await route.fulfill({
        json: {
          tasks: [
            {
              ...task,
              taskName:
                search === "old"
                  ? "old-result"
                  : search === "new"
                    ? "new-result"
                    : task.taskName,
            },
          ],
          page: 1,
          pageSize: 17,
          hasMore: false,
        },
      });
    });
  });
  await page.goto(`${prefix}/?view=tasks`);
  await page.getByLabel("Task ID or name contains").fill("old");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await oldStarted;
  await page.getByLabel("Task ID or name contains").fill("new");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await expect(
    page.getByRole("link", { name: "new-result", exact: true }),
  ).toBeVisible();
  releaseOld?.();
  await expect(
    page.getByRole("link", { name: "old-result", exact: true }),
  ).toHaveCount(0);
});

test("the migrated UI also loads real data from the demo backend", async ({
  page,
}) => {
  await page.goto(`${prefix}/`);
  await expect(
    page.getByRole("heading", { name: "Execution overview" }),
  ).toBeVisible();
  await expect(page.getByText("Polling · 3s")).toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("task results do not wait for filter metadata, and static lists are cached", async ({
  page,
}) => {
  await fixture(page);
  await page.clock.install();
  const paths: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/"))
      paths.push(new URL(request.url()).pathname);
  });
  let releaseWorkers: (() => void) | undefined;
  await page.route(`**${prefix}/api/v1/workers`, async (route) => {
    await new Promise<void>((resolve) => {
      releaseWorkers = resolve;
    });
    await route.fulfill({ json: workers });
  });
  try {
    await page.goto(`${prefix}/?view=tasks`);
    await expect(
      page.getByRole("link", { name: "demo.email", exact: true }),
    ).toBeVisible();
    await expect(
      page
        .getByRole("combobox", { name: "Task name", exact: true })
        .getByRole("option", { name: "demo.email" }),
    ).toHaveCount(1);
    await page.clock.runFor(2500);
    await expect
      .poll(() => paths.filter((path) => path.endsWith("tasks/search")).length)
      .toBeGreaterThan(1);
    expect(paths.filter((path) => path.endsWith("task-names"))).toHaveLength(1);
    expect(paths.filter((path) => path.endsWith("queues"))).toHaveLength(1);
    expect(
      paths.some(
        (path) => path.endsWith("overview") || path.endsWith("metrics"),
      ),
    ).toBe(false);
    releaseWorkers?.();
    await page.clock.fastForward(60000);
    await expect
      .poll(() => paths.filter((path) => path.endsWith("task-names")).length)
      .toBe(2);
  } finally {
    releaseWorkers?.();
  }
});

test("polling stops for dialogs, selection, and explicit pause, with accurate status", async ({
  page,
}) => {
  await fixture(page);
  await page.clock.install();
  let taskRequests = 0;
  page.on("request", (request) => {
    if (request.url().includes("tasks/search?")) taskRequests++;
  });
  await page.goto(`${prefix}/?view=tasks`);
  await expect(
    page.getByRole("link", { name: "demo.email", exact: true }),
  ).toBeVisible();
  await page.getByRole("link", { name: "demo.email", exact: true }).click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(page.locator("header")).toContainText(
    "Updates paused while a dialog is open",
  );
  let before = taskRequests;
  await page.clock.runFor(2500);
  expect(taskRequests).toBe(before);
  await page.getByRole("button", { name: "Close task details" }).click();
  await expect.poll(() => taskRequests).toBeGreaterThan(before);
  await page.getByLabel("Select all tasks on this page").check();
  await expect(page.locator("header")).toContainText(
    "Updates paused for selection",
  );
  before = taskRequests;
  await page.clock.runFor(2500);
  expect(taskRequests).toBe(before);
  await page.getByRole("button", { name: "Retry selected" }).click();
  await expect(page.getByRole("dialog")).toBeVisible();
  before = taskRequests;
  await page.clock.runFor(2500);
  expect(taskRequests).toBe(before);
  await page.getByRole("button", { name: "Go back" }).click();
  await page.getByRole("button", { name: "Clear selection" }).click();
  await page.getByRole("button", { name: "Pause updates" }).click();
  await expect(page.locator("header")).toContainText("Updates paused");
  before = taskRequests;
  await page.clock.runFor(2500);
  expect(taskRequests).toBe(before);
  await page.getByRole("button", { name: "Refresh dashboard" }).click();
  await expect.poll(() => taskRequests).toBeGreaterThan(before);
});

test("overview reuses embedded metrics and requests alternate windows only when needed", async ({
  page,
}) => {
  await fixture(page, { hasHistory: true });
  const paths: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/"))
      paths.push(new URL(request.url()).pathname);
  });
  await page.goto(`${prefix}/`);
  await expect(
    page.getByRole("img", { name: /Task completions, last hour/ }),
  ).toBeVisible();
  await expect(page.getByText("248 ms").first()).toBeVisible();
  expect(
    paths.some((path) => path.endsWith("/metrics") || path.endsWith("/queues")),
  ).toBe(false);
  const alternate = page.waitForRequest((request) =>
    request.url().includes("metrics?windowMinutes=15"),
  );
  await page.getByLabel("Metrics time window").selectOption("15");
  await alternate;
  await expect(
    page.getByRole("img", { name: /Task completions, last 15 minutes/ }),
  ).toBeVisible();
});

test("cancellation previews retain cooperative semantics and report acceptance", async ({
  page,
}) => {
  await fixture(page);
  await page.route(`**${prefix}/api/v1/tasks/actions/preview`, (route) =>
    route.fulfill({ json: { cancelTaskIds: [task.taskId], retryTaskIds: [] } }),
  );
  await page.route(`**${prefix}/api/v1/tasks/bulk/revoke`, async (route) => {
    expect(route.request().postDataJSON()).toEqual({
      taskIds: [task.taskId],
      terminate: true,
    });
    await route.fulfill({ json: { successCount: 1, failureCount: 0 } });
  });
  await page.goto(`${prefix}/?view=tasks`);
  await page.getByLabel("Select all tasks on this page").check();
  await page
    .getByRole("button", { name: "Cancel selected", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toContainText(
    "Cancellation is cooperative",
  );
  await page
    .getByRole("button", { name: "Request cancellation (1)", exact: true })
    .click();
  await expect(
    page
      .getByRole("status")
      .filter({ hasText: "1 cancellation request accepted." }),
  ).toBeVisible();
});

for (const openAnotherAction of [false, true]) {
  test(`late mutation completion preserves ${openAnotherAction ? "a newer action dialog" : "the browser Back destination"}`, async ({
    page,
  }) => {
    await fixture(page);
    let markSubmitted = () => {};
    const submitted = new Promise<void>((resolve) => {
      markSubmitted = resolve;
    });
    let releaseMutation = () => {};
    const mutationGate = new Promise<void>((resolve) => {
      releaseMutation = resolve;
    });
    await page.route(`**${prefix}/api/v1/tasks/bulk/retry`, async (route) => {
      markSubmitted();
      await mutationGate;
      await route.fulfill({ json: { successCount: 1, failureCount: 0 } });
    });
    try {
      await page.goto(`${prefix}/?view=overview`);
      await page
        .getByRole("navigation")
        .getByRole("link", { name: /^Tasks/ })
        .click();
      await page.getByLabel("Select all tasks on this page").check();
      await page.getByRole("button", { name: "Retry selected" }).click();
      await page
        .getByRole("button", { name: "Retry 1 task", exact: true })
        .click();
      await submitted;
      await page.goBack();
      await expect(
        page.getByRole("heading", { name: "Execution overview" }),
      ).toBeVisible();
      if (openAnotherAction) {
        await page
          .getByRole("navigation")
          .getByRole("link", { name: /^Tasks/ })
          .click();
        await page.getByLabel("Select all tasks on this page").check();
        await page.getByRole("button", { name: "Retry selected" }).click();
        await expect(
          page.getByRole("dialog", { name: "Retry tasks" }),
        ).toContainText("1 of 1");
      }
      const destination = page.url();
      releaseMutation();
      // The submitted POST still finishes and reports its result after the old dialog unmounts.
      await expect(
        page.getByRole("status").filter({ hasText: "1 task requeued." }),
      ).toBeAttached();
      await expect(page).toHaveURL(destination);
      if (openAnotherAction) {
        await expect(
          page.getByRole("dialog", { name: "Retry tasks" }),
        ).toBeVisible();
        await expect(
          page.getByRole("button", { name: "Retry 1 task", exact: true }),
        ).toBeEnabled();
      } else {
        await expect(
          page.getByRole("heading", { name: "Execution overview" }),
        ).toBeVisible();
        await expect(page.getByRole("dialog")).toHaveCount(0);
      }
    } finally {
      releaseMutation();
    }
  });
}

test("changed filters hide old actionable rows until their own results arrive", async ({
  page,
}) => {
  await fixture(page);
  let markRequested = () => {};
  const requested = new Promise<void>((resolve) => {
    markRequested = resolve;
  });
  let releaseQuery = () => {};
  const queryGate = new Promise<void>((resolve) => {
    releaseQuery = resolve;
  });
  await page.route(`**${prefix}/api/v1/tasks/search?**`, async (route) => {
    if (
      new URL(route.request().url()).searchParams.get("state") !== "Started"
    ) {
      await route.fallback();
      return;
    }
    markRequested();
    await queryGate;
    await route.fulfill({
      json: {
        tasks: [
          {
            ...task,
            taskId: "task-new",
            taskName: "new-task",
            state: "Started",
          },
        ],
        page: 1,
        pageSize: 17,
        hasMore: false,
      },
    });
  });
  try {
    await page.goto(`${prefix}/?view=tasks`);
    await expect(
      page.getByLabel(`Select ${task.taskName} ${task.taskId}`, {
        exact: true,
      }),
    ).toBeVisible();
    await page
      .getByRole("combobox", { name: "State", exact: true })
      .selectOption("Started");
    await requested;
    await expect(page).toHaveURL(/state=Started/);
    await expect(
      page.getByLabel(`Select ${task.taskName} ${task.taskId}`, {
        exact: true,
      }),
    ).toHaveCount(0);
    await expect(
      page.getByLabel("Select all tasks on this page"),
    ).toBeDisabled();
    await expect(
      page.getByRole("button", { name: "Retry selected" }),
    ).toBeDisabled();
    await expect(
      page.getByRole("status").filter({ hasText: "Loading tasks…" }),
    ).toBeVisible();
    releaseQuery();
    await expect(
      page.getByRole("link", { name: "new-task", exact: true }),
    ).toBeVisible();
    await page.getByLabel("Select new-task task-new", { exact: true }).check();
    await expect(page.getByText("1 selected", { exact: true })).toBeVisible();
  } finally {
    releaseQuery();
  }
});

test("cancellation details stop claiming a pending request after a terminal outcome", async ({
  page,
}) => {
  await fixture(page);
  let outcome = "Started";
  await page.route(`**${prefix}/api/v1/tasks/${task.taskId}`, (route) =>
    route.fulfill({ json: { ...task, state: outcome } }),
  );
  await page.route(`**${prefix}/api/v1/tasks/actions/preview`, (route) =>
    route.fulfill({
      json: {
        cancelTaskIds: outcome === "Started" ? [task.taskId] : [],
        retryTaskIds: [],
      },
    }),
  );
  await page.route(`**${prefix}/api/v1/tasks/bulk/revoke`, (route) =>
    route.fulfill({ json: { successCount: 1, failureCount: 0 } }),
  );
  await page.goto(`${prefix}/?view=tasks`);
  await page.getByLabel("Select all tasks on this page").check();
  await page
    .getByRole("button", { name: "Cancel selected", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Request cancellation (1)", exact: true })
    .click();
  await expect(
    page
      .getByRole("status")
      .filter({ hasText: "1 cancellation request accepted." }),
  ).toBeVisible();
  for (const state of [
    "Started",
    "Success",
    "Failure",
    "Revoked",
    "Rejected",
  ]) {
    outcome = state;
    await page.getByRole("link", { name: "demo.email", exact: true }).click();
    const dialog = page.getByRole("dialog");
    const label =
      state === "Started" ? "Running" : state === "Failure" ? "Failed" : state;
    await expect(
      dialog.getByText(label, { exact: true }).first(),
    ).toBeVisible();
    const pending = dialog.getByText(/The worker outcome will confirm whether/);
    if (state === "Started") await expect(pending).toBeVisible();
    else await expect(pending).toHaveCount(0);
    await page.getByRole("button", { name: "Close task details" }).click();
  }
});

/* The dashboard is shipped as embedded assets and makes only same-origin requests. */
(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const prefix = document.body.dataset.prefix;
  const api = `${prefix}/api`;
  const taskStates = [
    "Pending",
    "Received",
    "Started",
    "Success",
    "Failure",
    "Retry",
    "Progress",
    "Revoked",
    "Rejected",
    "Requeued",
  ];
  const stateLabels = { Started: "Running", Failure: "Failed" };
  const workerStates = ["Online", "Paused", "Unresponsive", "Offline"];
  const views = {
    overview: ["Execution overview", "A clear view of your background work."],
    tasks: [
      "Task history",
      "Find an execution, understand its outcome, and take the next step.",
    ],
    queues: [
      "Queue health",
      "See where work is waiting and which queues have consumers.",
    ],
    workers: [
      "Worker fleet",
      "Monitor availability, heartbeats, and execution capacity.",
    ],
  };
  const filters = {
    search: "",
    state: "",
    name: "",
    queue: "",
    worker: "",
    range: "",
    order: "newest",
    page: 1,
    size: 25,
  };
  let view = "overview";
  let metricsWindow = 60;
  let paused = false;
  let capabilities = null;
  let refreshTimer = null;
  const refreshSession = new DotCeleryDashboard.RequestSession();
  let lastSuccess = null;
  let lastAttemptFailed = false;
  let currentTasks = [];
  let detailId = null;
  let detailRevision = 0;
  let actionPlan = null;
  let detailActions = { cancelTaskIds: [], retryTaskIds: [] };
  let actionRunning = false;
  let actionRevision = 0;
  const selected = new Set();
  const cancellationRequests = new Set();
  const number = (value) =>
    new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(
      Number(value) || 0,
    );
  const esc = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (char) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[char],
    );
  const timeValue = (task) => task.completedAt || task.startedAt || task.sentAt;
  const windowLabel = () =>
    metricsWindow === 15
      ? "Last 15 minutes"
      : metricsWindow === 1440
        ? "Last 24 hours"
        : "Last hour";
  const taskUrl = (id) => {
    const query = new URLSearchParams(location.search);
    query.set("view", "tasks");
    query.set("task", id);
    return `${prefix}/?${query}`;
  };
  const badge = (state) => {
    const known = [
      ...taskStates,
      ...workerStates,
      "Healthy",
      "Warning",
    ].includes(state);
    return `<span class="badge badge-${known ? state.toLowerCase() : "unknown"}">${esc(stateLabels[state] || state || "Unknown")}</span>`;
  };
  const duration = DotCeleryDashboard.duration;
  function exactTime(value) {
    if (!value) return "Not recorded";
    const date = new Date(value);
    return Number.isNaN(date.valueOf())
      ? "Not recorded"
      : date.toLocaleString(undefined, { timeZoneName: "short" });
  }
  function relativeTime(value) {
    if (!value) return "—";
    const seconds = Math.round((new Date(value).valueOf() - Date.now()) / 1000);
    if (!Number.isFinite(seconds)) return "—";
    const formatter = new Intl.RelativeTimeFormat(undefined, {
      numeric: "auto",
    });
    if (Math.abs(seconds) < 60) return formatter.format(seconds, "second");
    if (Math.abs(seconds) < 3600)
      return formatter.format(Math.round(seconds / 60), "minute");
    if (Math.abs(seconds) < 86400)
      return formatter.format(Math.round(seconds / 3600), "hour");
    return formatter.format(Math.round(seconds / 86400), "day");
  }
  const timeCell = (value) =>
    `<time ${value ? `datetime="${esc(value)}"` : ""} title="${esc(exactTime(value))}">${esc(relativeTime(value))}</time>`;
  const emptyRow = (columns, message) =>
    `<tr><td colspan="${columns}" class="empty">${esc(message)}</td></tr>`;
  const taskLink = (task) =>
    `<a class="task-link" href="${esc(taskUrl(task.taskId))}" data-task-id="${esc(task.taskId)}" data-focus="task-${esc(task.taskId)}">${esc(task.taskName)}</a><span class="subtext" title="${esc(task.taskId)}">${esc(task.taskId.slice(0, 12))}…</span>`;

  const fetchJson = (path, options) =>
    DotCeleryDashboard.requestJson(`${api}/${path}`, options);
  function showMessage(text, error = false) {
    const element = $(error ? "error-banner" : "message");
    element.textContent = text;
    element.hidden = !text;
  }
  function setConnection() {
    $("connection-label").textContent = lastAttemptFailed
      ? "Data may be stale"
      : paused || selected.size
        ? selected.size
          ? "Updates paused for selection"
          : "Updates paused"
        : capabilities
          ? `Polling · ${capabilities.refreshSeconds}s`
          : "Connecting";
    $("connection-dot").classList.toggle(
      "stale",
      lastAttemptFailed || paused || selected.size > 0,
    );
    $("last-updated").textContent = lastSuccess
      ? `Updated ${new Date(lastSuccess).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`
      : "Waiting for data";
  }
  function renderPreservingFocus(id, html) {
    const target = $(id);
    const active = target.contains(document.activeElement)
      ? document.activeElement.dataset.focus
      : null;
    const scroll = target.closest(".table-scroll");
    const scrollLeft = scroll?.scrollLeft;
    if (target.innerHTML !== html) target.innerHTML = html;
    if (active) {
      const replacement = [...target.querySelectorAll("[data-focus]")].find(
        (element) => element.dataset.focus === active,
      );
      replacement?.focus({ preventScroll: true });
    }
    if (scroll) scroll.scrollLeft = scrollLeft;
  }
  function renderStats(overview, metrics) {
    const counts = overview.stateCounts || {};
    const queues = overview.queues || [];
    const waiting = queues.reduce((sum, queue) => sum + queue.messageCount, 0);
    const running = queues.length
      ? queues.reduce((sum, queue) => sum + queue.inProgressCount, 0)
      : (counts.Started || 0) + (counts.Progress || 0);
    const cards = [
      ["Queued", number(waiting), "Now · waiting in queues", "⇉"],
      ["Running", number(running), "Now · tasks in progress", "↗"],
      ["Failed", number(metrics.failureCount), windowLabel(), "!", true],
      [
        "Workers online",
        `${number(overview.activeWorkers)} <span class="muted" style="font-size:15px">/ ${number(overview.workerCount)}</span>`,
        "Now · registered workers",
        "▤",
      ],
      [
        "Throughput",
        `${number(metrics.tasksPerSecond)}<span class="muted" style="font-size:12px;letter-spacing:0"> /s</span>`,
        windowLabel(),
        "⌁",
      ],
      [
        "Avg. duration",
        esc(duration(metrics.averageExecutionTime)),
        windowLabel(),
        "◷",
      ],
    ];
    $("stats").innerHTML = cards
      .map(
        ([label, value, meta, icon, danger]) =>
          `<article class="stat-card"><h3 class="stat-label"><span class="stat-icon" aria-hidden="true">${icon}</span>${label}</h3><div class="stat-value ${danger ? "danger-text" : ""}">${value}</div><p class="stat-meta">${esc(meta)}</p></article>`,
      )
      .join("");
    $("stats").setAttribute("aria-busy", "false");
    $("task-count").textContent = number(
      Object.values(counts).reduce((sum, value) => sum + value, 0),
    );
    Object.entries(counts).forEach(([state, count]) => {
      const option = [...$("state").options].find(
        (item) => item.value === state,
      );
      if (option)
        option.textContent = `${stateLabels[state] || state} (${number(count)})`;
    });
  }
  function renderAttention(metrics) {
    const tasks = Object.entries(metrics.byTaskName || {})
      .filter(([, value]) => value.failureCount > 0)
      .sort((a, b) => b[1].failureCount - a[1].failureCount)
      .slice(0, 4);
    $("attention").innerHTML = tasks.length
      ? tasks
          .map(
            ([name, value]) =>
              `<div class="attention-item"><div><a href="?view=tasks" data-task-name="${esc(name)}" data-failure="true">${esc(name)}</a><small>${number(value.totalCount)} executions · ${number(value.totalCount ? (value.failureCount / value.totalCount) * 100 : 0)}% failed</small></div><span class="badge badge-failure">${number(value.failureCount)} failed</span></div>`,
          )
          .join("")
      : '<p class="attention-clear">No task failures in this window.<br>Failures will appear here when recorded.</p>';
  }
  function renderChart(points) {
    $("chart").setAttribute("aria-busy", "false");
    if (!capabilities.hasHistory) {
      $("chart").innerHTML =
        '<p class="empty">Historical metrics are not configured.<br>Live gauges and task history remain available.</p>';
      $("chart-note").textContent = "";
      return;
    }
    if (!points?.length) {
      $("chart").innerHTML =
        '<p class="empty">Waiting for the first metrics snapshot.<br>Task completions will appear here once collected.</p>';
      $("chart-note").textContent =
        `Snapshots are recorded every ${number(capabilities.snapshotSeconds)} seconds.`;
      return;
    }
    const width = 640,
      height = 190,
      left = 38,
      right = 12,
      top = 14,
      bottom = 29;
    const max = Math.max(
      1,
      ...points.map(
        (point) => Number(point.successCount) + Number(point.failureCount),
      ),
    );
    const plotWidth = width - left - right,
      plotHeight = height - top - bottom;
    const step = plotWidth / points.length;
    const barWidth = Math.min(28, step * 0.65);
    const grid = [0, 0.5, 1]
      .map((fraction) => {
        const y = top + plotHeight * (1 - fraction);
        return `<line x1="${left}" y1="${y}" x2="${width - right}" y2="${y}" stroke="#e9eef1" stroke-dasharray="3 4"/><text x="${left - 9}" y="${y + 3}" text-anchor="end">${number(max * fraction)}</text>`;
      })
      .join("");
    const bars = points
      .map((point, index) => {
        const x = left + step * index + (step - barWidth) / 2;
        const success =
          (Math.max(0, Number(point.successCount) || 0) / max) * plotHeight;
        const failure =
          (Math.max(0, Number(point.failureCount) || 0) / max) * plotHeight;
        return `<g><title>${esc(exactTime(point.timestamp))}: ${number(point.successCount)} successful, ${number(point.failureCount)} failed</title><rect x="${x}" y="${top + plotHeight - success}" width="${barWidth}" height="${success}" rx="2" fill="var(--brand)"/><rect x="${x}" y="${top + plotHeight - success - failure}" width="${barWidth}" height="${failure}" rx="2" fill="var(--danger)"/></g>`;
      })
      .join("");
    const labelIndexes = [
      ...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1]),
    ];
    const labels = labelIndexes
      .map(
        (index) =>
          `<text x="${left + step * (index + 0.5)}" y="${height - 7}" text-anchor="middle">${esc(new Date(points[index].timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }))}</text>`,
      )
      .join("");
    const totalSuccess = points.reduce(
        (sum, point) => sum + point.successCount,
        0,
      ),
      totalFailure = points.reduce((sum, point) => sum + point.failureCount, 0);
    $("chart").innerHTML =
      `<svg viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="chart-title chart-description"><title id="chart-title">Task completions, ${esc(windowLabel().toLowerCase())}</title><desc id="chart-description">${number(totalSuccess)} successful and ${number(totalFailure)} failed completions. Bars show counts per time bucket.</desc>${grid}${bars}${labels}</svg>`;
    $("chart-note").textContent =
      `Counts per ${metricsWindow === 1440 ? "hour" : "minute"} · local time · snapshots every ${number(capabilities.snapshotSeconds)}s`;
  }
  function renderRecent(tasks) {
    renderPreservingFocus(
      "recent-body",
      tasks.length
        ? tasks
            .map(
              (task) =>
                `<tr><td>${taskLink(task)}</td><td>${badge(task.state)}</td><td>${esc(task.worker || "—")}</td><td>${esc(duration(task.duration))}</td><td>${timeCell(timeValue(task))}</td></tr>`,
            )
            .join("")
        : emptyRow(
            5,
            capabilities.hasTaskRecords
              ? "No task activity recorded yet."
              : "Task history is unavailable: no task storage is configured.",
          ),
    );
  }
  const workerStatus = DotCeleryDashboard.workerStatus;
  function queueHealth(queue) {
    return queue.messageCount > 0 && queue.consumerCount === 0
      ? "Warning"
      : "Healthy";
  }
  function renderQueues(queues) {
    renderPreservingFocus(
      "queues-body",
      queues.length
        ? queues
            .map(
              (queue) =>
                `<tr><td><a href="?view=tasks" data-queue="${esc(queue.name)}" class="task-link">${esc(queue.name)}</a></td><td>${queueHealth(queue) === "Warning" ? badge("Warning") + ' <span class="muted">No consumers</span>' : badge("Healthy")}</td><td>${number(queue.messageCount)}</td><td>${number(queue.inProgressCount)}</td><td>${number(queue.consumerCount)}</td><td>${timeCell(queue.lastMessageAt)}</td></tr>`,
            )
            .join("")
        : emptyRow(
            6,
            "No queue metrics recorded yet. Queue metrics appear as configured stores receive activity.",
          ),
    );
    $("queue-summary").innerHTML = queues.length
      ? queues
          .slice(0, 3)
          .map(
            (queue) =>
              `<div class="mini-row"><div><a href="?view=tasks" data-queue="${esc(queue.name)}">${esc(queue.name)}</a><small>${queueHealth(queue) === "Warning" ? "Waiting work has no active consumers" : `${number(queue.consumerCount)} consumers available`}</small></div><div class="mini-values"><span><strong>${number(queue.messageCount)}</strong> waiting</span><span><strong>${number(queue.inProgressCount)}</strong> running</span></div></div>`,
          )
          .join("")
      : '<p class="empty">No queue metrics recorded yet.</p>';
  }
  function renderWorkers(workers) {
    renderPreservingFocus(
      "workers-body",
      workers.length
        ? workers
            .map(
              (worker) =>
                `<tr><td><a href="?view=tasks" data-worker="${esc(worker.workerId)}" class="task-link">${esc(worker.workerId)}</a><span class="subtext">${esc(worker.hostname)}</span></td><td>${badge(workerStatus(worker))}</td><td>${esc((worker.queues || []).join(", ") || "—")}</td><td><span class="capacity"><progress value="${Math.max(0, worker.activeTasks || 0)}" max="${Math.max(1, worker.concurrency || 1)}" aria-label="${esc(worker.workerId)} execution capacity"></progress>${number(worker.activeTasks)} / ${number(worker.concurrency)}</span></td><td>${number(worker.processedCount)}</td><td>${timeCell(worker.lastHeartbeat)}</td></tr>`,
            )
            .join("")
        : emptyRow(
            6,
            "No registered workers. Check the worker host and its shared registry.",
          ),
    );
    $("worker-summary").innerHTML = workers.length
      ? workers
          .slice(0, 3)
          .map(
            (worker) =>
              `<div class="mini-row"><div><a href="?view=tasks" data-worker="${esc(worker.workerId)}">${esc(worker.workerId)}</a><small>${number(worker.activeTasks)} / ${number(worker.concurrency)} slots active · heartbeat ${esc(relativeTime(worker.lastHeartbeat))}</small></div>${badge(workerStatus(worker))}</div>`,
          )
          .join("")
      : '<p class="empty">No workers registered.</p>';
  }
  function updateOptions(id, values, emptyLabel, value) {
    const sorted = [...new Set(values.filter(Boolean))].sort();
    if (value && !sorted.includes(value)) sorted.push(value);
    const html =
      `<option value="">${emptyLabel}</option>` +
      sorted
        .map((item) => `<option value="${esc(item)}">${esc(item)}</option>`)
        .join("");
    if (document.activeElement !== $(id) && $(id).innerHTML !== html)
      $(id).innerHTML = html;
    $(id).value = value;
  }
  function taskQuery(size = filters.size) {
    const query = new URLSearchParams({
      page: filters.page,
      pageSize: size,
      oldestFirst: filters.order === "oldest",
    });
    for (const [key, value] of Object.entries({
      search: filters.search,
      state: filters.state,
      taskName: filters.name,
      queue: filters.queue,
      worker: filters.worker,
    }))
      if (value) query.set(key, value);
    if (filters.range)
      query.set(
        "from",
        new Date(Date.now() - Number(filters.range) * 60000).toISOString(),
      );
    return query;
  }
  function renderTasks(data) {
    currentTasks = data.tasks;
    const visibleIds = new Set(currentTasks.map((task) => task.taskId));
    for (const id of selected) if (!visibleIds.has(id)) selected.delete(id);
    renderPreservingFocus(
      "tasks-body",
      currentTasks.length
        ? currentTasks
            .map(
              (task) =>
                `<tr><td class="selection-column">${capabilities.readOnly ? "" : `<input type="checkbox" data-select-id="${esc(task.taskId)}" data-focus="select-${esc(task.taskId)}" aria-label="Select ${esc(task.taskName)} ${esc(task.taskId)}" ${selected.has(task.taskId) ? "checked" : ""}/>`}</td><td>${taskLink(task)}</td><td>${badge(task.state)}${cancellationRequests.has(task.taskId) && !["Revoked", "Success", "Failure", "Rejected"].includes(task.state) ? '<small class="subtext">Cancellation requested</small>' : ""}</td><td>${esc(task.queue || "—")}</td><td>${esc(task.worker || "—")}</td><td>${esc(duration(task.duration))}</td><td>${timeCell(timeValue(task))}</td></tr>`,
            )
            .join("")
        : emptyRow(
            7,
            capabilities.hasTaskRecords
              ? "No tasks match these filters. Clear a filter or choose a wider time range."
              : "Task history is unavailable: no task storage is configured.",
          ),
    );
    const start = (data.page - 1) * data.pageSize;
    $("results-label").textContent = currentTasks.length
      ? `Showing ${number(start + 1)}–${number(start + currentTasks.length)} · ${data.hasMore ? "more available" : "end of results"}`
      : "No matching tasks";
    $("page-label").textContent = `Page ${number(data.page)}`;
    $("previous").disabled = data.page <= 1;
    $("next").disabled = !data.hasMore;
    renderSelection();
  }
  function renderSelection() {
    $("selection-count").textContent = `${selected.size} selected`;
    $("retry-selected").disabled = !selected.size;
    $("cancel-selected").disabled = !selected.size;
    $("clear-selection").disabled = !selected.size;
    $("select-page").checked =
      currentTasks.length > 0 &&
      currentTasks.every((task) => selected.has(task.taskId));
    $("select-page").indeterminate =
      selected.size > 0 && !$("select-page").checked;
    $("select-page").disabled = !currentTasks.length;
    if (selected.size) {
      refreshSession.invalidate();
      clearTimeout(refreshTimer);
      $("refresh").disabled = true;
    } else {
      $("refresh").disabled = false;
      schedule();
    }
    setConnection();
  }
  function renderChips() {
    const names = {
      search: "Contains",
      state: "State",
      name: "Task",
      queue: "Queue",
      worker: "Worker",
      range: "Updated within",
    };
    $("filter-chips").innerHTML = Object.entries(names)
      .filter(([key]) => filters[key])
      .map(
        ([key, name]) =>
          `<button class="chip" data-remove-filter="${key}" aria-label="Remove ${name} filter">${name}: ${esc(key === "range" ? `${filters[key]} min` : stateLabels[filters[key]] || filters[key])}<span aria-hidden="true">×</span></button>`,
      )
      .join("");
  }
  function applyCapabilities(data) {
    capabilities = data;
    if (data.pageSize) {
      const sizes = [10, 25, 50].filter((size) => size <= data.pageSize);
      if (!sizes.includes(data.pageSize)) sizes.push(data.pageSize);
      $("page-size").innerHTML = sizes
        .map((size) => `<option value="${size}">${size}</option>`)
        .join("");
      filters.size = Math.min(filters.size, data.pageSize);
      $("page-size").value = filters.size;
    }
    $("access-mode").hidden = !data.readOnly;
    $("bulk-controls").hidden =
      data.readOnly || (!data.canCancel && !data.canRetry);
    $("retry-selected").hidden = !data.canRetry;
    $("cancel-selected").hidden = !data.canCancel;
    $("select-page").hidden = data.readOnly;
    $("retention-label").textContent =
      `History retained for ${number(data.retentionSeconds / 3600)}h · recorded by participating hosts`;
  }
  async function refresh() {
    clearTimeout(refreshTimer);
    if (
      selected.size ||
      actionRunning ||
      $("action-dialog").open ||
      $("task-dialog").open
    ) {
      schedule();
      return;
    }
    const ticket = refreshSession.begin();
    const signal = ticket.signal;
    $("refresh").disabled = true;
    try {
      if (!capabilities)
        applyCapabilities(await fetchJson("capabilities", { signal }));
      const paths = [
        "overview",
        `metrics?windowMinutes=${metricsWindow}`,
        "workers",
        "queues",
      ];
      if (view === "overview") paths.push("tasks/search?pageSize=7");
      if (view === "tasks")
        paths.push(`tasks/search?${taskQuery()}`, "task-names");
      if (view === "overview" && capabilities.hasHistory) {
        const from = new Date(Date.now() - metricsWindow * 60000).toISOString();
        paths.push(
          `metrics/timeseries?${new URLSearchParams({ from, granularity: metricsWindow === 1440 ? "Hour" : "Minute" })}`,
        );
      }
      const results = await Promise.allSettled(
        paths.map((path) => fetchJson(path, { signal })),
      );
      if (!refreshSession.isCurrent(ticket)) return;
      let failed = false;
      const errors = [];
      for (let index = 0; index < results.length; index++) {
        const result = results[index];
        if (result.status === "rejected") {
          if (result.reason.name === "AbortError") return;
          failed = true;
          errors.push(result.reason.message);
          continue;
        }
        const data = result.value;
        const path = paths[index];
        if (path === "workers") {
          renderWorkers(data);
          updateOptions(
            "worker",
            data.map((worker) => worker.workerId),
            "All workers",
            filters.worker,
          );
        } else if (path === "queues") {
          renderQueues(data);
          updateOptions(
            "queue",
            data.map((queue) => queue.name),
            "All queues",
            filters.queue,
          );
        } else if (path === "task-names")
          updateOptions("task-name", data, "All task names", filters.name);
        else if (path.startsWith("tasks/search"))
          view === "overview" ? renderRecent(data.tasks) : renderTasks(data);
        else if (path.startsWith("metrics/timeseries")) renderChart(data);
      }
      if (
        results[0].status === "fulfilled" &&
        results[1].status === "fulfilled"
      ) {
        renderStats(results[0].value, results[1].value);
        renderAttention(results[1].value);
      }
      if (view === "overview" && !capabilities.hasHistory) renderChart(null);
      lastAttemptFailed = failed;
      if (!failed) lastSuccess = new Date().toISOString();
      showMessage(
        failed
          ? `${[...new Set(errors)].join(" ")} Displayed data may be stale. Use Refresh to try again.`
          : "",
        true,
      );
    } catch (error) {
      if (!refreshSession.isCurrent(ticket) || error.name === "AbortError")
        return;
      lastAttemptFailed = true;
      showMessage(
        `${error.message} Displayed data may be stale. Use Refresh to try again.`,
        true,
      );
    } finally {
      if (refreshSession.isCurrent(ticket)) {
        $("refresh").disabled = false;
        setConnection();
        schedule();
      }
    }
  }
  function schedule() {
    clearTimeout(refreshTimer);
    if (!paused && !document.hidden)
      refreshTimer = setTimeout(
        refresh,
        (capabilities?.refreshSeconds || 5) * 1000,
      );
  }
  function writeUrl() {
    const query = new URLSearchParams({ view });
    for (const key of Object.keys(filters))
      if (
        filters[key] &&
        filters[key] !== { page: 1, size: 25, order: "newest" }[key]
      )
        query.set(key, filters[key]);
    if (metricsWindow !== 60) query.set("window", metricsWindow);
    if (detailId) query.set("task", detailId);
    history.replaceState(null, "", `${location.pathname}?${query}`);
  }
  function readUrl() {
    const params = new URLSearchParams(location.search);
    view = views[params.get("view")] ? params.get("view") : "overview";
    for (const key of [
      "search",
      "state",
      "name",
      "queue",
      "worker",
      "range",
      "order",
    ])
      filters[key] = params.get(key) || { order: "newest" }[key] || "";
    if (!taskStates.includes(filters.state)) filters.state = "";
    if (!["", "15", "60", "1440"].includes(filters.range)) filters.range = "";
    if (!["newest", "oldest"].includes(filters.order)) filters.order = "newest";
    filters.page = Math.max(
      1,
      Math.min(10000, Number.parseInt(params.get("page"), 10) || 1),
    );
    filters.size = [10, 25, 50].includes(Number(params.get("size")))
      ? Number(params.get("size"))
      : 25;
    metricsWindow = [15, 60, 1440].includes(Number(params.get("window")))
      ? Number(params.get("window"))
      : 60;
  }
  function syncControls() {
    for (const [id, key] of [
      ["search", "search"],
      ["state", "state"],
      ["task-range", "range"],
      ["task-order", "order"],
      ["page-size", "size"],
    ])
      $(id).value = filters[key];
    $("metrics-window").value = metricsWindow;
    renderChips();
  }
  function setView(nextView, updateUrl = true) {
    if (nextView !== view) selected.clear();
    view = nextView;
    $("metrics-window").hidden = view !== "overview";
    for (const key of Object.keys(views))
      $(`${key}-view`).hidden = key !== view;
    document.querySelectorAll("nav [data-view]").forEach((link) => {
      if (link.dataset.view === view) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    $("page-title").textContent = views[view][0];
    $("page-subtitle").textContent = views[view][1];
    if (updateUrl) writeUrl();
    refresh();
  }
  function filtersChanged() {
    filters.page = 1;
    selected.clear();
    renderSelection();
    syncControls();
    writeUrl();
    refresh();
  }
  async function openTask(id) {
    const ownRevision = ++detailRevision;
    detailId = id;
    writeUrl();
    $("detail-title").textContent = "Task details";
    $("detail-body").innerHTML = '<p class="muted">Loading task details…</p>';
    $("detail-actions").replaceChildren();
    $("detail-body").setAttribute("aria-busy", "true");
    if (!$("task-dialog").open) $("task-dialog").showModal();
    try {
      const [taskResult, actionResult] = await Promise.allSettled([
        fetchJson(`tasks/${encodeURIComponent(id)}`),
        capabilities?.readOnly
          ? Promise.resolve({ cancelTaskIds: [], retryTaskIds: [] })
          : fetchJson("tasks/actions/preview", {
              method: "POST",
              body: JSON.stringify({ taskIds: [id] }),
            }),
      ]);
      if (ownRevision !== detailRevision || !$("task-dialog").open) return;
      if (taskResult.status === "rejected") throw taskResult.reason;
      const task = taskResult.value;
      detailActions =
        actionResult.status === "fulfilled"
          ? actionResult.value
          : { cancelTaskIds: [], retryTaskIds: [] };
      $("detail-title").textContent = task.taskName;
      const fields = [
        ["State", stateLabels[task.state] || task.state],
        ["Queue", task.queue || "Not recorded"],
        ["Worker", task.worker || "Not recorded"],
        ["Duration", duration(task.duration)],
        ["Sent", exactTime(task.sentAt)],
        ["Started", exactTime(task.startedAt)],
        ["Completed", exactTime(task.completedAt)],
        ["Retry count", number(task.retries)],
        ["Scheduled for", exactTime(task.eta)],
      ];
      $("detail-body").innerHTML =
        `<div class="detail-id"><code>${esc(task.taskId)}</code><button class="button secondary" id="copy-task-id">Copy ID</button><button class="button secondary" id="copy-task-link">Copy link</button></div>${badge(task.state)}${cancellationRequests.has(id) ? '<p class="muted">Cancellation requested. The worker outcome will confirm whether execution stopped.</p>' : ""}<dl class="detail-grid">${fields.map(([label, value]) => `<div><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`).join("")}</dl>${task.exceptionMessage ? `<div class="error-detail" role="note">${esc(task.exceptionMessage)}</div>` : ""}${actionResult.status === "rejected" ? '<p class="muted">Task actions are temporarily unavailable.</p>' : ""}`;
      const buttons = [];
      if (detailActions.retryTaskIds.includes(id))
        buttons.push(
          '<button class="button secondary" id="retry-task">Retry task</button>',
        );
      if (detailActions.cancelTaskIds.includes(id))
        buttons.push(
          '<button class="button danger-outline" id="cancel-task">Request cancellation</button>',
        );
      $("detail-actions").innerHTML = capabilities?.readOnly
        ? '<span class="muted">Read-only access</span>'
        : buttons.join("") ||
          '<span class="muted">No task actions available for this execution.</span>';
    } catch (error) {
      if (ownRevision === detailRevision)
        $("detail-body").textContent = error.message;
    } finally {
      if (ownRevision === detailRevision)
        $("detail-body").setAttribute("aria-busy", "false");
    }
  }
  async function copyText(text, button) {
    try {
      await navigator.clipboard.writeText(text);
      button.textContent = "Copied";
    } catch {
      button.textContent = "Select and copy manually";
    }
  }
  async function planAction(action, ids) {
    if (actionRunning) return;
    const ownActionRevision = ++actionRevision;
    actionPlan = null;
    $("action-title").textContent =
      action === "cancel" ? "Request task cancellation" : "Retry tasks";
    $("action-body").textContent = "Checking selected tasks…";
    $("confirm-action").disabled = true;
    if (!$("action-dialog").open) $("action-dialog").showModal();
    try {
      const preview = await fetchJson("tasks/actions/preview", {
        method: "POST",
        body: JSON.stringify({ taskIds: ids }),
      });
      if (!$("action-dialog").open || ownActionRevision !== actionRevision)
        return;
      const eligible =
        action === "cancel" ? preview.cancelTaskIds : preview.retryTaskIds;
      actionPlan = { action, ids: eligible };
      $("action-body").innerHTML =
        `<p><strong>${eligible.length} of ${ids.length}</strong> selected tasks ${action === "cancel" ? "can receive a cancellation request" : "are available in the dead-letter store for retry"}.</p><p class="muted">${action === "cancel" ? "Cancellation is cooperative. A running task can complete before the worker handles the request." : "Retry requeues the original task. Its business effects may run again."}</p>${eligible.length < ids.length ? `<p class="muted">${ids.length - eligible.length} unavailable tasks will be skipped.</p>` : ""}${eligible.length ? `<ul class="action-list">${eligible.map((id) => `<li>${esc(id)}</li>`).join("")}</ul>` : ""}`;
      $("confirm-action").textContent =
        action === "cancel"
          ? `Request cancellation (${eligible.length})`
          : `Retry ${eligible.length} ${eligible.length === 1 ? "task" : "tasks"}`;
      $("confirm-action").className =
        `button ${action === "cancel" ? "danger" : "primary"}`;
      $("confirm-action").disabled = eligible.length === 0;
    } catch (error) {
      $("action-body").textContent = error.message;
    }
  }
  async function confirmAction() {
    if (!actionPlan || actionRunning) return;
    actionRunning = true;
    $("confirm-action").disabled = true;
    $("confirm-action").textContent = "Working…";
    $("dismiss-action").disabled = true;
    $("close-action").disabled = true;
    try {
      const plan = actionPlan;
      const result = await fetchJson(
        `tasks/bulk/${plan.action === "cancel" ? "revoke" : "retry"}`,
        {
          method: "POST",
          body: JSON.stringify({ taskIds: plan.ids, terminate: true }),
        },
      );
      if (plan.action === "cancel")
        for (const id of plan.ids)
          if (!(result.failedTaskIds || []).includes(id))
            cancellationRequests.add(id);
      const completed =
        plan.action === "cancel"
          ? result.successCount === 1
            ? "cancellation request accepted"
            : "cancellation requests accepted"
          : result.successCount === 1
            ? "task requeued"
            : "tasks requeued";
      const text = `${result.successCount} ${completed}.${result.failureCount ? ` ${result.failureCount} failed: ${result.failedTaskIds.join(", ")}.` : ""}`;
      showMessage(text);
      $("message").classList.toggle("error-notice", result.failureCount > 0);
      selected.clear();
      $("action-dialog").close();
      $("task-dialog").close();
      detailId = null;
      writeUrl();
    } catch (error) {
      $("action-body").textContent =
        `${error.message} The result may be uncertain; inspect task state before repeating the action.`;
    } finally {
      actionRunning = false;
      actionPlan = null;
      $("dismiss-action").disabled = false;
      $("close-action").disabled = false;
      refresh();
    }
  }

  document.addEventListener("click", (event) => {
    const task = event.target.closest("[data-task-id]");
    if (task) {
      event.preventDefault();
      openTask(task.dataset.taskId);
      return;
    }
    const queue = event.target.closest("[data-queue]"),
      worker = event.target.closest("[data-worker]"),
      name = event.target.closest("[data-task-name]");
    if (queue || worker || name) {
      event.preventDefault();
      for (const key of ["search", "state", "name", "queue", "worker", "range"])
        filters[key] = "";
      if (queue) filters.queue = queue.dataset.queue;
      if (worker) filters.worker = worker.dataset.worker;
      if (name) {
        filters.name = name.dataset.taskName;
        if (name.dataset.failure) filters.state = "Failure";
      }
      filters.page = 1;
      selected.clear();
      syncControls();
      setView("tasks");
      return;
    }
    const nav = event.target.closest("[data-view]");
    if (nav) {
      event.preventDefault();
      setView(nav.dataset.view);
      return;
    }
    const chip = event.target.closest("[data-remove-filter]");
    if (chip) {
      filters[chip.dataset.removeFilter] = "";
      filtersChanged();
    }
    if (event.target.id === "copy-task-id") copyText(detailId, event.target);
    if (event.target.id === "copy-task-link")
      copyText(new URL(taskUrl(detailId), location.origin).href, event.target);
    if (event.target.id === "retry-task") planAction("retry", [detailId]);
    if (event.target.id === "cancel-task") planAction("cancel", [detailId]);
  });
  $("task-filters").addEventListener("submit", (event) => {
    event.preventDefault();
    filters.search = $("search").value.trim();
    filtersChanged();
  });
  for (const [id, key] of [
    ["state", "state"],
    ["task-name", "name"],
    ["queue", "queue"],
    ["worker", "worker"],
    ["task-range", "range"],
    ["task-order", "order"],
  ])
    $(id).addEventListener("change", () => {
      filters[key] = $(id).value;
      filtersChanged();
    });
  $("page-size").addEventListener("change", () => {
    filters.size = Number($("page-size").value);
    filtersChanged();
  });
  $("previous").addEventListener("click", () => {
    filters.page = Math.max(1, filters.page - 1);
    selected.clear();
    writeUrl();
    refresh();
  });
  $("next").addEventListener("click", () => {
    filters.page++;
    selected.clear();
    writeUrl();
    refresh();
  });
  $("metrics-window").addEventListener("change", () => {
    metricsWindow = Number($("metrics-window").value);
    writeUrl();
    refresh();
  });
  $("refresh").addEventListener("click", refresh);
  $("pause").addEventListener("click", () => {
    paused = !paused;
    $("pause").setAttribute("aria-pressed", String(paused));
    $("pause").textContent = paused ? "Resume updates" : "Pause updates";
    if (paused) {
      clearTimeout(refreshTimer);
      refreshSession.invalidate();
      $("refresh").disabled = false;
    } else refresh();
    setConnection();
  });
  $("tasks-body").addEventListener("change", (event) => {
    const id = event.target.dataset.selectId;
    if (!id) return;
    event.target.checked ? selected.add(id) : selected.delete(id);
    renderSelection();
  });
  $("select-page").addEventListener("change", () => {
    for (const task of currentTasks)
      $("select-page").checked
        ? selected.add(task.taskId)
        : selected.delete(task.taskId);
    $("tasks-body")
      .querySelectorAll("[data-select-id]")
      .forEach((input) => {
        input.checked = selected.has(input.dataset.selectId);
      });
    renderSelection();
  });
  $("clear-selection").addEventListener("click", () => {
    selected.clear();
    $("tasks-body")
      .querySelectorAll("[data-select-id]")
      .forEach((input) => {
        input.checked = false;
      });
    renderSelection();
  });
  $("retry-selected").addEventListener("click", () =>
    planAction("retry", [...selected]),
  );
  $("cancel-selected").addEventListener("click", () =>
    planAction("cancel", [...selected]),
  );
  $("close-details").addEventListener("click", () => $("task-dialog").close());
  $("task-dialog").addEventListener("close", () => {
    detailRevision++;
    detailId = null;
    writeUrl();
    schedule();
  });
  for (const id of ["close-action", "dismiss-action"])
    $(id).addEventListener("click", () => {
      if (!actionRunning) $("action-dialog").close();
    });
  $("action-dialog").addEventListener("cancel", (event) => {
    if (actionRunning) event.preventDefault();
  });
  $("action-dialog").addEventListener("close", () => {
    actionPlan = null;
    actionRevision++;
    schedule();
  });
  $("confirm-action").addEventListener("click", confirmAction);
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      clearTimeout(refreshTimer);
      refreshSession.invalidate();
    } else if (!paused) refresh();
  });
  window.addEventListener("popstate", () => {
    selected.clear();
    readUrl();
    syncControls();
    setView(view, false);
  });
  readUrl();
  syncControls();
  setView(view, false);
  const initialTask = new URLSearchParams(location.search).get("task");
  if (initialTask) openTask(initialTask);
})();

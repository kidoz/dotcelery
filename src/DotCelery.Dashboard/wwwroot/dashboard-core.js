/* Shared, dependency-free presentation and request rules. Also exercised by node:test. */
((root, factory) => {
  const core = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = core;
  else root.DotCeleryDashboard = core;
})(globalThis, () => {
  "use strict";
  const number = (value) =>
    new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(
      value,
    );
  function durationMs(value) {
    if (value == null) return null;
    const match = String(value).match(
      /^(?:(\d+)\.)?(\d+):(\d+):(\d+(?:\.\d+)?)$/,
    );
    if (!match) return null;
    return (
      ((Number(match[1] || 0) * 24 + Number(match[2])) * 3600 +
        Number(match[3]) * 60 +
        Number(match[4])) *
      1000
    );
  }
  function duration(value) {
    const ms = durationMs(value);
    if (ms == null) return "—";
    if (ms < 1 && ms > 0) return "<1 ms";
    if (ms < 1000) return `${number(Math.round(ms))} ms`;
    if (ms < 60000) return `${number(ms / 1000)} s`;
    if (ms < 3600000)
      return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
    return `${Math.floor(ms / 3600000)}h ${Math.floor((ms % 3600000) / 60000)}m`;
  }
  function workerStatus(worker) {
    return typeof worker.status === "number"
      ? ["Online", "Paused", "Unresponsive", "Offline"][worker.status] ||
          "Unknown"
      : worker.status || "Unknown";
  }
  async function requestJson(url, options = {}, transport = fetch) {
    let response;
    const deadline = AbortSignal.timeout(15000);
    const signal = options.signal
      ? AbortSignal.any([options.signal, deadline])
      : deadline;
    try {
      response = await transport(url, {
        cache: "no-store",
        ...options,
        signal,
        headers: {
          "X-DotCelery-Dashboard": "1",
          ...(options.body ? { "Content-Type": "application/json" } : {}),
          ...options.headers,
        },
      });
    } catch (error) {
      if (error.name === "TimeoutError")
        throw new Error(
          "The dashboard request timed out. Try refreshing again.",
        );
      if (error instanceof TypeError)
        throw new Error(
          "Could not reach the dashboard server. Check your connection and try again.",
        );
      throw error;
    }
    if (!response.ok) {
      let message =
        response.status === 403
          ? "Access denied. Your session may have expired or this action is not permitted."
          : response.status === 404
            ? "This task is no longer available. Its history may have expired."
            : `Dashboard request failed (HTTP ${response.status}).`;
      try {
        const error = JSON.parse(await response.text());
        if (error.detail || error.title)
          message = `${message} ${error.detail || error.title}`;
      } catch {
        /* Never render HTML error responses as markup. */
      }
      throw new Error(message);
    }
    return response.status === 204 ? null : response.json();
  }
  class RequestSession {
    version = 0;
    controller = null;
    begin() {
      this.controller?.abort();
      this.controller = new AbortController();
      return { version: ++this.version, signal: this.controller.signal };
    }
    isCurrent(ticket) {
      return ticket.version === this.version;
    }
    invalidate() {
      this.version++;
      this.controller?.abort();
    }
  }
  return { durationMs, duration, workerStatus, requestJson, RequestSession };
});

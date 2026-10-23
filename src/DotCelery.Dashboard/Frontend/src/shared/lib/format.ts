// Dependency-free presentation rules shared by the dashboard views.

export const queueHealth = (queue: {
  messageCount: number;
  consumerCount: number;
}) =>
  queue.messageCount > 0 && queue.consumerCount === 0 ? "Warning" : "Healthy";

export const windowLabel = (window: number) =>
  window === 15
    ? "Last 15 minutes"
    : window === 1440
      ? "Last 24 hours"
      : "Last hour";

export const number = (value: number) =>
  new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value);

export function durationMs(value?: string | null): number | null {
  if (value == null) return null;
  const match = value.match(/^(?:(\d+)\.)?(\d+):(\d+):(\d+(?:\.\d+)?)$/);
  if (!match) return null;
  return (
    ((Number(match[1] || 0) * 24 + Number(match[2])) * 3600 +
      Number(match[3]) * 60 +
      Number(match[4])) *
    1000
  );
}

export function duration(value?: string | null): string {
  const ms = durationMs(value);
  if (ms == null) return "—";
  if (ms < 1 && ms > 0) return "<1 ms";
  if (ms < 1000) return `${number(Math.round(ms))} ms`;
  if (ms < 60000) return `${number(ms / 1000)} s`;
  if (ms < 3600000)
    return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
  return `${Math.floor(ms / 3600000)}h ${Math.floor((ms % 3600000) / 60000)}m`;
}

export function workerStatus(worker: { status: number | string }): string {
  return typeof worker.status === "number"
    ? (["Online", "Paused", "Unresponsive", "Offline"][worker.status] ??
        "Unknown")
    : worker.status || "Unknown";
}

export function exactTime(value?: string | null): string {
  if (!value) return "Not recorded";
  const date = new Date(value);
  return Number.isNaN(date.valueOf())
    ? "Not recorded"
    : date.toLocaleString(undefined, { timeZoneName: "short" });
}

export function relativeTime(value?: string | null): string {
  if (!value) return "—";
  const seconds = Math.round((new Date(value).valueOf() - Date.now()) / 1000);
  if (!Number.isFinite(seconds)) return "—";
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (Math.abs(seconds) < 60) return formatter.format(seconds, "second");
  if (Math.abs(seconds) < 3600)
    return formatter.format(Math.round(seconds / 60), "minute");
  if (Math.abs(seconds) < 86400)
    return formatter.format(Math.round(seconds / 3600), "hour");
  return formatter.format(Math.round(seconds / 86400), "day");
}

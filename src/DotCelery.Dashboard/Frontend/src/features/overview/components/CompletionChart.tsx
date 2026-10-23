import { exactTime, number, windowLabel } from "../../../shared/lib/format.ts";
import type { TimePoint } from "../../../shared/api/contracts.ts";

export function CompletionChart({
  points,
  hasHistory,
  window,
  snapshotSeconds,
}: {
  points?: TimePoint[];
  hasHistory?: boolean;
  window: number;
  snapshotSeconds?: number;
}) {
  if (hasHistory === undefined)
    return <p className="empty">Loading history…</p>;
  if (!hasHistory)
    return (
      <p className="empty">
        Historical metrics are not configured.
        <br />
        Live gauges and task history remain available.
      </p>
    );
  if (!points?.length)
    return (
      <>
        <p className="empty">
          Waiting for the first metrics snapshot.
          <br />
          Task completions will appear here once collected.
        </p>
        <p className="panel-note">
          Snapshots are recorded every {snapshotSeconds} seconds.
        </p>
      </>
    );
  const max = Math.max(
    1,
    ...points.map((point) => point.successCount + point.failureCount),
  );
  const step = 590 / points.length;
  const barWidth = Math.min(28, step * 0.65);
  return (
    <>
      <div className="chart-container">
        <svg
          viewBox="0 0 640 190"
          role="img"
          aria-labelledby="chart-title chart-description"
        >
          <title id="chart-title">
            Task completions, {windowLabel(window).toLowerCase()}
          </title>
          <desc id="chart-description">
            {number(points.reduce((sum, point) => sum + point.successCount, 0))}{" "}
            successful and{" "}
            {number(points.reduce((sum, point) => sum + point.failureCount, 0))}{" "}
            failed completions. Bars show counts per time bucket.
          </desc>
          {[0, 0.5, 1].map((fraction) => (
            <g key={fraction}>
              <line
                x1={38}
                y1={14 + 147 * (1 - fraction)}
                x2={628}
                y2={14 + 147 * (1 - fraction)}
                stroke="#e9eef1"
                strokeDasharray="3 4"
              />
              <text x={29} y={17 + 147 * (1 - fraction)} textAnchor="end">
                {number(max * fraction)}
              </text>
            </g>
          ))}
          {points.map((point, index) => {
            const success = (Math.max(0, point.successCount) / max) * 147;
            const failure = (Math.max(0, point.failureCount) / max) * 147;
            const x = 38 + step * index + (step - barWidth) / 2;
            return (
              <g key={`${point.timestamp}-${index}`}>
                <title>
                  {exactTime(point.timestamp)}: {number(point.successCount)}{" "}
                  successful, {number(point.failureCount)} failed
                </title>
                <rect
                  x={x}
                  y={161 - success}
                  width={barWidth}
                  height={success}
                  rx={2}
                  fill="var(--color-brand)"
                />
                <rect
                  x={x}
                  y={161 - success - failure}
                  width={barWidth}
                  height={failure}
                  rx={2}
                  fill="var(--color-danger)"
                />
              </g>
            );
          })}
          {[
            ...new Set([
              0,
              Math.floor((points.length - 1) / 2),
              points.length - 1,
            ]),
          ].map((index) => (
            <text
              key={index}
              x={38 + step * (index + 0.5)}
              y={183}
              textAnchor="middle"
            >
              {new Date(points[index]?.timestamp ?? "").toLocaleTimeString([], {
                hour: "2-digit",
                minute: "2-digit",
              })}
            </text>
          ))}
        </svg>
      </div>
      <p className="panel-note">
        Counts per {window === 1440 ? "hour" : "minute"} · local time ·
        snapshots every {snapshotSeconds}s
      </p>
    </>
  );
}

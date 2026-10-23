import { Badge } from "../../shared/ui/Badge.tsx";
import { EmptyRow, Table } from "../../shared/ui/Table.tsx";
import { Link } from "../../shared/ui/Link.tsx";
import { Panel } from "../../shared/ui/Panel.tsx";
import { Time } from "../../shared/ui/Time.tsx";
import { number, workerStatus } from "../../shared/lib/format.ts";
import type { Navigation } from "../../shared/model/navigation.ts";
import type { DashboardData } from "../../shared/model/dashboardData.ts";

export function Workers({
  data,
  navigation,
}: {
  data: DashboardData;
  navigation: Navigation;
}) {
  return (
    <Panel
      title="Worker fleet"
      description="Availability is determined by the registered worker heartbeat"
    >
      <Table
        label="Worker fleet"
        headings={[
          "Worker / host",
          "Status",
          "Queues",
          "Capacity",
          "Processed",
          "Last heartbeat",
        ]}
      >
        {data.workers?.length ? (
          data.workers.map((worker) => (
            <tr key={worker.workerId}>
              <td>
                <Link
                  href={`?view=tasks&worker=${encodeURIComponent(worker.workerId)}`}
                  onNavigate={() =>
                    navigation.inspect({ worker: worker.workerId })
                  }
                >
                  {worker.workerId}
                </Link>
                <span className="subtext">{worker.hostname}</span>
              </td>
              <td>
                <Badge state={workerStatus(worker)} />
              </td>
              <td>{worker.queues.join(", ") || "—"}</td>
              <td>
                <span className="flex items-center gap-2 whitespace-nowrap">
                  <progress
                    value={Math.max(0, worker.activeTasks)}
                    max={Math.max(1, worker.concurrency)}
                    aria-label={`${worker.workerId} execution capacity`}
                  />
                  {number(worker.activeTasks)} / {number(worker.concurrency)}
                </span>
              </td>
              <td>{number(worker.processedCount)}</td>
              <td>
                <Time value={worker.lastHeartbeat} />
              </td>
            </tr>
          ))
        ) : (
          <EmptyRow columns={6}>
            {data.workers
              ? "No registered workers. Check the worker host and its shared registry."
              : "Loading workers…"}
          </EmptyRow>
        )}
      </Table>
    </Panel>
  );
}

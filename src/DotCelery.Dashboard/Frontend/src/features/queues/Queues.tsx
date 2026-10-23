import { Badge } from "../../shared/ui/Badge.tsx";
import { EmptyRow, Table } from "../../shared/ui/Table.tsx";
import { Link } from "../../shared/ui/Link.tsx";
import { Panel } from "../../shared/ui/Panel.tsx";
import { Time } from "../../shared/ui/Time.tsx";
import { number, queueHealth } from "../../shared/lib/format.ts";
import type { Navigation } from "../../shared/model/navigation.ts";
import type { DashboardData } from "../../shared/model/dashboardData.ts";

export function Queues({
  data,
  navigation,
}: {
  data: DashboardData;
  navigation: Navigation;
}) {
  return (
    <Panel
      title="Message queues"
      description="Current queue gauges · select a queue to inspect its task history"
    >
      <Table
        label="Queue statistics"
        headings={[
          "Queue",
          "Health",
          "Waiting",
          "Running",
          "Consumers",
          "Last published",
        ]}
      >
        {data.queues?.length ? (
          data.queues.map((queue) => (
            <tr key={queue.name}>
              <td>
                <Link
                  href={`?view=tasks&queue=${encodeURIComponent(queue.name)}`}
                  onNavigate={() => navigation.inspect({ queue: queue.name })}
                >
                  {queue.name}
                </Link>
              </td>
              <td>
                <Badge state={queueHealth(queue)} />
                {queueHealth(queue) === "Warning" && (
                  <span className="ml-2 text-muted">No consumers</span>
                )}
              </td>
              <td>{number(queue.messageCount)}</td>
              <td>{number(queue.inProgressCount)}</td>
              <td>{number(queue.consumerCount)}</td>
              <td>
                <Time value={queue.lastMessageAt} />
              </td>
            </tr>
          ))
        ) : (
          <EmptyRow columns={6}>
            {data.queues
              ? "No queue metrics recorded yet. Queue metrics appear as configured stores receive activity."
              : "Loading queues…"}
          </EmptyRow>
        )}
      </Table>
    </Panel>
  );
}

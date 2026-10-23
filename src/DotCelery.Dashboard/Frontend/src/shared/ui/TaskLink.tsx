import { Link } from "./Link.tsx";
import type { TaskSummary } from "../api/contracts.ts";

export function TaskLink({
  task,
  href,
  open,
}: {
  task: TaskSummary;
  href: string;
  open: (id: string) => void;
}) {
  return (
    <>
      <Link
        className="task-link"
        href={href}
        onNavigate={() => open(task.taskId)}
      >
        {task.taskName}
      </Link>
      <span className="subtext" title={task.taskId}>
        {task.taskId.slice(0, 12)}…
      </span>
    </>
  );
}

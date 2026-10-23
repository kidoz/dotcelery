import { useEffect, useState } from "react";
import { Badge } from "../../../shared/ui/Badge.tsx";
import { Modal } from "../../../shared/ui/Modal.tsx";
import { CopyButton } from "../../../shared/ui/CopyButton.tsx";
import { duration, exactTime, number } from "../../../shared/lib/format.ts";
import { errorMessage } from "../../../shared/api/errors.ts";
import { requestJson } from "../../../shared/api/client.ts";
import {
  isTerminalTaskState,
  stateLabel,
} from "../../../shared/domain/taskState.ts";
import type {
  ActionPreview,
  ActionRequest,
  Capabilities,
  TaskSummary,
} from "../../../shared/api/contracts.ts";

export function TaskDetails({
  api,
  id,
  capabilities,
  cancelled,
  link,
  dismiss,
  plan,
}: {
  api: string;
  id: string;
  capabilities?: Capabilities;
  cancelled: boolean;
  link: string;
  dismiss: () => void;
  plan: (request: ActionRequest) => void;
}) {
  const [task, setTask] = useState<TaskSummary | null>(null);
  const [actions, setActions] = useState<ActionPreview | null>(null);
  const [error, setError] = useState("");
  const [actionsError, setActionsError] = useState(false);
  const readOnly = capabilities?.readOnly;
  useEffect(() => {
    const controller = new AbortController();
    async function load() {
      const options = { signal: controller.signal };
      const [details, preview] = await Promise.allSettled([
        requestJson<TaskSummary>(
          `${api}/tasks/${encodeURIComponent(id)}`,
          options,
        ),
        readOnly === false
          ? requestJson<ActionPreview>(`${api}/tasks/actions/preview`, {
              ...options,
              method: "POST",
              body: JSON.stringify({ taskIds: [id] }),
            })
          : Promise.resolve(null),
      ]);
      if (controller.signal.aborted) return;
      if (details.status === "fulfilled") {
        setTask(details.value);
        setError("");
      } else setError(errorMessage(details.reason));
      setActions(preview.status === "fulfilled" ? preview.value : null);
      setActionsError(preview.status === "rejected");
    }
    void load();
    return () => controller.abort();
  }, [api, id, readOnly]);
  const fields = task
    ? [
        ["State", stateLabel(task.state)],
        ["Queue", task.queue || "Not recorded"],
        ["Worker", task.worker || "Not recorded"],
        ["Duration", duration(task.duration)],
        ["Sent", exactTime(task.sentAt)],
        ["Started", exactTime(task.startedAt)],
        ["Completed", exactTime(task.completedAt)],
        ["Retry count", number(task.retries)],
        ["Scheduled for", exactTime(task.eta)],
      ]
    : [];
  return (
    <Modal
      id="detail"
      title={task?.taskName ?? "Task details"}
      onDismiss={dismiss}
      footer={
        capabilities?.readOnly ? (
          <span className="text-muted">Read-only access</span>
        ) : (
          <>
            {actions?.retryTaskIds.includes(id) && (
              <button
                className="button secondary"
                onClick={() => plan({ action: "retry", ids: [id] })}
              >
                Retry task
              </button>
            )}
            {actions?.cancelTaskIds.includes(id) && (
              <button
                className="button danger-outline"
                onClick={() => plan({ action: "cancel", ids: [id] })}
              >
                Request cancellation
              </button>
            )}
            {!actions?.retryTaskIds.includes(id) &&
              !actions?.cancelTaskIds.includes(id) && (
                <span className="text-muted">
                  {actionsError
                    ? "Task actions are temporarily unavailable."
                    : "No task actions available for this execution."}
                </span>
              )}
          </>
        )
      }
    >
      {error ? (
        <p role="alert">{error}</p>
      ) : !task ? (
        <p role="status">Loading task details…</p>
      ) : (
        <>
          <div className="mb-4 flex flex-wrap items-center gap-2">
            <code className="break-all">{task.taskId}</code>
            <CopyButton text={task.taskId}>Copy ID</CopyButton>
            <CopyButton text={new URL(link, location.origin).href}>
              Copy link
            </CopyButton>
          </div>
          <Badge state={task.state} />
          {cancelled && !isTerminalTaskState(task.state) && (
            <p className="mt-3 text-muted">
              Cancellation requested. The worker outcome will confirm whether
              execution stopped.
            </p>
          )}
          <dl className="detail-grid">
            {fields.map(([label, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
          {task.exceptionMessage && (
            <pre className="error-detail" role="note">
              {task.exceptionMessage}
            </pre>
          )}
        </>
      )}
    </Modal>
  );
}

import { useEffect, useRef, useState } from "react";
import { Modal } from "../../../shared/ui/Modal.tsx";
import { errorMessage, isAbort } from "../../../shared/api/errors.ts";
import { requestJson } from "../../../shared/api/client.ts";
import type {
  ActionPreview,
  ActionRequest,
  BulkResult,
} from "../../../shared/api/contracts.ts";

export function ActionDialog({
  api,
  request,
  dismiss,
  complete,
}: {
  api: string;
  request: ActionRequest;
  dismiss: () => void;
  complete: (request: ActionRequest, result: BulkResult) => void;
}) {
  const [preview, setPreview] = useState<ActionPreview | null>(null);
  const [error, setError] = useState("");
  const [running, setRunning] = useState(false);
  const inFlight = useRef(false);
  const { action, ids } = request;
  useEffect(() => {
    const controller = new AbortController();
    requestJson<ActionPreview>(`${api}/tasks/actions/preview`, {
      signal: controller.signal,
      method: "POST",
      body: JSON.stringify({ taskIds: ids }),
    })
      .then((result) => {
        if (!controller.signal.aborted) setPreview(result);
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted && !isAbort(error))
          setError(errorMessage(error));
      });
    return () => controller.abort();
  }, [api, ids]);
  const eligible =
    (action === "cancel" ? preview?.cancelTaskIds : preview?.retryTaskIds) ??
    [];
  async function confirm() {
    if (!eligible.length || inFlight.current || error) return;
    inFlight.current = true;
    setRunning(true);
    try {
      const result = await requestJson<BulkResult>(
        `${api}/tasks/bulk/${action === "cancel" ? "revoke" : "retry"}`,
        {
          method: "POST",
          body: JSON.stringify({ taskIds: eligible, terminate: true }),
        },
      );
      complete({ action, ids: eligible }, result);
    } catch (error) {
      setError(
        `${errorMessage(error)} The result may be uncertain; inspect task state before repeating the action.`,
      );
    } finally {
      inFlight.current = false;
      setRunning(false);
    }
  }
  return (
    <Modal
      id="action"
      title={action === "cancel" ? "Request task cancellation" : "Retry tasks"}
      busy={running}
      onDismiss={() => {
        if (!inFlight.current) dismiss();
      }}
      footer={
        <>
          <button
            className="button secondary"
            disabled={running}
            onClick={dismiss}
          >
            Go back
          </button>
          <button
            className={`button ${action === "cancel" ? "danger" : "primary"}`}
            disabled={running || !eligible.length || !!error}
            onClick={() => {
              void confirm();
            }}
          >
            {running
              ? "Working…"
              : action === "cancel"
                ? `Request cancellation (${eligible.length})`
                : `Retry ${eligible.length} ${eligible.length === 1 ? "task" : "tasks"}`}
          </button>
        </>
      }
    >
      {error ? (
        <p role="alert">{error}</p>
      ) : !preview ? (
        <p role="status">Checking selected tasks…</p>
      ) : (
        <>
          <p>
            <strong>
              {eligible.length} of {ids.length}
            </strong>{" "}
            selected tasks{" "}
            {action === "cancel"
              ? "can receive a cancellation request"
              : "are available in the dead-letter store for retry"}
            .
          </p>
          <p className="mt-3 text-muted">
            {action === "cancel"
              ? "Cancellation is cooperative. A running task can complete before the worker handles the request."
              : "Retry requeues the original task. Its business effects may run again."}
          </p>
          {eligible.length < ids.length && (
            <p className="mt-3 text-muted">
              {ids.length - eligible.length} unavailable tasks will be skipped.
            </p>
          )}
          {eligible.length > 0 && (
            <ul className="mt-4 max-h-40 list-disc overflow-auto pl-5 font-mono text-xs">
              {eligible.map((id) => (
                <li key={id}>{id}</li>
              ))}
            </ul>
          )}
        </>
      )}
    </Modal>
  );
}

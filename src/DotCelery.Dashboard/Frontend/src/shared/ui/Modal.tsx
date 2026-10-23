import { useEffect, useRef } from "react";
import type { ReactNode } from "react";

export function Modal({
  title,
  id,
  onDismiss,
  busy = false,
  children,
  footer,
}: {
  title: string;
  id: string;
  onDismiss: () => void;
  busy?: boolean;
  children: ReactNode;
  footer?: ReactNode;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const dialog = ref.current;
    if (dialog && !dialog.open) dialog.showModal();
    return () => {
      dialog?.close();
    };
  }, []);
  return (
    <dialog
      ref={ref}
      aria-labelledby={`${id}-title`}
      onCancel={(event) => {
        event.preventDefault();
        if (!busy) onDismiss();
      }}
    >
      <div className="dialog-heading">
        <h2 id={`${id}-title`}>{title}</h2>
        <button
          className="icon-button"
          disabled={busy}
          aria-label={`Close ${id === "detail" ? "task details" : "action confirmation"}`}
          onClick={onDismiss}
        >
          ×
        </button>
      </div>
      <div className="detail-body">{children}</div>
      {footer && <div className="dialog-actions">{footer}</div>}
    </dialog>
  );
}

import { useState } from "react";

export function CopyButton({
  text,
  children,
}: {
  text: string;
  children: string;
}) {
  const [message, setMessage] = useState(children);
  async function copy() {
    try {
      await navigator.clipboard.writeText(text);
      setMessage("Copied");
    } catch {
      setMessage("Select and copy manually");
    }
  }
  return (
    <button
      className="button secondary"
      onClick={() => {
        void copy();
      }}
    >
      {message}
    </button>
  );
}

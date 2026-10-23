import { useEffect, useRef } from "react";

export function SelectAll({
  checked,
  indeterminate,
  disabled,
  onChange,
}: {
  checked: boolean;
  indeterminate: boolean;
  disabled: boolean;
  onChange: (checked: boolean) => void;
}) {
  const ref = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (ref.current) ref.current.indeterminate = indeterminate;
  }, [indeterminate]);
  return (
    <input
      ref={ref}
      type="checkbox"
      aria-label="Select all tasks on this page"
      checked={checked}
      disabled={disabled}
      onChange={(event) => onChange(event.target.checked)}
    />
  );
}

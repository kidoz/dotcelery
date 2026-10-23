import type { ReactNode } from "react";

export function Filter({
  label,
  value,
  onChange,
  children,
}: {
  label: string;
  value: string | number;
  onChange: (value: string) => void;
  children: ReactNode;
}) {
  return (
    <label className="filter-label">
      {label}
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {children}
      </select>
    </label>
  );
}

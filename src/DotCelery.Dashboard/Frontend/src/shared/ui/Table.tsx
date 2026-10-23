import type { ReactNode } from "react";

export function Table({
  label,
  headings,
  children,
}: {
  label: string;
  headings: ReactNode[];
  children: ReactNode;
}) {
  return (
    <div className="table-scroll" tabIndex={0} role="region" aria-label={label}>
      <table>
        <thead>
          <tr>
            {headings.map((heading, index) => (
              <th scope="col" key={index}>
                {heading}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}

export function EmptyRow({
  columns,
  children,
}: {
  columns: number;
  children: ReactNode;
}) {
  return (
    <tr>
      <td colSpan={columns} className="empty">
        {children}
      </td>
    </tr>
  );
}

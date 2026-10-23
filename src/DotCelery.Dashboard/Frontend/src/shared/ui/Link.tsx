import type { MouseEvent, ReactNode } from "react";

export function Link({
  href,
  onNavigate,
  children,
  className = "text-link",
}: {
  href: string;
  onNavigate: () => void;
  children: ReactNode;
  className?: string;
}) {
  function click(event: MouseEvent<HTMLAnchorElement>) {
    if (
      event.button ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey
    )
      return;
    event.preventDefault();
    onNavigate();
  }
  return (
    <a href={href} className={className} onClick={click}>
      {children}
    </a>
  );
}

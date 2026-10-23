import { exactTime, relativeTime } from "../lib/format.ts";

export function Time({ value }: { value?: string | null }) {
  return (
    <time dateTime={value ?? undefined} title={exactTime(value)}>
      {relativeTime(value)}
    </time>
  );
}

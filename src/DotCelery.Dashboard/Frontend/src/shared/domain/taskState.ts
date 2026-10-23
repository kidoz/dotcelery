export const taskStates = [
  "Pending",
  "Received",
  "Started",
  "Success",
  "Failure",
  "Retry",
  "Progress",
  "Revoked",
  "Rejected",
  "Requeued",
];

export function stateLabel(state: string): string {
  if (state === "Started") return "Running";
  if (state === "Failure") return "Failed";
  return state;
}

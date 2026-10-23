import { stateLabel, taskStates } from "../domain/taskState.ts";

export function Badge({ state }: { state: string }) {
  const known = [
    ...taskStates,
    "Online",
    "Paused",
    "Unresponsive",
    "Offline",
    "Healthy",
    "Warning",
  ].includes(state);
  return (
    <span className={`badge badge-${known ? state.toLowerCase() : "unknown"}`}>
      {stateLabel(state) || "Unknown"}
    </span>
  );
}

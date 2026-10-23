import type { Filters } from "./filters.ts";

export type View = "overview" | "tasks" | "queues" | "workers";

export interface Navigation {
  navigate: (view: View) => void;
  inspect: (filters: Partial<Filters>) => void;
  taskHref: (id: string) => string;
  openTask: (id: string) => void;
}

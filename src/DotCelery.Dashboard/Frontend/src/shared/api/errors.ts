export function errorMessage(error: unknown): string {
  return error instanceof Error
    ? error.message
    : "An unexpected dashboard error occurred.";
}

export function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

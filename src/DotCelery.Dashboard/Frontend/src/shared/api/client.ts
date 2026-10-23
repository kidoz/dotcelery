export async function requestJson<T = unknown>(
  url: string,
  options: RequestInit = {},
  transport: typeof fetch = fetch,
): Promise<T> {
  const deadline = AbortSignal.timeout(15000);
  const signal = options.signal
    ? AbortSignal.any([options.signal, deadline])
    : deadline;
  const headers = new Headers(options.headers);
  headers.set("X-DotCelery-Dashboard", "1");
  if (options.body && !headers.has("Content-Type"))
    headers.set("Content-Type", "application/json");
  let response: Response;
  try {
    response = await transport(url, {
      cache: "no-store",
      credentials: "same-origin",
      ...options,
      signal,
      headers,
    });
  } catch (error) {
    if (error instanceof Error && error.name === "TimeoutError")
      throw new Error(
        "The dashboard request timed out. Try refreshing again.",
        { cause: error },
      );
    if (error instanceof TypeError)
      throw new Error(
        "Could not reach the dashboard server. Check your connection and try again.",
        { cause: error },
      );
    throw error;
  }
  if (!response.ok) {
    let message =
      response.status === 403
        ? "Access denied. Your session may have expired or this action is not permitted."
        : response.status === 404
          ? "This task is no longer available. Its history may have expired."
          : `Dashboard request failed (HTTP ${response.status}).`;
    try {
      const body: unknown = await response.json();
      if (body && typeof body === "object") {
        const detail = "detail" in body ? body.detail : undefined;
        const title = "title" in body ? body.title : undefined;
        if (typeof detail === "string") message += ` ${detail}`;
        else if (typeof title === "string") message += ` ${title}`;
      }
    } catch {
      /* Never render HTML error responses as markup. */
    }
    throw new Error(message);
  }
  return response.status === 204 ? (null as T) : ((await response.json()) as T);
}

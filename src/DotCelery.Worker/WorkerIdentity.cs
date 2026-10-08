namespace DotCelery.Worker;

/// <summary>
/// Resolves the name a worker reports under, shared by the worker service and the heartbeat it
/// sends to an <c>IWorkerRegistry</c>.
/// </summary>
internal static class WorkerIdentity
{
    /// <summary>
    /// Gets the worker's name: the configured one, or a name derived from the machine and process.
    /// </summary>
    /// <param name="options">The worker options.</param>
    /// <returns>The worker name.</returns>
    public static string Resolve(WorkerOptions options) =>
        options.WorkerName ?? $"worker-{Environment.MachineName}-{Environment.ProcessId}";
}

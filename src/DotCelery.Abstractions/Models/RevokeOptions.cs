namespace DotCelery.Core.Models;

/// <summary>
/// Options for task revocation.
/// </summary>
public sealed record RevokeOptions
{
    /// <summary>
    /// Gets or sets whether to terminate running tasks by sending a cancellation signal.
    /// If false, only pending tasks will be affected.
    /// </summary>
    public bool Terminate { get; init; }

    /// <summary>
    /// Gets or sets how long to keep the revocation record.
    /// After this time, the revocation may be forgotten and the task could be re-executed
    /// if it's still in the queue. Default is 24 hours.
    /// </summary>
    public TimeSpan? Expiry { get; init; }

    /// <summary>
    /// Gets or sets the signal type for running tasks.
    /// Only applies when <see cref="Terminate"/> is true.
    /// </summary>
    public CancellationSignal Signal { get; init; } = CancellationSignal.Graceful;

    /// <summary>
    /// Creates default revoke options that only prevents pending task execution.
    /// </summary>
    public static RevokeOptions Default { get; } = new();

    /// <summary>
    /// Creates revoke options that also terminates running tasks gracefully.
    /// </summary>
    public static RevokeOptions WithTermination { get; } = new() { Terminate = true };

    /// <summary>
    /// Creates revoke options that terminates running tasks immediately.
    /// </summary>
    public static RevokeOptions Immediate { get; } =
        new() { Terminate = true, Signal = CancellationSignal.Immediate };
}

/// <summary>
/// Specifies how to signal cancellation to running tasks.
/// </summary>
public enum CancellationSignal
{
    /// <summary>
    /// Request graceful cancellation. The task can catch the cancellation
    /// and perform cleanup before exiting.
    /// </summary>
    Graceful,

    /// <summary>
    /// Request immediate cancellation. The task should stop as soon as possible
    /// without extended cleanup.
    /// </summary>
    Immediate,
}

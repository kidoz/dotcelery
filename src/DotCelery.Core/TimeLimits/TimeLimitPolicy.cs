namespace DotCelery.Core.TimeLimits;

/// <summary>
/// Defines time limits for task execution.
/// </summary>
/// <param name="SoftLimit">The soft time limit. When exceeded, a SoftTimeLimitExceededException is thrown
/// but the task can catch it and perform cleanup.</param>
/// <param name="HardLimit">The hard time limit. When exceeded, the task's CancellationToken is cancelled
/// forcing immediate termination.</param>
public sealed record TimeLimitPolicy(TimeSpan? SoftLimit = null, TimeSpan? HardLimit = null)
{
    /// <summary>
    /// Creates a policy with only a soft limit.
    /// </summary>
    /// <param name="softLimit">The soft time limit.</param>
    /// <returns>A new TimeLimitPolicy.</returns>
    public static TimeLimitPolicy WithSoftLimit(TimeSpan softLimit) => new(SoftLimit: softLimit);

    /// <summary>
    /// Creates a policy with only a hard limit.
    /// </summary>
    /// <param name="hardLimit">The hard time limit.</param>
    /// <returns>A new TimeLimitPolicy.</returns>
    public static TimeLimitPolicy WithHardLimit(TimeSpan hardLimit) => new(HardLimit: hardLimit);

    /// <summary>
    /// Creates a policy with both soft and hard limits.
    /// </summary>
    /// <param name="softLimit">The soft time limit.</param>
    /// <param name="hardLimit">The hard time limit.</param>
    /// <returns>A new TimeLimitPolicy.</returns>
    public static TimeLimitPolicy WithLimits(TimeSpan softLimit, TimeSpan hardLimit) =>
        new(SoftLimit: softLimit, HardLimit: hardLimit);

    /// <summary>
    /// Gets whether this policy has any time limits defined.
    /// </summary>
    public bool HasLimits => SoftLimit.HasValue || HardLimit.HasValue;
}
